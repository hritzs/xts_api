"""
API Routes - UPDATED with correct imports
"""
import asyncio
import os
import pandas as pd
from fastapi import APIRouter, HTTPException
import re
from collections import defaultdict
import multiprocessing
from typing import Optional
from datetime import datetime
from utils.logger import logger
from market_data import get_option_chain
from trading.builder import build_straddle, manual_sync_trade_orders
from trading.config_builder import build_with_config
from trading.order_executor import get_order_executor
from trading.trade_manager import create_trade_manager, get_trade_manager
from trading.event_bus import get_event_bus, EventPriority
from trading.trade_process import trade_process_worker_entry
from models.state import state
from background.tasks import broadcast_message, get_live_pnl_data, create_snapshot_for_trade
from utils.helpers import get_ist_now
from pydantic import BaseModel, Field
import httpx
import config

# Import from models/schemas.py
from models.schemas import (
    StraddleRequest, ConfigBuildRequest, ConfigBuildResponse,
    HealthResponse, APIResponse, OrderBookResponse, PositionsResponse,
    StraddlesResponse, PnLResponse, OptionChainResponse, PartialSquareOffRequest,
    CustomStraddleRequest # NEW
)

router = APIRouter(prefix="/api")

async def _trigger_and_get_snapshot(trade_uid: str) -> Optional[dict]:
    """Triggers a snapshot and fetches it via HTTP, ensuring it's fresh."""
    from background.tasks import trigger_snapshot_and_broadcast
    
    # 1. Trigger a high-priority snapshot computation
    await trigger_snapshot_and_broadcast(trade_uid, bypass_debounce=True)
    
    # 2. Poll the snapshot service until we get a fresh snapshot
    url = f"http://localhost:{getattr(config, 'SNAPSHOT_SERVICE_PORT', 8003)}/api/snapshots/{trade_uid}"
    for _ in range(5): # Poll for up to 2.5 seconds
        await asyncio.sleep(0.5)
        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                resp = await client.get(url)
                if resp.status_code == 200:
                    return resp.json()
        except Exception:
            continue
    return None

@router.get("/snapshot/{trade_uid}")
async def api_get_snapshot_proxy(trade_uid: str):
    """Proxies a request to the snapshot service to get the latest data for a trade."""
    snapshot_service_port = getattr(config, 'SNAPSHOT_SERVICE_PORT', 8003)
    url = f"http://localhost:{snapshot_service_port}/api/snapshots/{trade_uid}"
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json()
            else:
                # Return the error from the service as our own
                raise HTTPException(status_code=resp.status_code, detail=f"Snapshot service error: {resp.text}")
    except httpx.RequestError as e:
        logger.error(f"Could not proxy request to snapshot service: {e}")
        raise HTTPException(status_code=503, detail="Could not connect to the snapshot service.")



@router.get("/health")
async def health():
    """Health check"""
    from trading.event_bus import get_event_bus
    event_bus = get_event_bus()
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "db_status": "connected" if state.db else "disconnected",
        "db_connected": state.db is not None,
        "socket_connected": state.socket_connected,
        "event_bus": "active" if event_bus else "inactive",
        "cached_prices": len(state.prices),
        "subscribed_tokens": len(state.subscribed_tokens),
        "active_straddles": len(state.db.get_active_straddles()) if state.db else 0
    }


@router.get("/option-chain/{symbol}")
async def api_get_option_chain(symbol: str, strike_range: int = 15):
    """Get option chain with Greeks from cache."""
    try:
        # Serve directly from the cache populated by the background task
        chain = state.option_chains.get(symbol.upper())
        if chain:
            return {'success': True, 'data': chain}
        else:
            # The API endpoint is now a pure consumer. It does not build the chain.
            logger.warning(f"⚠️ Option chain for {symbol} not in cache for API request.")
            return {'success': False, 'error': f'Option chain for {symbol.upper()} not available in cache. It may be building.'}
    except Exception as e:
        logger.error(f"Option chain API error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {'success': False, 'error': str(e)}


@router.post("/straddle/sell")
async def api_sell_straddle(request: StraddleRequest):
    """Sell straddle - simple manual entry"""
    try:
        now = datetime.now()
        
        logger.info(f"📥 API: Manual straddle sell request for {request.symbol}")
        
        # For manual trades, create a default config to pass to the builder.
        # The builder will then persist this config and the worker process will use it.
        default_config = {
            "symbol": request.symbol, "size": request.lots, "entry_time": None, "exit_time": None,
            "sl_bps": 14, "hedge_div": 57, "straddle_div": 4, "roll_straddle_div": 2.0,
            "hedge_monitor_interval": request.hedge_monitor_interval,
            "sl_monitor_interval": request.sl_monitor_interval,
            "roll_monitor_interval": request.roll_monitor_interval,
            "roll_flag_check_interval": 60.0, "hedge_frac": 1.0, "straddle_price_drop_trigger": 0.0,
            "straddle_price_monitor_interval": 10.0, "hedge_start_time": None, "sl_start_time": None,
            "roll_start_time": None,
            "order_lots_per_call": request.order_lots_per_call,
        }
        if "SENSEX" in request.symbol.upper():
            default_config['buy_buffer'] = 6
            default_config['sell_buffer'] = 6
        else:
            default_config['buy_buffer'] = 2
            default_config['sell_buffer'] = 2

        build_result = await build_straddle(
            symbol=request.symbol,
            lots=request.lots,
            trade_uid=None, # Let builder generate the robust suffixed UID
            delta_neutral=request.delta_neutral,
            trade_config=default_config
        )
        
        if build_result and build_result.get('success'):
            trade_uid = build_result['straddle_data']['trade_uid']
            logger.info(f"✅ Build process for manual trade {trade_uid} initiated successfully. Worker process will handle monitoring.")

            await broadcast_message({
                'type': 'straddle_placed',
                'trade_uid': trade_uid,
                'data': build_result,
                'timestamp': now.isoformat()
            })
            
            return {
                'success': True,
                'trade_uid': trade_uid,
                'data': build_result,
                'message': f'Straddle placed: {trade_uid}',
                'timestamp': now.isoformat()
            }
        else:
            error_message = (build_result or {}).get('error', 'Order placement failed')
            raise HTTPException(status_code=400, detail=error_message)
            
    except Exception as e:
        logger.error(f"❌ API error in /straddle/sell: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/straddle/custom-sell")
async def api_sell_custom_straddle(request: CustomStraddleRequest):
    """Sell straddle or strangle with custom CE and PE strike prices."""
    try:
        now = datetime.now()
        
        logger.info(f"📥 API: Custom straddle/strangle sell request for {request.symbol} (CE: {request.ce_strike_price}, PE: {request.pe_strike_price})")
        
        # For manual trades, create a default config to pass to the builder.
        default_config = {
            "symbol": request.symbol, "size": request.lots, "entry_time": None, "exit_time": None,
            "sl_bps": 14, "hedge_div": 57, "straddle_div": 4, "roll_straddle_div": 0.2,
            "hedge_monitor_interval": request.hedge_monitor_interval,
            "sl_monitor_interval": request.sl_monitor_interval,
            "roll_monitor_interval": request.roll_monitor_interval,
            "roll_flag_check_interval": 60.0, "hedge_frac": 1.0, "straddle_price_drop_trigger": 0.0,
            "straddle_price_monitor_interval": 10.0, "hedge_start_time": None, "sl_start_time": None,
            "roll_start_time": None,
            "order_lots_per_call": request.order_lots_per_call,
        }
        if "SENSEX" in request.symbol.upper():
            default_config['buy_buffer'] = 6
            default_config['sell_buffer'] = 6
        else:
            default_config['buy_buffer'] = 2
            default_config['sell_buffer'] = 2

        build_result = await build_straddle(
            symbol=request.symbol,
            lots=request.lots,
            trade_uid=None, # Let builder generate the robust suffixed UID
            delta_neutral=request.delta_neutral,
            product_type=request.product_type,
            trade_config=default_config,
            ce_strike_price=request.ce_strike_price,
            pe_strike_price=request.pe_strike_price
        )
        
        if build_result and build_result.get('success'):
            trade_uid = build_result['straddle_data']['trade_uid']
            logger.info(f"✅ Build process for custom trade {trade_uid} initiated successfully. Worker process will handle monitoring.")

            await broadcast_message({
                'type': 'straddle_placed',
                'trade_uid': trade_uid,
                'data': build_result,
                'timestamp': now.isoformat()
            })
            
            return {
                'success': True,
                'trade_uid': trade_uid,
                'data': build_result,
                'message': f'Custom straddle/strangle placed: {trade_uid}',
                'timestamp': now.isoformat()
            }
        else:
            error_message = (build_result or {}).get('error', 'Order placement failed')
            raise HTTPException(status_code=400, detail=error_message)
            
    except Exception as e:
        logger.error(f"❌ API error in /straddle/custom-sell: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/straddle/config-build")
async def api_config_build(request: ConfigBuildRequest):
    """
    Configuration-based automated build
    
    Features:
    - Entry filters (IV, Straddle price)
    - Time-based entry/exit
    - Automated monitoring (SL, Hedge, Roll, Square-off)
    - Event-driven execution
    """
    try:
        logger.info("="*100)
        logger.info("📥 API: Config-based build request")
        logger.info(f"   Symbol: {request.symbol}")
        logger.info(f"   Size: {request.size} lots")
        logger.info(f"   Entry: {request.entry_time}, Exit: {request.exit_time}")
        logger.info(f"   IDV: {request.idv}, Divisor: {request.idv_divisor}")
        logger.info(f"   Straddle Filter: ₹{request.straddle_filter}")
        logger.info(f"   Straddle Stop %: {getattr(request, 'straddle_stop_loss_pct', 1.0)}%")
        logger.info(f"   SL BPS: {request.sl_bps}")
        logger.info(f"   Buy/Sell Buffer: {request.buy_buffer}/{request.sell_buffer} ticks")
        logger.info("="*100)
        
        # Convert to dict
        config = request.dict()
        
        # --- FIX: Create a DB record immediately for visibility ---
        loop = asyncio.get_event_loop()
        now = get_ist_now()
        
        # Use entry_time for timestamp if available, otherwise current time
        if request.entry_time:
            try:
                et = datetime.strptime(request.entry_time, "%H:%M:%S").time()
                timestamp = datetime.combine(now.date(), et).strftime("%d%m%y%H%M%S")
            except ValueError:
                try:
                    et = datetime.strptime(request.entry_time, "%H:%M").time()
                    timestamp = datetime.combine(now.date(), et).strftime("%d%m%y%H%M%S")
                except ValueError:
                    timestamp = now.strftime("%d%m%y%H%M%S")
        else:
            timestamp = now.strftime("%d%m%y%H%M%S")
            
        symbol_upper = request.symbol.upper()
        
        # Generate a unique trade_uid
        from trading.builder import SYMBOL_CONFIG as BUILDER_SYMBOL_CONFIG
        SYMBOL_PREFIXES = { "NIFTY": "ny", "SENSEX": "sx", "BANKNIFTY": "bn", "FINNIFTY": "fn", "MIDCPNIFTY": "mc" }
        sorted_keys = sorted(SYMBOL_PREFIXES.keys(), key=len, reverse=True)
        prefix = next((SYMBOL_PREFIXES[key] for key in sorted_keys if key in symbol_upper), "cb")

        base_trade_uid = f"{prefix}{timestamp}"
        # Always start with 'a'
        suffix_counter = 0
        trade_uid = f"{base_trade_uid}{chr(ord('a') + suffix_counter)}"
        
        while await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid):
            suffix_counter += 1
            trade_uid = f"{base_trade_uid}{chr(ord('a') + suffix_counter)}"

        # Create a 'monitors' object to match the live trade structure for UI consistency
        monitors_config = {
            'sl': {'sl_bps': request.sl_bps, 'start_time': request.sl_start_time, 'interval': request.sl_monitor_interval, 'running': False, 'sl_points': 0},
            'hedge': {'hedge_div': request.hedge_div, 'straddle_div': request.straddle_div, 'start_time': request.hedge_start_time, 'interval': request.hedge_monitor_interval, 'running': False},
            'roll': {'roll_straddle_div': request.roll_straddle_div, 'start_time': request.roll_start_time, 'interval': request.roll_monitor_interval, 'running': False},
            'square_off': {'exit_time': request.exit_time, 'running': False}
        }
        
        # Determine display strike for pending trade
        display_strike = 'N/A'
        if request.ce_strike_price and request.pe_strike_price:
            if request.ce_strike_price == request.pe_strike_price:
                display_strike = str(request.ce_strike_price)
            else:
                display_strike = f"{request.pe_strike_price}/{request.ce_strike_price}"
        
        pending_trade_data = {
            'trade_uid': trade_uid, 'straddle_id': trade_uid, 'symbol': request.symbol, 'status': 'PENDING',
            'monitors': monitors_config, 'config': config, 'entry_timestamp': now.isoformat(), 'strike': display_strike, 'ce_symbol': 'N/A', 'pe_symbol': 'N/A',
            'lots': request.size, 'ce_quantity': 0, 'pe_quantity': 0, 'ce_token': None, 'pe_token': None, 'expiry': 'N/A'
        }
        await loop.run_in_executor(None, state.db.insert_straddle, pending_trade_data)
        logger.info(f"✅ Created PENDING trade record for {trade_uid}.")
        # --- END FIX ---

        # Start build task in background
        async def build_task():
            try:
                # Pass the generated trade_uid to the builder
                built_trade_uid = await build_with_config(config, trade_uid=trade_uid)
                
                if not built_trade_uid:
                    # This means the build was not triggered (e.g., filters failed or time expired)
                    if state.cancellation_flags.get(trade_uid):
                        logger.info(f"Config build for {trade_uid} was cancelled by user. Updating status.")
                        await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'CANCELLED')
                        del state.cancellation_flags[trade_uid]
                    else:
                        # Check if the trade is already in PARTIAL status (meaning some orders were placed)
                        current_trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
                        current_status = current_trade.get('status') if current_trade else 'UNKNOWN'
                        
                        if current_status == 'PARTIAL':
                            logger.error(f"Build for {trade_uid} failed mid-execution but status is PARTIAL. Keeping status as PARTIAL.")
                        else:
                            logger.warning(f"⚠️  Config build for {trade_uid} did not execute. Updating status to FAILED_FILTER.")
                            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'FAILED_FILTER')
                    logger.info(f"✅ Config build for {trade_uid} completed and transitioned to worker process.")
                    
            except Exception as e:
                logger.error(f"❌ Build task error: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        # Start background task
        asyncio.create_task(build_task())
        
        return {
            'success': True,
            'trade_uid': trade_uid,
            'message': 'Config build scheduled. Trade record created.',
            'status': 'PENDING',
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Config build API error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


@router.get("/orders")
async def api_get_orders():
    """Get orders"""
    try:
        orders = state.db.get_todays_orders()
        # Format datetime for JS
        for order in orders:
            dt_str = order.get('last_update_datetime') or order.get('order_generated_datetime')
            if dt_str:
                dt = None
                try:
                    # First, try the most robust and standard method
                    dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    # If ISO format fails, try other common formats
                    formats_to_try = [
                        "%d-%m-%Y %H:%M:%S",        # 01-01-2024 15:30:00
                        "%d-%b-%Y %H:%M:%S",        # 01-Jan-2024 15:30:00
                        "%b %d %Y %H:%M:%S",        # Jan 01 2024 15:30:00
                        "%d%b%Y %H:%M:%S",          # 01Jan2024 15:30:00
                    ]
                    for fmt in formats_to_try:
                        try:
                            dt = datetime.strptime(dt_str, fmt)
                            break # Success
                        except (ValueError, TypeError):
                            continue # Try next format
                
                if dt:
                    order['formatted_time'] = dt.isoformat()
                else:
                    logger.error(f"Date parse error for '{dt_str}': All formats failed.")
                    order['formatted_time'] = dt_str # fallback
        return {
            'success': True,
            'count': len(orders),
            'orders': orders
        }
    except Exception as e:
        logger.error(f"Get orders error: {e}")
        return {'success': False, 'error': str(e)}


@router.get("/straddles")
async def api_get_straddles():
    """Get straddles with live calculations"""
    if not state.db:
        logger.warning("DB access in /straddles skipped: database is not available (shutting down?).")
        return {'success': True, 'count': 0, 'straddles': []}

    try:
        straddles_from_db = state.db.get_todays_straddles()

        response_straddles = []
        for trade in straddles_from_db:
            trade_uid = trade.get('straddle_id')
            if not trade_uid:
                continue

            # --- HOTFIX for existing positions with roll_straddle_div = 2.0 ---
            live_config = trade.get('config') or {}
            try:
                if float(live_config.get('roll_straddle_div', 0.2)) == 2.0:
                    live_config['roll_straddle_div'] = 0.2
                    trade['config'] = live_config
                    # Update DB in background
                    asyncio.get_event_loop().run_in_executor(None, state.db.update_straddle_config, trade_uid, live_config, trade.get('sl_points', 0.0))
                    if hasattr(state, 'trade_processes') and trade_uid in state.trade_processes:
                        state.trade_processes[trade_uid]['command_q'].put({
                            'command': 'UPDATE_CONFIG',
                            'data': live_config
                        })
            except Exception as e:
                logger.error(f"Failed to hot-patch roll_straddle_div for {trade_uid}: {e}")
            # -------------------------------------------------------------------

            trade_status = str(trade.get('status', '')).strip().upper()

            if trade_status.startswith('CLOSED'):
                realized_pnl   = trade.get('realized_pnl') or 0.0
                initial_ce_qty = trade.get('initial_ce_quantity', 0)
                initial_pe_qty = trade.get('initial_pe_quantity', 0)

                if initial_ce_qty == 0 and initial_pe_qty == 0:
                    initial_ce_qty = trade.get('ce_quantity', 0)
                    initial_pe_qty = trade.get('pe_quantity', 0)

                num_straddle_units = (initial_ce_qty + initial_pe_qty) / 2.0
                if num_straddle_units <= 0:
                    num_straddle_units = 1
                pnl_per_straddle_calculated = realized_pnl / num_straddle_units

                merged_trade = {
                    **trade,
                    'live_pnl':       realized_pnl,
                    'unrealized_pnl': 0.0,
                    'realized_pnl':   realized_pnl,
                    'pnl_per_lot':    pnl_per_straddle_calculated,
                    'live_net_delta': 0,
                    'net_gamma':      0,
                    'net_theta':      0,
                    'net_vega':       0,
                    'live_positions': [],
                    'pts_out':        0,
                    'points_allowed': 0,
                    'monitors':       None,
                    'events':         trade.get('events', [])
                }
                response_straddles.append(merged_trade)
                continue

            snapshot = state.trade_snapshots.get(trade_uid, {})

            live_positions = snapshot.get('live_positions', [])
            summary_ce_qty = 0
            summary_pe_qty = 0
            unique_strikes = set()

            if live_positions:
                for pos in live_positions:
                    unique_strikes.add(pos['strike'])
                    action     = str(pos.get('action', '')).upper()
                    signed_qty = pos.get('quantity', 0) if action == 'SELL' else -pos.get('quantity', 0)
                    if str(pos.get('option_type', '')).upper() == 'CE':
                        summary_ce_qty += signed_qty
                    elif str(pos.get('option_type', '')).upper() == 'PE':
                        summary_pe_qty += signed_qty

            if len(unique_strikes) > 1:
                display_strike = f"{min(unique_strikes)}-{max(unique_strikes)}"
            elif len(unique_strikes) == 1:
                display_strike = str(list(unique_strikes)[0])
            else:
                display_strike = trade.get('strike')

            points_allowed_from_snapshot = snapshot.get('points_allowed')
            sanitized_points_allowed = None if points_allowed_from_snapshot == float('inf') else points_allowed_from_snapshot

            merged_trade = {
                **trade,
                'strike':             display_strike,
                'ce_quantity':        summary_ce_qty,
                'pe_quantity':        summary_pe_qty,
                'live_pnl':           snapshot.get('total_pnl', 0.0),
                'realized_pnl':       snapshot.get('realized_pnl', 0.0),
                'unrealized_pnl':     snapshot.get('unrealized_pnl', 0.0),
                'pnl_per_lot':        snapshot.get('pnl_per_straddle', 0.0),
                'live_net_delta':     snapshot.get('net_delta'),
                'net_gamma':          snapshot.get('net_gamma'),
                'net_theta':          snapshot.get('net_theta'),
                'net_vega':           snapshot.get('net_vega'),
                'live_positions':     snapshot.get('live_positions', []),
                'pts_out':            snapshot.get('pts_out'),
                'points_allowed':     sanitized_points_allowed,
                'roll_trigger_price': snapshot.get('roll_trigger_price'),
            }

            manager   = get_trade_manager(trade_uid)
            event_bus = get_event_bus()

            if manager:
                live_config = trade.get('config', {})
                merged_trade['monitors'] = {
                    'sl': {
                        'running':    manager.sl_monitor.running,
                        'interval':   manager.sl_monitor.interval,        # ← FIXED: was sl_monitor_interval
                        'sl_bps':     manager.sl_monitor.sl_bps,
                        'sl_points':  manager.sl_monitor.sl_points,
                        'start_time': live_config.get('sl_start_time') or "Trade Start",
                    },
                    'hedge': {
                        'running':      manager.hedge_monitor.running,
                        'interval':     manager.hedge_monitor.interval,   # ← FIXED: was hedge_monitor_interval
                        'hedge_div':    manager.hedge_monitor.hedge_div,
                        'straddle_div': manager.hedge_monitor.straddle_div,
                        'start_time':   live_config.get('hedge_start_time') or "Trade Start",
                    },
                    'roll': {
                        'running':           manager.roll_monitor.running,
                        'interval':          manager.roll_monitor.interval,  # ← FIXED: was roll_monitor_interval
                        'roll_straddle_div': 0.2 if manager.roll_monitor.roll_straddle_div in [2, 2.0, "2", "2.0"] else manager.roll_monitor.roll_straddle_div,
                        'start_time':        live_config.get('roll_start_time') or "Trade Start",
                    },
                    'square_off': {
                        'running':   manager.square_off_monitor.running,
                        'exit_time': manager.square_off_monitor.exit_time_str or "Not Set"
                    }
                }

            if event_bus:
                trade_events = event_bus.get_trade_events(trade_uid)
                merged_trade['events'] = [
                    {
                        'timestamp': evt.timestamp.strftime('%H:%M:%S'),
                        'type':      evt.event_type,
                        'priority':  EventPriority(evt.priority).name
                    }
                    for evt in reversed(trade_events[-5:])
                ]

            response_straddles.append(merged_trade)

        return {
            'success':   True,
            'count':     len(response_straddles),
            'straddles': response_straddles
        }
    except Exception as e:
        logger.error(f"Get straddles error: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

@router.get("/straddles/active")
async def api_get_active_straddles():
    """Get active straddles"""
    try:
        straddles = state.db.get_active_straddles()
        return {
            'success': True,
            'count': len(straddles),
            'straddles': straddles
        }
    except Exception as e:
        logger.error(f"Get active straddles error: {e}")
        return {'success': False, 'error': str(e)}


@router.get("/pnl")
async def api_get_pnl():
    """Get live PnL"""
    try:
        pnl = get_live_pnl_data()
        return {
            'success': True,
            'data': pnl,
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Get PnL error: {e}")
        return {'success': False, 'error': str(e)}


@router.post("/straddle/square-off/{trade_uid}", tags=["Straddle Actions"])
async def api_square_off_straddle(trade_uid: str):
    """Manual square-off"""
    try:
        event_bus = get_event_bus()
        if not event_bus:
            raise HTTPException(status_code=503, detail="Event bus not available.")

        trade = state.db.get_straddle_by_id(trade_uid)
        if not trade:
            raise HTTPException(status_code=404, detail=f"Trade {trade_uid} not found.")

        # Allow square-off for most non-closed statuses
        trade_status = trade.get('status')
        if trade_status.startswith('CLOSED'):
             raise HTTPException(status_code=400, detail=f"Trade {trade_uid} is already closed.")

        logger.info(f"📥 API: Manual SQUARE-OFF request for {trade_uid}")

        await event_bus.emit(
            event_type="square_off_needed",
            trade_uid=trade_uid,
            priority=EventPriority.SQUARE_OFF,
            data={'manual_trigger': True, 'reason': 'Manual square-off from API'}
        )

        return {
            'success': True,
            'message': f'Manual square-off for {trade_uid} has been queued.'
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Square-off API error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/straddle/partial-square-off/{trade_uid}", tags=["Straddle Actions"])
async def api_partial_square_off_straddle(trade_uid: str, request: PartialSquareOffRequest):
    """Manual partial square-off of a percentage of the current position."""
    try:
        event_bus = get_event_bus()
        if not event_bus:
            raise HTTPException(status_code=503, detail="Event bus not available.")

        # Check if trade is active
        trade = state.db.get_straddle_by_id(trade_uid)
        if not trade or trade.get('status') != 'ACTIVE':
            raise HTTPException(status_code=404, detail=f"Trade {trade_uid} not found or not active.")

        logger.info(f"📥 API: Partial square-off request for {trade_uid} ({request.percentage}%)")

        # Emit event to the bus
        await event_bus.emit(
            event_type="partial_square_off_needed",
            trade_uid=trade_uid,
            priority=EventPriority.SQUARE_OFF, # Same priority as full square-off
            data={'percentage': request.percentage}
        )

        return {
            'success': True,
            'message': f'Partial square-off request for {request.percentage}% of {trade_uid} has been queued.'
        }
            
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Partial square-off API error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/straddle/manual-hedge/{trade_uid}", tags=["Straddle Actions"])
async def api_manual_hedge(trade_uid: str):
    """Manually triggers a hedge action by directly queueing an event."""
    try:
        event_bus = get_event_bus()
        if not event_bus:
            raise HTTPException(status_code=503, detail="Event bus not available.")

        trade = state.db.get_straddle_by_id(trade_uid)
        if not trade:
            raise HTTPException(status_code=404, detail=f"Trade {trade_uid} not found.")
        
        trade_status = trade.get('status')
        if trade_status not in ['ACTIVE', 'PARTIAL-SQF']:
            raise HTTPException(status_code=400, detail=f"Trade {trade_uid} is not in a hedgeable state (status: {trade_status}).")

        logger.info(f"📥 API: Manual HEDGE EXECUTION request for {trade_uid}")

        # Create a fresh snapshot to get the current delta
        snapshot = await _trigger_and_get_snapshot(trade_uid)
        if not snapshot:
            raise HTTPException(status_code=500, detail=f"Could not get fresh snapshot for {trade_uid} after triggering.")

        net_delta = snapshot.get('net_delta', 0.0)

        if abs(net_delta) < 1.0:
            return {
                'success': True,
                'message': f'Manual hedge for {trade_uid} skipped: Net delta ({net_delta:.2f}) is negligible.'
            }

        target_delta_reduction = -net_delta
        
        hedge_params = {
            "net_delta": net_delta,
            "target_delta_reduction": target_delta_reduction,
            "trigger_time": get_ist_now(),
            "manual_trigger": True,
            "atm_strike": snapshot.get('strike')
        }
        
        await event_bus.emit(
            event_type="hedge_needed",
            trade_uid=trade_uid,
            priority=EventPriority.HEDGE,
            data=hedge_params
        )

        return {
            'success': True,
            'message': f'Manual hedge for {trade_uid} has been queued. Target Delta Reduction: {target_delta_reduction:.2f}'
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Manual hedge API error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/straddle/manual-roll/{trade_uid}", tags=["Straddle Actions"])
async def api_manual_roll(trade_uid: str):
    """Manually triggers a roll action by directly queueing an event."""
    try:
        event_bus = get_event_bus()
        if not event_bus:
            raise HTTPException(status_code=503, detail="Event bus not available.")

        trade = state.db.get_straddle_by_id(trade_uid)
        if not trade or trade.get('status') != 'ACTIVE':
            raise HTTPException(status_code=400, detail=f"Trade {trade_uid} is not active.")

        logger.info(f"📥 API: Manual ROLL EXECUTION request for {trade_uid}")

        # Emit the event directly to the bus. The handler will do the work.
        await event_bus.emit(
            event_type="roll_needed",
            trade_uid=trade_uid,
            priority=EventPriority.ROLL,
            data={'manual_trigger': True}
        )

        return {
            'success': True,
            'message': f'Manual roll for {trade_uid} has been queued.'
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Manual roll API error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/straddle/manual-verify/{trade_uid}", tags=["Straddle Actions"])
async def api_manual_verify(trade_uid: str):
    """Manually triggers a full verification and sync for a given trade."""
    try:
        logger.info(f"📥 API: Manual verification request for {trade_uid}")
        result = await manual_sync_trade_orders(trade_uid)
        
        if result and result.get('success'):
            return {
                'success': True,
                'message': result.get('message', 'Manual verification completed.')
            }
        else:
            error_msg = result.get('error', 'Manual verification failed.')
            raise HTTPException(status_code=500, detail=error_msg)

    except Exception as e:
        logger.error(f"Manual verification API error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
        
@router.get("/diagnostics/parity-check", tags=["Diagnostics"])
async def api_check_parity():
    """
    Compares the broker's order book with the local database for ALL detected trades.
    Returns a report of discrepancies between DB quantity and Broker Net quantity.
    """
    logger.info("🔍 DIAGNOSTICS: Starting full parity check...")

    loop = asyncio.get_event_loop()

    # --- FIX: Use a temporary, fresh login for parity check to avoid token conflicts ---
    try:
        from Connect import XTSConnect
        import cred
        
        logger.info("   - Creating a temporary XTS session for parity check...")
        temp_xt = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
        login_resp = await loop.run_in_executor(None, temp_xt.interactive_login)
        if not login_resp or login_resp.get('type') != 'success':
            raise HTTPException(status_code=503, detail=f"Temporary login failed: {login_resp.get('description')}")

        temp_xt.isInvestorClient = False
        client_id = getattr(cred, 'clientID', login_resp['result'].get('userID'))
        
        order_book_func = functools.partial(temp_xt.get_order_book, clientID=client_id)
        response = await loop.run_in_executor(None, order_book_func)
    except Exception as e:
        logger.error(f"Failed to fetch order book: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    # --- END FIX ---

    if not response or response.get('type') != 'success':
        raise HTTPException(status_code=500, detail=f"Broker error: {response}")

    broker_orders = response.get('result', [])
    logger.info(f"🔍 Fetched {len(broker_orders)} orders from broker.")

    # 2. Fetch Local DB Data
    db_straddles_all = await loop.run_in_executor(None, state.db.get_todays_straddles)
    db_straddles_map = {s['trade_uid']: s for s in db_straddles_all}

    # 3. Group Broker Orders by Trade UID
    broker_map = defaultdict(lambda: {'orders': []})
    
    # Regex matches standard UIDs: prefix(2) + 12 digits + optional suffix(1) to handle 20-char truncation
    uid_pattern = re.compile(r'((?:ny|sx|bn|fn|mc)\d{12}[a-z]?)(?:_.*)?')
    
    # Create reverse map from base_uid to full trade_uid
    truncated_to_full_uid = {uid[:-1]: uid for uid in db_straddles_map.keys() if len(uid) > 1}

    for order in broker_orders:
        ouid = order.get('OrderUniqueIdentifier', '')
        trade_uid = None

        # Extract UID from OrderUniqueIdentifier
        if ouid:
            match = uid_pattern.search(ouid)
            if match:
                extracted_uid = match.group(1)
                # Resolve truncated UID back to full UID if necessary
                if extracted_uid in truncated_to_full_uid:
                    trade_uid = truncated_to_full_uid[extracted_uid]
                else:
                    trade_uid = extracted_uid
        
        # If valid trade_uid found, add to map
        if trade_uid:
            broker_map[trade_uid]['orders'].append(order)

    # 4. Compare
    discrepancies = []
    all_uids = set(broker_map.keys()) | set(db_straddles_map.keys())
    
    for uid in all_uids:
        b_data = broker_map.get(uid, {'orders': []})
        d_data = db_straddles_map.get(uid, {})

        # -- DB State --
        d_ce_qty = d_data.get('ce_quantity', 0)
        d_pe_qty = d_data.get('pe_quantity', 0)
        d_status = d_data.get('status', 'NOT_IN_DB')
        ce_token = int(d_data.get('ce_token') or 0)
        pe_token = int(d_data.get('pe_token') or 0)

        # -- Broker State (Calculate Net Open) --
        net_ce = 0
        net_pe = 0
        
        for order in b_data['orders']:
            status = str(order.get('OrderStatus', '')).upper()
            if status not in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']:
                continue
            
            qty = int(order.get('CumulativeQuantity') or order.get('FilledQty') or 0)
            side = str(order.get('OrderSide', '')).upper()
            token = int(order.get('ExchangeInstrumentID') or 0)

            # Sign: SELL adds to Short Position (+), BUY removes from Short Position (-)
            signed_qty = qty if side == 'SELL' else -qty

            if token == ce_token and ce_token != 0:
                net_ce += signed_qty
            elif token == pe_token and pe_token != 0:
                net_pe += signed_qty
            # Note: We ignore tokens that don't match current DB tokens (e.g. previous rolls)
            # strictly for the purpose of checking current open position parity.

        diff_ce = d_ce_qty - net_ce
        diff_pe = d_pe_qty - net_pe

        # If there is a mismatch, record it
        if diff_ce != 0 or diff_pe != 0:
            discrepancies.append({
                'trade_uid': uid,
                'status': d_status,
                'db_ce': d_ce_qty,
                'broker_ce': net_ce,
                'diff_ce': diff_ce,
                'db_pe': d_pe_qty,
                'broker_pe': net_pe,
                'diff_pe': diff_pe
            })

    return {
        'success': True,
        'total_trades_checked': len(all_uids),
        'discrepancies': discrepancies
    }

@router.post("/straddle/cancel-action/{trade_uid}", tags=["Straddle Actions"])
async def api_cancel_action(trade_uid: str):
    """Sets a flag to cancel an ongoing long-running action for a trade."""
    try:
        trade = state.db.get_straddle_by_id(trade_uid)
        if not trade:
            raise HTTPException(status_code=404, detail=f"Trade {trade_uid} not found.")

        # Set the cancellation flag. The running task will check this flag.
        if not hasattr(state, 'cancellation_flags'):
            state.cancellation_flags = {}
        state.cancellation_flags[trade_uid] = True

        logger.warning(f"🛑 API: User requested to CANCEL ongoing action for {trade_uid}")

        return {
            'success': True,
            'message': f'Cancellation request for {trade_uid} has been sent. The task will stop at the next checkpoint.'
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Cancel action API error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

async def get_positions() -> list:
    """Helper to get broker positions asynchronously."""
    client = getattr(state, 'xt_i', None)
    if not client:
        raise Exception("Interactive client not initialized")

    loop = asyncio.get_event_loop()
    # Run the synchronous SDK call in a thread pool to avoid blocking
    response = await loop.run_in_executor(None, client.get_position_daywise)

    if response and response.get('type') == 'success':
        result = response.get('result', {})
        if isinstance(result, dict):
            return result.get('positionList', []) or result.get('PositionList', [])
        elif isinstance(result, list):
            return result
    logger.error(f"Failed to get positions: {response}")
    return []

@router.get("/positions")
async def api_get_positions():
    """Get broker positions"""
    try:
        positions = await get_positions()
        return {
            'success': True,
            'count': len(positions),
            'positions': positions
        }
    except Exception as e:
        logger.error(f"Get positions error: {e}")
        return {'success': False, 'error': str(e)}


@router.get("/latest-idv", tags=["Market Data"])
async def api_get_latest_idv():
    """Reads the latest calculated IDVs from the shared CSV file."""
    # The path from the user's Python script
    idv_file_path = r"\\172.16.1.85\Shared\Aryan\LATEST_INDEX_IDV.csv"
    try:
        if not os.path.exists(idv_file_path):
            raise HTTPException(status_code=404, detail="Latest IDV file not found on the server.")
        
        df = pd.read_csv(idv_file_path)
        
        # Convert to a dictionary of {Index: IDV}
        idv_map = pd.Series(df.IDV.values, index=df.Index).to_dict()
        
        return {"success": True, "data": idv_map}
        
    except Exception as e:
        logger.error(f"Failed to read or process latest IDV file: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to process IDV file: {str(e)}")
