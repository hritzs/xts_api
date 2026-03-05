"""
API Routes - UPDATED with correct imports
"""
import asyncio
from fastapi import APIRouter, HTTPException
import multiprocessing
from datetime import datetime
from utils.logger import logger
from market_data import get_option_chain
from trading.builder import build_straddle, manual_sync_trade_orders
from trading.config_builder import build_with_config
from trading.trade_manager import create_trade_manager, get_trade_manager
from trading.event_bus import get_event_bus, EventPriority
from trading.trade_process import trade_process_worker_entry
from models.state import state
from background.tasks import broadcast_message, get_live_pnl_data, create_snapshot_for_trade
from utils.helpers import get_ist_now
from pydantic import BaseModel, Field

# Import from models/schemas.py
from models.schemas import (
    StraddleRequest, ConfigBuildRequest, ConfigBuildResponse,
    HealthResponse, APIResponse, OrderBookResponse, PositionsResponse,
    StraddlesResponse, PnLResponse, OptionChainResponse
)

class PartialSquareOffRequest(BaseModel):
    percentage: float = Field(..., gt=0, le=100, description="Percentage of the original position to square off.")

router = APIRouter(prefix="/api")


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
async def api_get_option_chain(symbol: str, strike_range: int = 5):
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
        timestamp = now.strftime("%d%m%y%H%M%S")
        
        logger.info(f"📥 API: Manual straddle sell request for {request.symbol}")
        
        # For manual trades, create a default config to pass to the builder.
        # The builder will then persist this config and the worker process will use it.
        default_config = {
            "symbol": request.symbol, "size": request.lots, "entry_time": None, "exit_time": None,
            "sl_bps": 14, "hedge_div": 19, "straddle_div": 3, "roll_straddle_div": 2,
            "hedge_monitor_interval": 60.0, "sl_monitor_interval": 60.0, "roll_monitor_interval": 60.0,
            "roll_flag_check_interval": 60.0, "hedge_frac": 1.0, "straddle_price_drop_trigger": 0.0,
            "straddle_price_monitor_interval": 5.0, "hedge_start_time": None, "sl_start_time": None,
            "roll_start_time": None,
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
            return {
                'success': False,
                'error': 'Order placement failed',
                'timestamp': now.isoformat()
            }
            
    except Exception as e:
        logger.error(f"❌ API error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e),
            'timestamp': now.isoformat()
        }


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
        
        pending_trade_data = {
            'trade_uid': trade_uid, 'straddle_id': trade_uid, 'symbol': request.symbol, 'status': 'PENDING',
            'monitors': monitors_config, 'config': config, 'entry_timestamp': now.isoformat(), 'strike': 'N/A', 'ce_symbol': 'N/A', 'pe_symbol': 'N/A',
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
    # --- FIX: Add a check to prevent DB access during shutdown ---
    if not state.db:
        logger.warning("DB access in /straddles skipped: database is not available (shutting down?).")
        return {'success': True, 'count': 0, 'straddles': []}
    # --- END FIX ---

    try:
        straddles_from_db = state.db.get_todays_straddles()
        
        response_straddles = []
        for trade in straddles_from_db:
            trade_uid = trade.get('straddle_id')
            if not trade_uid:
                continue

            trade_status = str(trade.get('status', '')).strip().upper()

            # If trade is closed, use stored data and don't fetch live snapshot
            if trade_status.startswith('CLOSED'):
                realized_pnl = trade.get('realized_pnl') or 0.0
                # --- Calculate PnL per straddle unit for consistency ---
                initial_ce_qty = trade.get('initial_ce_quantity', 0)
                initial_pe_qty = trade.get('initial_pe_quantity', 0)
                
                # --- FIX: Add a fallback to the main quantity fields if initial quantities are missing ---
                if initial_ce_qty == 0 and initial_pe_qty == 0:
                    initial_ce_qty = trade.get('ce_quantity', 0)
                    initial_pe_qty = trade.get('pe_quantity', 0)
                # --- END FIX ---

                # Ensure num_straddle_units is at least 1 to avoid division by zero if quantities are somehow 0
                # for a trade that should have had positions.
                num_straddle_units = (initial_ce_qty + initial_pe_qty) / 2.0
                if num_straddle_units <= 0:
                    num_straddle_units = 1 # Prevent division by zero for trades with no initial quantity data
                pnl_per_straddle_calculated = realized_pnl / num_straddle_units

                merged_trade = {
                    **trade,
                    'live_pnl': realized_pnl,  # Use stored PnL
                    'unrealized_pnl': 0.0,
                    'realized_pnl': realized_pnl,
                    # The UI expects 'pnl_per_lot', so we map our per-straddle calculation to this key.
                    'pnl_per_lot': pnl_per_straddle_calculated,
                    'live_net_delta': 0, # It's closed, so delta is 0
                    'net_gamma': 0,
                    'net_theta': 0,
                    'net_vega': 0,
                    'live_positions': [], # No live positions
                    'pts_out': 0,
                    'points_allowed': 0,
                    'monitors': None, # No active monitors
                    'events': trade.get('events', []) # Keep existing events if any
                }
                response_straddles.append(merged_trade)
                continue

            # Get the live snapshot for this trade, which is the single source of truth
            snapshot = state.trade_snapshots.get(trade_uid, {})

            # --- AGGREGATION FOR SUMMARY VIEW ---
            # Instead of using stale DB data, we derive the summary from the live snapshot.
            live_positions = snapshot.get('live_positions', [])
            summary_ce_qty = 0
            summary_pe_qty = 0
            unique_strikes = set()

            if live_positions:
                for pos in live_positions:
                    # The snapshot 'quantity' is the net open quantity for that instrument.
                    unique_strikes.add(pos['strike'])

                    # Correctly calculate net short quantity. SELL is positive (short), BUY is negative (long).
                    # Making the check case-insensitive and safer with .get() for added robustness.
                    action = str(pos.get('action', '')).upper()
                    signed_qty = pos.get('quantity', 0) if action == 'SELL' else -pos.get('quantity', 0)

                    if str(pos.get('option_type', '')).upper() == 'CE':
                        summary_ce_qty += signed_qty
                    elif str(pos.get('option_type', '')).upper() == 'PE':
                        summary_pe_qty += signed_qty
            
            # Determine a representative strike for display in the summary table.
            if len(unique_strikes) > 1:
                # For multi-strike positions (e.g., after a roll), show a range.
                display_strike = f"{min(unique_strikes)}-{max(unique_strikes)}"
            elif len(unique_strikes) == 1:
                display_strike = str(list(unique_strikes)[0])
            else:
                # Fallback to the original strike if no live positions are found.
                display_strike = trade.get('strike')
            
            # --- FIX: Sanitize float('inf') for JSON compatibility ---
            points_allowed_from_snapshot = snapshot.get('points_allowed')
            sanitized_points_allowed = None if points_allowed_from_snapshot == float('inf') else points_allowed_from_snapshot
            # --- END FIX ---

            merged_trade = {
                **trade,
                'strike': display_strike,
                'ce_quantity': summary_ce_qty,
                'pe_quantity': summary_pe_qty,  
                'live_pnl': snapshot.get('total_pnl', 0.0),
                'realized_pnl': snapshot.get('realized_pnl', 0.0),
                'unrealized_pnl': snapshot.get('unrealized_pnl', 0.0),
                'pnl_per_lot': snapshot.get('pnl_per_straddle', 0.0), # Use the new per-straddle value
                'live_net_delta': snapshot.get('net_delta'),
                'net_gamma': snapshot.get('net_gamma'),
                'net_theta': snapshot.get('net_theta'),
                'net_vega': snapshot.get('net_vega'),
                'live_positions': snapshot.get('live_positions', []),
                'pts_out': snapshot.get('pts_out'),
                'points_allowed': sanitized_points_allowed,
                'roll_trigger_price': snapshot.get('roll_trigger_price'),
            }

            # Add monitor and event bus data
            manager = get_trade_manager(trade_uid)
            event_bus = get_event_bus()

            if manager:
                live_config = trade.get('config', {})
                merged_trade['monitors'] = {
                    'sl': {
                        'running': manager.sl_monitor.running,
                        'interval': manager.sl_monitor.sl_monitor_interval,
                        'sl_bps': manager.sl_monitor.sl_bps,
                        'sl_points': manager.sl_monitor.sl_points,
                        'start_time': live_config.get('sl_start_time') or "Trade Start",
                    },
                    'hedge': {
                        'running': manager.hedge_monitor.running,
                        'interval': manager.hedge_monitor.hedge_monitor_interval,
                        'hedge_div': manager.hedge_monitor.hedge_div,
                        'straddle_div': manager.hedge_monitor.straddle_div,
                        'start_time': live_config.get('hedge_start_time') or "Trade Start",
                    },
                    'roll': {
                        'running': manager.roll_monitor.running,
                        'interval': manager.roll_monitor.roll_monitor_interval,
                        'roll_straddle_div': manager.roll_monitor.roll_straddle_div,
                        'start_time': live_config.get('roll_start_time') or "Trade Start",
                    },
                    'square_off': {
                        'running': manager.square_off_monitor.running,
                        'exit_time': manager.square_off_monitor.exit_time_str or "Not Set"
                    }
                }

            if event_bus:
                trade_events = event_bus.get_trade_events(trade_uid)
                merged_trade['events'] = [
                    {'timestamp': evt.timestamp.strftime('%H:%M:%S'), 'type': evt.event_type, 'priority': EventPriority(evt.priority).name}
                    for evt in reversed(trade_events[-5:])  # latest 5
                ]
            
            response_straddles.append(merged_trade)

        return {
            'success': True,
            'count': len(response_straddles),
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
        # Use the dedicated helper which correctly reconstructs all positions
        from trading.square_off import square_off_by_trade_uid
        from trading.trade_manager import get_trade_manager, remove_trade_manager
        
        # This function correctly finds the trade and all its associated positions (including hedges/rolls)
        # before executing the square-off.
        result = await square_off_by_trade_uid(trade_uid)
        
        if result and result['success']:
            # --- FIX: Check if this was just a dispatch to a worker process ---
            if "dispatched" in result.get('message', ''):
                logger.info(f"Square-off command for {trade_uid} was dispatched to worker. Worker will handle final status.")
                return result # Return the dispatch message to the client
            # --- END FIX ---

            # This block now only runs if the square-off was executed synchronously in the main process.
            loop = asyncio.get_event_loop()
            pnl_result = result.get('pnl')
            realized_pnl = pnl_result.get('realized_pnl', 0.0) if pnl_result else 0.0
            
            trade_to_close = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
            if trade_to_close:
                trade_to_close['status'] = 'CLOSED'
                trade_to_close['realized_pnl'] = realized_pnl
                await loop.run_in_executor(None, state.db.insert_straddle, trade_to_close)
                logger.info(f"✅ Saved realized PnL (₹{realized_pnl:,.2f}) and set status to CLOSED for manual square-off of {trade_uid}.")
            
            manager = get_trade_manager(trade_uid)
            if manager:
                # The square_off function stops the monitors, but we still need to remove the in-memory manager instance.
                remove_trade_manager(trade_uid)
            
            await broadcast_message({
                'type': 'straddle_closed',
                'trade_uid': trade_uid,
                'data': result,
                'timestamp': datetime.now().isoformat()
            })
            
            return {
                'success': True,
                'trade_uid': trade_uid,
                'data': result,
                'message': f'Straddle squared off: {trade_uid}'
            }
        else:
            # If square-off fails, decide whether to revert the status.
            error_details = result.get('error') if result else 'Unknown error'
            # Only revert to ACTIVE if the failure was not due to an invalid initial state.
            # If the trade was already in a non-active state (e.g., FAILED_FILTER), we should not change its status.
            if "status is" in str(error_details):
                logger.warning(f"Square-off for {trade_uid} rejected due to invalid initial status. Status remains unchanged.")
            else:
                # The trade was likely active, but the square-off failed mid-process. Revert to ACTIVE for monitoring.
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                logger.warning(f"⚠️ Status for {trade_uid} reverted to ACTIVE after failed manual square-off.")

            return {'success': False, 'error': 'Square-off failed', 'details': error_details}
            
    except Exception as e:
        logger.error(f"Square-off error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {'success': False, 'error': str(e)}

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

        # --- FIX: Provide more specific feedback for non-active trades ---
        trade = state.db.get_straddle_by_id(trade_uid)
        if not trade:
            raise HTTPException(status_code=404, detail=f"Trade {trade_uid} not found.")
        
        trade_status = trade.get('status')
        if trade_status != 'ACTIVE':
            raise HTTPException(status_code=400, detail=f"Trade {trade_uid} is not active (status: {trade_status}). A hedge may already be in progress.")

        logger.info(f"📥 API: Manual HEDGE EXECUTION request for {trade_uid}")

        # Create a fresh snapshot to get the current delta
        await create_snapshot_for_trade(trade_uid)
        snapshot = state.trade_snapshots.get(trade_uid)
        if not snapshot:
            raise HTTPException(status_code=500, detail=f"Could not create snapshot for {trade_uid}.")

        net_delta = snapshot.get('net_delta', 0.0)

        # Safeguard: Don't hedge if delta is negligible
        if abs(net_delta) < 1.0:
            return {
                'success': True,
                'message': f'Manual hedge for {trade_uid} skipped: Net delta ({net_delta:.2f}) is negligible.'
            }

        # --- FIX: Correctly calculate target_delta_reduction to be signed ---
        # For a manual trigger, we hedge the full delta to bring it to zero.
        target_delta_reduction = -net_delta
        
        # The event handler reads from state.hedge_params, so we must populate it.
        hedge_params = {
            "net_delta": net_delta,
            "target_delta_reduction": target_delta_reduction,
            "trigger_time": get_ist_now(),
            "manual_trigger": True
        }

        if not hasattr(state, 'hedge_params'): state.hedge_params = {}
        state.hedge_params[trade_uid] = hedge_params
        
        # Emit the event to the bus
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
