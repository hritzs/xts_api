"""
Configuration-Based Builder
Builds positions with IV and straddle price filters
"""
import asyncio
from datetime import datetime, time, timedelta
from typing import Optional, Dict
from utils.logger import logger
from models.state import state
from utils.helpers import get_ist_now
from trading.data_client import get_option_chain_from_service



async def build_with_config(config: Dict, trade_uid: str = None) -> Optional[str]:
    """
    Build position with configuration filters.

    Args:
        config: Build configuration dict with:
                - symbol, size, idv, idv_divisor, straddle_filter
                - entry_time, exit_time
                - monitoring intervals, hedge params, sl_bps
        trade_uid: Optional pre-generated trade UID.

    Returns:
        Trade UID if built successfully, None otherwise.
    """
    try:
        # Parse times
        entry_parts = list(map(int, config['entry_time'].split(':')))
        exit_parts = list(map(int, config['exit_time'].split(':')))
        entry_time = time(entry_parts[0], entry_parts[1], entry_parts[2] if len(entry_parts) > 2 else 0)
        exit_time = time(exit_parts[0], exit_parts[1], exit_parts[2] if len(exit_parts) > 2 else 0)

        sl_start_time_str = config.get('sl_start_time')
        hedge_start_time_str = config.get('hedge_start_time')
        roll_start_time_str = config.get('roll_start_time')

        logger.info("="*100)
        logger.info(f"CONFIG BUILD: {config['symbol']}")
        logger.info(f"   Entry: {config['entry_time']}, Exit: {config['exit_time']}")
        logger.info(f"   Size: {config['size']} lots")
        logger.info(f"   IDV: {config['idv']}, Divisor: {config['idv_divisor']}")
        logger.info(f"   Straddle Filter: ₹{config['straddle_filter']}")
        if sl_start_time_str:
            logger.info(f"   SL Start: {sl_start_time_str}")
        if hedge_start_time_str:
            logger.info(f"   Hedge Start: {hedge_start_time_str}")
        if roll_start_time_str:
            logger.info(f"   Roll Start: {roll_start_time_str}")
        logger.info("="*100)

        # ── CHANGE 1: Reduced pre-entry check window from 15s → 2s ──────────
        PRE_ENTRY_CHECK_SECONDS = 2
        now_dt_init = get_ist_now()
        today = now_dt_init.date()
        entry_datetime = datetime.combine(today, entry_time, tzinfo=now_dt_init.tzinfo)
        pre_entry_check_time = (entry_datetime - timedelta(seconds=PRE_ENTRY_CHECK_SECONDS)).time()

        filters_passed_at_least_once = False
        build_triggered = False

        while True:
            if trade_uid and state.cancellation_flags.get(trade_uid):
                logger.info(f"🛑 Build task for {trade_uid} detected cancellation flag. Aborting.")
                state.cancellation_flags.pop(trade_uid, None)
                return None

            now_dt = get_ist_now()
            now_time = now_dt.time()

            # 1. Check for exit condition
            if now_time >= exit_time:
                logger.warning("Exit time reached, aborting build.")
                return None

            # 2. Check if we are in the active period (pre-check window or later)
            if now_time < pre_entry_check_time:
                sleep_until = datetime.combine(today, pre_entry_check_time, tzinfo=now_dt.tzinfo)
                sleep_duration = (sleep_until - now_dt).total_seconds()
                if sleep_duration > 0:
                    logger.info(f"Waiting until pre-entry check time {pre_entry_check_time}...")
                    await asyncio.sleep(sleep_duration)
                continue

            # Check if it's time to build
            if filters_passed_at_least_once and now_time >= entry_time:
                logger.info("="*100)
                logger.info("✅ ALL FILTERS PASSED & ENTRY TIME REACHED — BUILDING POSITION")
                logger.info("="*100)
                build_triggered = True
                break

            # 3. We are in the active period, perform filter checks
            logger.info(f"🔍 CHECKING ENTRY FILTERS @ {now_time}")

            symbol_upper = config['symbol'].upper()
            chain_data = state.get_option_chain(symbol_upper)
            if not chain_data:
                logger.info(f"Config build: Cache miss for {symbol_upper}. Fetching from service...")
                chain_data = await get_option_chain_from_service(symbol_upper)
                if chain_data:
                    state.update_option_chain(symbol_upper, chain_data)

            if not chain_data:
                logger.warning("Could not get option chain from cache or service. Waiting 10s...")
                await asyncio.sleep(10)
                continue

            atm_row = next((row for row in chain_data['chain'] if row['is_atm']), None)
            if not atm_row:
                logger.warning("Could not find ATM row in option chain. Waiting 10s...")
                await asyncio.sleep(10)
                continue

            ce_iv = atm_row.get('ce_iv', 0)
            pe_iv = atm_row.get('pe_iv', 0)
            current_iv = (ce_iv + pe_iv) / 2

            # --- IV FILTER ---
            logger.info("="*100)
            logger.info(f"📊 IV FILTER CHECK: {config['symbol']}")
            idv = float(config['idv'])
            divisor = float(config['idv_divisor'])
            threshold = idv / divisor if divisor > 0 else 0.0
            iv_passed = (current_iv >= threshold) if threshold > 0 else True

            logger.info(f"  Current IV : {current_iv:.2f}")
            logger.info(f"  IDV        : {idv:.2f}")
            logger.info(f"  Divisor    : {divisor:.2f}")
            logger.info(f"  Threshold  : {threshold:.2f}")
            logger.info(f"  Result     : {'✅ PASS' if iv_passed else '❌ FAIL'}")
            logger.info("="*100)

            # --- STRADDLE FILTER ---
            logger.info("="*100)
            logger.info(f"📊 STRADDLE FILTER CHECK: {config['symbol']}")
            ce_ltp = atm_row.get('ce_ltp', 0)
            pe_ltp = atm_row.get('pe_ltp', 0)
            straddle_price = ce_ltp + pe_ltp
            filter_price = float(config['straddle_filter'])
            straddle_passed = (straddle_price > filter_price) if filter_price > 0 else True

            logger.info(f"  ATM Strike     : {atm_row['strike']}")
            logger.info(f"  CE LTP         : {ce_ltp:.2f}")
            logger.info(f"  PE LTP         : {pe_ltp:.2f}")
            logger.info(f"  Straddle Price : {straddle_price:.2f}")
            logger.info(f"  Filter         : {filter_price:.2f}")
            logger.info(f"  Result         : {'✅ PASS' if straddle_passed else '❌ FAIL'}")
            logger.info("="*100)

            all_filters_passed = iv_passed and straddle_passed

            if all_filters_passed:
                logger.info("✅ All entry filters passed. Waiting for entry time.")
                filters_passed_at_least_once = True

            if all_filters_passed:
                if now_time < entry_time:
                    target_entry_dt = datetime.combine(today, entry_time, tzinfo=now_dt.tzinfo)
                    sleep_duration = (target_entry_dt - now_dt).total_seconds()
                    sleep_interval = min(1.0, max(0.1, sleep_duration))
                    logger.info(f"  Waiting for entry time in {sleep_duration:.1f}s...")
                else: # now_time >= entry_time
                    logger.info(f"  ✅ Time reached and filters passed. Proceeding to build sequence...")
                    sleep_interval = 0.1
            else: # Filters failed
                if now_time < entry_time:
                    sleep_interval = 1.0 # Check again soon
                    logger.info(f"  Filters not passed. Re-checking in {sleep_interval:.1f}s.")
                else: # after entry time
                    seconds_until_next_minute = 60 - now_dt.second - (now_dt.microsecond / 1_000_000)
                    sleep_interval = max(1.0, seconds_until_next_minute + 0.2)
                    logger.info(f"  Filters not passed. Re-checking in {sleep_interval:.1f}s.")

            # Cancellable sleep loop
            elapsed = 0.0
            while elapsed < sleep_interval:
                if trade_uid and state.cancellation_flags.get(trade_uid):
                    logger.info(f"🛑 Build task for {trade_uid} detected cancellation flag during sleep. Aborting.")
                    state.cancellation_flags.pop(trade_uid, None)
                    return None
                step = min(1.0, sleep_interval - elapsed)
                await asyncio.sleep(step)
                elapsed += step

        if not build_triggered:
            return None

        # ── Generate trade_uid if not supplied ───────────────────────────────
        if not trade_uid:
            logger.info("No trade_uid provided to build_with_config, generating a new one.")
            timestamp = get_ist_now().strftime("%d%m%y%H%M%S")
            symbol_upper = config['symbol'].upper()
            SYMBOL_PREFIXES = {
                'NIFTY': 'ny', 'SENSEX': 'sx', 'BANKNIFTY': 'bn',
                'FINNIFTY': 'fn', 'MIDCPNIFTY': 'mc',
            }
            sorted_keys = sorted(SYMBOL_PREFIXES.keys(), key=len, reverse=True)
            prefix = next(
                (SYMBOL_PREFIXES[k] for k in sorted_keys if k in symbol_upper),
                symbol_upper[:2].lower()
            )
            base_trade_uid = f"{prefix}{timestamp}"
            trade_uid = base_trade_uid
            suffix_counter = 0
            loop = asyncio.get_event_loop()
            while await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid):
                suffix_counter += 1
                suffix = chr(ord('a') + suffix_counter - 1)
                trade_uid = f"{base_trade_uid}{suffix}"

        # ── Execute the actual build ─────────────────────────────────────────
        config_for_builder = config.copy()
        config_for_builder.pop('order_lots_per_call', None)
        logger.info("Using legacy chunking for config-based build (order_lots_per_call removed).")

        from trading.builder import build_straddle
        result = await build_straddle(
            symbol=config['symbol'],
            lots=config['size'],
            trade_uid=trade_uid,
            delta_neutral=True,
            trade_config=config_for_builder,
            ce_strike_price=config.get('ce_strike_price'),
            pe_strike_price=config.get('pe_strike_price')
        )

        if result and result.get('success'):
            logger.info(f"✅ Position built: {trade_uid}")
            straddle_data = result.get('straddle_data', {})
            entry_spot = straddle_data.get('entry_spot', 0)

            sl_points = config['sl_bps'] * entry_spot / 10000 if entry_spot > 0 else 0
            logger.info(f"  Entry Spot : {entry_spot:.2f}")
            logger.info(f"  SL Points  : {sl_points:.2f} per straddle")
            state.db.update_straddle_config(trade_uid, config, sl_points)

            return trade_uid

        else:
            loop = asyncio.get_event_loop()
            current_trade  = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
            current_status = current_trade.get('status', 'UNKNOWN') if current_trade else 'UNKNOWN'

            # ── CHANGE 2: Guard ALL terminal statuses — never overwrite ──────
            TERMINAL_STATUSES = {
                'CLOSED_SL_BUILD',   # SL hit during build → SQF already ran
                'CLOSED_SQF',        # Already squared off
                'CLOSED_SL',         # Closed by SL monitor
                'CLOSED_ROLL',       # Closed by roll
                'PARTIAL',           # Orders placed but verification failed
            }

            if current_status in TERMINAL_STATUSES:
                if current_status == 'PARTIAL':
                    logger.error(
                        f"⚠️  [{trade_uid}] Build ended PARTIAL — orders were placed but "
                        f"order-book verification failed (network timeout). "
                        f"Preserving PARTIAL status. MANUAL REVIEW REQUIRED."
                    )
                    # Re-affirm PARTIAL so any async DB write race can't overwrite it
                    state.db.update_straddle_status(trade_uid, 'PARTIAL')
                    if not hasattr(state, 'partial_build_uids'):
                        state.partial_build_uids = set()
                    state.partial_build_uids.add(trade_uid)

                elif current_status == 'CLOSED_SL_BUILD':
                    logger.warning(
                        f"⚠️  [{trade_uid}] Build returned None but trade was already "
                        f"closed by in-build SL hit (status=CLOSED_SL_BUILD). "
                        "Preserving closed status — NOT marking as FAILED_FILTER."
                    )

                else:
                    logger.warning(
                        f"⚠️  [{trade_uid}] Build returned None but trade already has "
                        f"terminal status={current_status}. Preserving — NOT overwriting."
                    )

                return None  # Return None but status is already correctly set in DB

            # Only reaches here if status is truly a clean miss (PENDING, UNKNOWN, etc.)
            logger.error(
                f"❌ [{trade_uid}] Build failed completely. "
                f"No orders placed. Current status: {current_status}"
            )
            state.db.update_straddle_status(trade_uid, 'FAILED_FILTER')
            return None

    except Exception as e:
        logger.error(f"Config build error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
