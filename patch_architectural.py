import re, os

fpath = 'trading/builder.py'

if not os.path.exists(fpath):
    print(f"Error: {fpath} not found.")
    exit(1)

with open(fpath, 'r', encoding='utf-8') as f:
    code = f.read()

# 1. Completely replace execute_prepared_build to fetch live market data
new_execute = '''async def execute_prepared_build(trade_uid: str):
    loop = asyncio.get_running_loop()
    trade_data = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)

    if not trade_data:
        logger.error(f"[{trade_uid}] Execute live build failed: Trade not found in DB.")
        return

    logger.info(f"[{trade_uid}] Live execution triggered! Calculating delta-neutral and executing now.")
    trade_config = trade_data.get("config", {})
    if "entry_at_straddle" in trade_config:
        del trade_config["entry_at_straddle"]

    symbol = trade_data.get("symbol", "NIFTY")
    lots = trade_data.get("lots", 1)
    delta_neutral = trade_data.get("delta_neutral", True)
    product_type = trade_data.get("product_type", "MIS")

    try:
        await build_straddle(
            symbol=symbol,
            lots=lots,
            trade_uid=trade_uid,
            delta_neutral=delta_neutral,
            product_type=product_type,
            trade_config=trade_config
        )
    except Exception as e:
        logger.error(f"[{trade_uid}] Error during live build execution: {e}", exc_info=True)
        try:
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'FAILED_BUILD')
        except Exception as db_e:
            logger.error(f"[{trade_uid}] CRITICAL: Failed to update status: {db_e}")'''

code = re.sub(r'async def execute_prepared_build\(trade_uid: str\):.*?async def build_multi_straddle', new_execute + r'\n\nasync def build_multi_straddle', code, flags=re.DOTALL)

# 2. Fix the Process Pickling AuthenticationString Error
code = code.replace("state.trade_processes[trade_uid]['process'] = process", "")

# 3. Remove the old Waiting for Entry block further down in the logic
code = re.sub(r'if current_straddle < entry_target:.*?target_straddle": entry_target,\n\s+\}', '', code, flags=re.DOTALL)

# 4. Insert the new Entry Check logic higher up in the flow, providing default KeyError fixes
if 'FIX 1: Added all required fields' not in code:
    anchor = "current_straddle = ce_ltp + pe_ltp"
    
    new_entry = '''current_straddle = ce_ltp + pe_ltp

        entry_target = trade_config.get("entry_at_straddle")
        if entry_target in ("", None, 0, "0"):
            entry_target = None
        elif entry_target is not None:
            try:
                entry_target = float(entry_target)
            except Exception:
                entry_target = None

        if entry_target is not None:
            logger.debug("=" * 100)
            logger.info(f"[ENTRY CHECK] Current={current_straddle:.2f} | Target={entry_target:.2f}")
            logger.info(f"Current < Target  : {current_straddle < entry_target}")
            logger.debug("=" * 100)

            if current_straddle < entry_target:
                logger.info(f"[{trade_uid}] [WAITING FOR ENTRY] Current {current_straddle:.2f} < target {entry_target:.2f}. Preparing trade.")
                
                # FIX 1: Added all required fields to prevent KeyError: 'strike'
                pending_data = {
                    'trade_uid': trade_uid, 
                    'straddle_id': trade_uid, 
                    'symbol': symbol,
                    'strike': atm,  
                    'expiry': chain_data['expiry'],
                    'ce_token': ce_token,
                    'pe_token': pe_token,
                    'ce_quantity': 0,
                    'pe_quantity': 0,
                    'ce_entry_price': 0.0,
                    'pe_entry_price': 0.0,
                    'total_premium': 0.0,
                    'status': 'PENDING_ENTRY', 
                    'config': trade_config, 
                    'lots': lots,
                    'entry_timestamp': get_ist_now().isoformat(),
                }
                await loop.run_in_executor(None, state.db.insert_straddle, pending_data)

                from trading.trade_process import trade_process_worker_entry
                command_q = multiprocessing.Queue()
                process = multiprocessing.Process(
                    target=trade_process_worker_entry,
                    args=(trade_uid, pending_data, command_q, getattr(state, 'trade_data_cache', None) or {}, []),
                    daemon=True, name=f"trade-{trade_uid}"
                )
                process.start()
                
                # FIX 2: Do NOT put the process object in the shared dictionary
                state.trade_processes[trade_uid] = {'pid': process.pid, 'status': 'PENDING_ENTRY'}
                state.local_process_refs[trade_uid] = process
                state.local_command_queues[trade_uid] = command_q
                
                logger.info(f"Process for {trade_uid} started in PENDING_ENTRY mode (PID: {process.pid}).")

                return {
                    "success": False,
                    "pending_entry": True,
                    "trade_uid": trade_uid,
                    "current_straddle": current_straddle,
                    "target_straddle": entry_target,
                }
            else:
                logger.info("[ENTRY STRADDLE] Target satisfied. Proceeding to calculate live spot and delta.")'''

    code = code.replace(anchor, new_entry, 1)

with open(fpath, 'w', encoding='utf-8') as f:
    f.write(code)

print("Successfully patched trading/builder.py with structural fixes and crash resolutions!")
