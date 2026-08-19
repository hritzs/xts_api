from pathlib import Path
from datetime import datetime
import shutil
import py_compile

p = Path("trading/square_off.py")

if not p.exists():
    raise SystemExit("ERROR: trading/square_off.py not found.")

stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
backup = p.with_name(
    f"{p.stem}.before_partial_sqf_direct_exec_{stamp}{p.suffix}"
)

shutil.copy2(p, backup)

code = p.read_text(encoding="utf-8")

print("=" * 100)
print("PARTIAL SQUARE-OFF DIRECT EXECUTION PATCH")
print("=" * 100)
print("BACKUP:", backup)

# ============================================================
# 1. REMOVE THE QUEUE-AND-RETURN SHORT CIRCUIT
# ============================================================

old_queue_block = '''        if hasattr(state, 'trade_processes') and trade_uid in state.trade_processes:
            process = getattr(state, 'local_process_refs', {}).get(trade_uid)
            command_q = getattr(state, 'local_command_queues', {}).get(trade_uid)
            if not process or not process.is_alive():
                getattr(state, 'local_process_refs', {}).pop(trade_uid, None)
                getattr(state, 'local_command_queues', {}).pop(trade_uid, None)
                state.trade_processes.pop(trade_uid, None)
                return {'success': False, 'error': 'Trade process is not running.'}
            if not command_q: return {'success': False, 'error': 'Trade command queue missing.'}
            command_q.put({'command': 'PARTIAL_SQUARE_OFF', 'percentage': percentage_of_original})
            return {'success': True, 'message': 'Command dispatched.'}

'''

new_queue_block = '''        # ========================================================
        # PARTIAL SQF DIRECT EXECUTION
        # ========================================================
        # IMPORTANT:
        # Do NOT only enqueue PARTIAL_SQUARE_OFF and return here.
        #
        # The public partial_square_off() call must reach the actual
        # executor.execute_batch() path below so that the requested
        # partial quantity is really submitted.
        #
        # The normal worker/command architecture may still exist,
        # but this function is the execution boundary for a direct
        # partial-square-off request.
        # ========================================================
        if hasattr(state, 'trade_processes') and trade_uid in state.trade_processes:
            process = getattr(state, 'local_process_refs', {}).get(trade_uid)
            command_q = getattr(state, 'local_command_queues', {}).get(trade_uid)

            if process and process.is_alive():
                logger.info(
                    f"⚡ PARTIAL-SQF DIRECT EXECUTION | "
                    f"{trade_uid} | "
                    f"Worker process is active, but execution will continue "
                    f"through square_off.py executor path."
                )
            else:
                logger.warning(
                    f"⚠️ PARTIAL-SQF | "
                    f"Worker reference exists but is not active. "
                    f"Continuing with direct execution."
                )

'''

if old_queue_block not in code:
    raise SystemExit(
        "ERROR: Exact PARTIAL_SQUARE_OFF queue block not found. "
        "NO CHANGES MADE."
    )

code = code.replace(
    old_queue_block,
    new_queue_block,
    1
)

print(
    "PATCHED: partial-square-off no longer returns after "
    "queue dispatch."
)

# ============================================================
# 2. ADD EXPLICIT EXECUTION LOGGING
# ============================================================

old_execute = '''            chunk_result = await executor.execute_batch(chunk_orders, f"PSQF_{trade_uid}_CHUNK{chunk_idx}")
            successful_in_chunk = chunk_result.get('successful_orders', [])
'''

new_execute = '''            logger.warning(
                f"🚨 PARTIAL-SQF ORDER SUBMISSION | "
                f"Trade={trade_uid} | "
                f"Chunk={chunk_idx}/{len(all_chunks)} | "
                f"Orders={len(chunk_orders)} | "
                f"ExitTarget={exit_target}"
            )

            for _order in chunk_orders:
                logger.info(
                    f"   PARTIAL-SQF ORDER | "
                    f"Action={_order.get('action')} | "
                    f"Token={_order.get('token')} | "
                    f"Qty={_order.get('quantity')} | "
                    f"Expected={_order.get('expected_price')}"
                )

            chunk_result = await executor.execute_batch(
                chunk_orders,
                f"PSQF_{trade_uid}_CHUNK{chunk_idx}"
            )

            successful_in_chunk = chunk_result.get(
                'successful_orders',
                []
            )
'''

if old_execute not in code:
    raise SystemExit(
        "ERROR: Partial-square-off execute_batch boundary not found. "
        "NO CHANGES MADE."
    )

code = code.replace(
    old_execute,
    new_execute,
    1
)

print(
    "PATCHED: explicit partial SQF order-submission logging."
)

# ============================================================
# 3. ADD EXPLICIT FILL RECONCILIATION LOGGING
# ============================================================

old_verify_result = '''            all_verified_fills.extend(verified_fills_for_chunk)

        batch_execution_time = (datetime.now() - batch_execution_start).total_seconds()
'''

new_verify_result = '''            all_verified_fills.extend(verified_fills_for_chunk)

            # ----------------------------------------------------
            # ACTUAL PARTIAL-SQF FILL RECONCILIATION
            # ----------------------------------------------------
            verified_qty = 0

            for _fill in verified_fills_for_chunk:
                try:
                    verified_qty += int(
                        _fill.get('CumulativeQuantity')
                        or _fill.get('filled_qty')
                        or 0
                    )
                except (TypeError, ValueError):
                    pass

            logger.warning(
                f"✅ PARTIAL-SQF FILL RESULT | "
                f"Trade={trade_uid} | "
                f"Chunk={chunk_idx} | "
                f"Placed={len(successful_in_chunk)} | "
                f"VerifiedFillQty={verified_qty} | "
                f"VerifiedFillCount={len(verified_fills_for_chunk)}"
            )

            if verified_fills_for_chunk:
                for _fill in verified_fills_for_chunk:
                    logger.info(
                        f"   PARTIAL-SQF VERIFIED FILL | "
                        f"Token={_fill.get('ExchangeInstrumentID') or _fill.get('exchange_instrument_id')} | "
                        f"Qty={_fill.get('CumulativeQuantity') or _fill.get('filled_qty')} | "
                        f"Price={_fill.get('OrderAverageTradedPrice') or _fill.get('fill_price')}"
                    )
            else:
                logger.warning(
                    f"⚠️ PARTIAL-SQF CHUNK {chunk_idx} "
                    f"produced NO VERIFIED FILLS."
                )

        batch_execution_time = (datetime.now() - batch_execution_start).total_seconds()
'''

if old_verify_result not in code:
    raise SystemExit(
        "ERROR: Partial-square-off fill reconciliation boundary not found. "
        "NO CHANGES MADE."
    )

code = code.replace(
    old_verify_result,
    new_verify_result,
    1
)

print(
    "PATCHED: explicit partial SQF fill reconciliation logging."
)

# ============================================================
# 4. ADD FINAL PARTIAL EXIT SUMMARY
# ============================================================

old_return = '''        return {'success': is_successful, 'successful_count': len(all_successful_orders), 'failed_count': len(all_failed_orders), 'execution_time': batch_execution_time, 'trade_uid': trade_uid}
'''

new_return = '''        logger.warning(
            f"🏁 PARTIAL-SQF EXECUTION COMPLETE | "
            f"Trade={trade_uid} | "
            f"Requested={percentage_of_original:.2f}% | "
            f"SuccessfulOrders={len(all_successful_orders)} | "
            f"FailedOrders={len(all_failed_orders)} | "
            f"VerifiedFills={len(all_verified_fills)} | "
            f"Success={is_successful}"
        )

        return {
            'success': is_successful,
            'successful_count': len(all_successful_orders),
            'failed_count': len(all_failed_orders),
            'verified_fill_count': len(all_verified_fills),
            'execution_time': batch_execution_time,
            'trade_uid': trade_uid
        }
'''

if old_return not in code:
    raise SystemExit(
        "ERROR: Partial-square-off return block not found. "
        "NO CHANGES MADE."
    )

code = code.replace(
    old_return,
    new_return,
    1
)

print(
    "PATCHED: final partial SQF execution summary."
)

# ============================================================
# 5. WRITE
# ============================================================

p.write_text(
    code,
    encoding="utf-8"
)

# ============================================================
# 6. COMPILE
# ============================================================

try:
    py_compile.compile(
        str(p),
        doraise=True
    )
except Exception as e:

    print("")
    print("COMPILE FAILED:")
    print(e)

    print("")
    print("RESTORING BACKUP...")

    shutil.copy2(
        backup,
        p
    )

    py_compile.compile(
        str(p),
        doraise=True
    )

    print("BACKUP RESTORED")
    raise SystemExit(2)

# ============================================================
# 7. VERIFY
# ============================================================

final = p.read_text(
    encoding="utf-8"
)

checks = {
    "direct execution message":
        "PARTIAL-SQF DIRECT EXECUTION" in final,

    "queue return removed":
        "Command dispatched." not in final,

    "partial execute_batch exists":
        'PSQF_{trade_uid}_CHUNK{chunk_idx}' in final,

    "price guard remains":
        "exit_chunk_price_allowed" in final,

    "price guard is before execution":
        final.find("exit_chunk_price_allowed")
        < final.find("executor.execute_batch("),

    "fill verification remains":
        "verify_orders_bulk" in final,

    "fill result logging":
        "PARTIAL-SQF FILL RESULT" in final,

    "final execution summary":
        "PARTIAL-SQF EXECUTION COMPLETE" in final,
}

print("")
print("=" * 100)
print("PARTIAL SQF PATCH VERIFICATION")
print("=" * 100)

failed = []

for name, ok in checks.items():

    print(
        f"{name:<42}: "
        f"{'OK' if ok else 'FAILED'}"
    )

    if not ok:
        failed.append(name)

if failed:

    print("")
    print("FAILED:")
    for item in failed:
        print("  -", item)

    print("")
    print("RESTORING BACKUP...")

    shutil.copy2(
        backup,
        p
    )

    py_compile.compile(
        str(p),
        doraise=True
    )

    print("BACKUP RESTORED")
    raise SystemExit(3)

print("")
print("SUCCESS")
print("")
print("Partial square-off now reaches execute_batch() directly.")
print("Exact exit price guard remains active before each chunk.")
print("Broker fill verification remains active.")
print("No change made to straddle_price_guard.py.")
print("")
print("PY_COMPILE: SUCCESS")
print("BACKUP:", backup)
print("=" * 100)
