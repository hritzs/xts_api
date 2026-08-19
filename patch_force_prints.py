import os

monitors_dir = 'trading/monitors'
if os.path.exists(monitors_dir):
    for f in os.listdir(monitors_dir):
        if f.endswith('.py'):
            fpath = os.path.join(monitors_dir, f)
            with open(fpath, 'r', encoding='utf-8') as file:
                code = file.read()
            
            # Force print statements or logger calls into guaranteed visible INFO logs
            if 'sl_monitor' in f or 'hedge_monitor' in f or 'roll_monitor' in f:
                code = code.replace('logger.debug(', 'logger.info(')
                code = code.replace('logger.warning(', 'logger.info(')
                
                # Ensure a visible print wrapper exists inside check loops
                if 'def check(' in code or 'async def _run(' in code:
                    if 'MONITOR_CHECK_PRINT' not in code:
                        code = code.replace('async def ', '# MONITOR_CHECK_PRINT\n    logger.info(f"[{trade_uid}] 🔍 Running monitor check cycle...")\nasync def ')
            
            with open(fpath, 'w', encoding='utf-8') as file:
                file.write(code)
            print(f"Force-patched monitor: {fpath}")

print("✅ All background monitors are now hardcoded to log their cycles at INFO level!")
