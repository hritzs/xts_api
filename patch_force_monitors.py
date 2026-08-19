import os

target_keywords = [
    "SL Check",
    "Hedge Check",
    "Roll Check",
    "Positions used for SL Check",
    "Positions used for Roll Check",
    "SL Params",
    "Roll Params",
    "StraddlePriceMonitor",
    "TP Monitor"
]

for root, dirs, files in os.walk('.'):
    if 'venv' in root or 'env' in root or '__pycache__' in root or '.git' in root:
        continue
    for f in files:
        if f.endswith('.py'):
            fpath = os.path.join(root, f)
            try:
                with open(fpath, 'r', encoding='utf-8') as file:
                    lines = file.readlines()
                
                changed = False
                for i, line in enumerate(lines):
                    # If any monitoring keyword is in the line, force it to logger.info
                    if any(kw in line for kw in target_keywords):
                        if 'logger.debug' in line or 'print' in line or 'logger.warning' in line:
                            # Strip out the old logger/print call and wrap it cleanly in logger.info
                            for kw in target_keywords:
                                if kw in line:
                                    # Extract the message string
                                    lines[i] = f'    logger.info({line.strip().split("(", 1)[1]}\n'
                                    changed = True
                                    break
                
                if changed:
                    with open(fpath, 'w', encoding='utf-8') as file:
                        file.writelines(lines)
                    print(f"Forced monitoring logs to INFO in {fpath}")
            except Exception:
                pass

print("✅ All SL, Hedge, and Roll check prints have been forcefully promoted to INFO level.")
