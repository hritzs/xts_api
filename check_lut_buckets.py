
import pandas as pd
from pathlib import Path

files = [
    r"\\172.16.1.85\Shared\Hardik\Custom_5_Stage_Run104-436\LUT_0916_Custom.csv",
    r"\\172.16.1.85\Shared\Hardik\Custom_5_Stage_Run104-436\LUT_0917_Custom.csv",
    r"\\172.16.1.85\Shared\Hardik\Custom_5_Stage_Run104-436\LUT_0918_Custom.csv",
    r"\\172.16.1.85\Shared\Hardik\Custom_5_Stage_Run104-436\LUT_0919_Custom.csv",
    r"\\172.16.1.85\Shared\Hardik\Custom_5_Stage_Run104-436\LUT_0920_Onwards_Custom.csv",
]

for f in files:
    print("="*80)
    print(Path(f).name)
    df = pd.read_csv(f)
    for col in ["DTE","IV_Ratio","Straddle_Ratio","Build_IV","Norm_OG_Gap","Adj_IV_Chg","Trade"]:
        print(f"\n{col}")
        for v in sorted(df[col].dropna().astype(str).unique()):
            print("  ", v)
