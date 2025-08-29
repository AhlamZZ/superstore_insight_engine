import os, sys
import pandas as pd
import numpy as np

DATA_IN  = os.path.join("data", "Sample - Superstore.csv")
ART_DIR  = "artifacts"
FIG_DIR  = os.path.join(ART_DIR, "figures")
REP_DIR  = os.path.join(ART_DIR, "reports")
DATA_OUT = os.path.join(ART_DIR, "clean_superstore.csv")

def log(msg): print(msg, flush=True)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c: c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns}
    return df.rename(columns=cols)

def read_csv_with_fallback(path):
    # Try common encodings for Windows CSVs
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            return pd.read_csv(
                path,
                encoding=enc,
                parse_dates=["Order Date", "Ship Date"],
                dayfirst=False
            ), enc
        except UnicodeDecodeError:
            continue
    # Last resort: python engine with latin-1 (slower but robust)
    return pd.read_csv(
        path,
        encoding="latin-1",
        engine="python",
        parse_dates=["Order Date", "Ship Date"],
        dayfirst=False
    ), "latin-1 (python engine)"

def main():
    log(f"Running with Python: {sys.executable}")
    log(f"CWD: {os.getcwd()}")
    for d in (ART_DIR, FIG_DIR, REP_DIR):
        os.makedirs(d, exist_ok=True)

    if not os.path.exists(DATA_IN):
        raise FileNotFoundError(f"CSV not found at {DATA_IN}")

    log("== Step 1: Read CSV (auto encoding fallback) ==")
    df, used_enc = read_csv_with_fallback(DATA_IN)
    log(f"Loaded rows={len(df)}, cols={len(df.columns)}, encoding={used_enc}")

    log("== Step 2: Normalize columns ==")
    df = normalize_columns(df)

    # Numeric coercion
    for col in ("sales", "profit", "discount", "quantity"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Features
    if "order_date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["order_date"]):
        df["order_month"] = df["order_date"].dt.to_period("M").astype(str)
        df["order_year"]  = df["order_date"].dt.year
    if "sales" in df.columns and "profit" in df.columns:
        df["profit_ratio"] = np.where(df["sales"] != 0, df["profit"] / df["sales"], np.nan)

    # Missing values
    num_cols = df.select_dtypes(include=[np.number]).columns
    cat_cols = df.select_dtypes(exclude=[np.number, "datetime64[ns]"]).columns
    df[num_cols] = df[num_cols].fillna(0)
    df[cat_cols] = df[cat_cols].fillna("Unknown")

    # Drop duplicates
    df = df.drop_duplicates()

    # Save
    df.to_csv(DATA_OUT, index=False)
    log(f"SUCCESS: saved {os.path.abspath(DATA_OUT)}  rows={len(df)} cols={len(df.columns)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("ERROR:", e, flush=True)
        with open(os.path.join(REP_DIR, "01_load_clean_error.txt"), "w", encoding="utf-8") as f:
            f.write(f"{type(e).__name__}: {e}\n")
            f.write(traceback.format_exc())
