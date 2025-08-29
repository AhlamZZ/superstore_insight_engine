import os, sys, json
import pandas as pd
import multiprocessing as mp

DATA_CLEAN = os.path.join("artifacts", "clean_superstore.csv")
OUT_JSON   = os.path.join("artifacts", "reports", "all_reports.json")

def log(m): print(m, flush=True)

def report_kpis(path):
    df = pd.read_csv(path)
    by_cat = df.groupby("category")[["sales","profit"]].sum().sort_values("profit", ascending=False)
    return {"kpis": by_cat.reset_index().to_dict(orient="records")}

def report_top_losses(path, n=15):
    df = pd.read_csv(path)
    worst = df.sort_values("profit").head(n)[["product_name","category","sub_category","sales","profit"]]
    return {"top_losses": worst.reset_index(drop=True).to_dict(orient="records")}

def report_monthly(path):
    df = pd.read_csv(path)
    monthly = df.groupby("order_month")[["sales","profit"]].sum().sort_index()
    return {"monthly": monthly.reset_index().to_dict(orient="records")}

def run_parallel():
    log("Starting parallel pool (3 workers)...")
    with mp.Pool(processes=3) as pool:
        r1 = pool.apply_async(report_kpis, args=(DATA_CLEAN,))
        r2 = pool.apply_async(report_top_losses, args=(DATA_CLEAN, 15))
        r3 = pool.apply_async(report_monthly, args=(DATA_CLEAN,))
        log("Waiting for worker results...")
        out = {}
        out.update(r1.get())
        out.update(r2.get())
        out.update(r3.get())
    return out

def run_sequential():
    log("Running sequential fallback...")
    out = {}
    out.update(report_kpis(DATA_CLEAN))
    out.update(report_top_losses(DATA_CLEAN, 15))
    out.update(report_monthly(DATA_CLEAN))
    return out

def main():
    log(f"Python: {sys.executable}")
    if not os.path.exists(DATA_CLEAN):
        raise FileNotFoundError("Run 01_load_clean.py first: " + DATA_CLEAN)
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)

    try:
        out = run_parallel()
    except Exception as e:
        log(f"Parallel run failed ({type(e).__name__}: {e})")
        out = run_sequential()

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    log(f"SAVED -> {os.path.abspath(OUT_JSON)}")
    log(f"Keys: {list(out.keys())}")

if __name__ == "__main__":
    # Windows-safe guards
    mp.freeze_support()
    try:
        mp.set_start_method("spawn")
    except RuntimeError:
        pass  # already set
    main()
