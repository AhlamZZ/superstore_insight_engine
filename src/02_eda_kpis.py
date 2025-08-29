import os, sys, json
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # ensure non-GUI backend
import matplotlib.pyplot as plt

DATA_CLEAN = os.path.join("artifacts","clean_superstore.csv")
REPORT_JSON = os.path.join("artifacts","reports","eda_summary.json")
FIG_DIR = os.path.join("artifacts","figures")

def log(m): print(m, flush=True)

def save_plot(path):
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()
    log(f"saved figure: {os.path.abspath(path)}")

def main():
    log(f"Python: {sys.executable}")
    if not os.path.exists(DATA_CLEAN):
        raise FileNotFoundError("Run 01_load_clean.py first: " + DATA_CLEAN)

    os.makedirs(FIG_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_JSON), exist_ok=True)

    df = pd.read_csv(DATA_CLEAN, parse_dates=["order_date","ship_date"])
    log(f"loaded clean csv rows={len(df)} cols={len(df.columns)}")

    by_cat     = df.groupby("category")[["sales","profit"]].sum().sort_values("profit", ascending=False)
    by_subcat  = df.groupby("sub_category")[["sales","profit"]].sum().sort_values("profit", ascending=False)
    by_region  = df.groupby("region")[["sales","profit"]].sum().sort_values("profit", ascending=False)
    by_segment = df.groupby("segment")[["sales","profit"]].sum().sort_values("profit", ascending=False)

    monthly = df.groupby("order_month")[["sales","profit"]].sum().sort_index()

    # Figures
    monthly["sales"].plot(kind="line", title="Monthly Sales")
    save_plot(os.path.join(FIG_DIR, "monthly_sales.png"))

    by_subcat.head(10)["profit"].sort_values().plot(kind="barh", title="Top 10 Sub-Categories by Profit")
    save_plot(os.path.join(FIG_DIR, "top10_subcategories_profit.png"))

    df.plot.scatter(x="discount", y="profit", title="Discount vs Profit")
    save_plot(os.path.join(FIG_DIR, "discount_vs_profit.png"))

    summary = {
        "top_category_by_profit": by_cat.index[0] if not by_cat.empty else None,
        "top_subcategory_by_profit": by_subcat.index[0] if not by_subcat.empty else None,
        "best_region_by_profit": by_region.index[0] if not by_region.empty else None,
        "best_segment_by_profit": by_segment.index[0] if not by_segment.empty else None,
        "monthly_periods": monthly.index.tolist(),
        "monthly_sales": monthly["sales"].round(2).tolist(),
        "monthly_profit": monthly["profit"].round(2).tolist(),
    }
    with open(REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    log(f"EDA summary -> {os.path.abspath(REPORT_JSON)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback, os
        print("ERROR:", e, flush=True)
        os.makedirs(os.path.join("artifacts","reports"), exist_ok=True)
        with open(os.path.join("artifacts","reports","02_eda_kpis_error.txt"), "w", encoding="utf-8") as f:
            f.write(type(e).__name__ + ": " + str(e) + "\n")
            f.write(traceback.format_exc())
        raise
