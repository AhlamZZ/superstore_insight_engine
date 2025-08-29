import os, json, sys

EDA_JSON = os.path.join("artifacts","reports","eda_summary.json")
MODEL_JSON = os.path.join("artifacts","reports","model_report.json")
MERGED_JSON = os.path.join("artifacts","reports","dashboard_payload.json")

def log(m): print(m, flush=True)

def main():
    log(f"Python: {sys.executable}")
    if not os.path.exists(EDA_JSON):
        log(f"WARNING: {EDA_JSON} not found (run 02_eda_kpis.py).")
        eda = {}
    else:
        with open(EDA_JSON, "r", encoding="utf-8") as f:
            eda = json.load(f)
        log(f"Loaded EDA from {os.path.abspath(EDA_JSON)}")

    if not os.path.exists(MODEL_JSON):
        log(f"WARNING: {MODEL_JSON} not found (run 03_preprocess_ml.py).")
        model = {}
    else:
        with open(MODEL_JSON, "r", encoding="utf-8") as f:
            model = json.load(f)
        log(f"Loaded model report from {os.path.abspath(MODEL_JSON)}")

    payload = {"summary": eda, "model_report": model}
    os.makedirs(os.path.dirname(MERGED_JSON), exist_ok=True)
    with open(MERGED_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log(f"Saved merged dashboard payload ? {os.path.abspath(MERGED_JSON)}")

if __name__ == "__main__":
    main()
