import os, sys, json
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score

DATA_CLEAN  = os.path.join("artifacts","clean_superstore.csv")
MODEL_REPORT = os.path.join("artifacts","reports","model_report.json")

def log(m): print(m, flush=True)

def main():
    log(f"Python: {sys.executable}")
    if not os.path.exists(DATA_CLEAN):
        raise FileNotFoundError("Run 01_load_clean.py first: " + DATA_CLEAN)
    os.makedirs(os.path.dirname(MODEL_REPORT), exist_ok=True)

    df = pd.read_csv(DATA_CLEAN)
    log(f"loaded rows={len(df)} cols={len(df.columns)}")

    target = "profit"
    numeric_features = [c for c in ["sales", "quantity", "discount"] if c in df.columns]
    categorical_features = [c for c in ["ship_mode","category","sub_category","region","segment","state","city"] if c in df.columns]

    X = df[numeric_features + categorical_features].copy()
    y = df[target].copy()

    log(f"numeric_features={numeric_features}")
    log(f"categorical_features={categorical_features}")

    numeric_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="mean")),
        ("scaler", StandardScaler())
    ])
    categorical_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore"))
    ])
    preprocessor = ColumnTransformer(
        transformers=[
            ("nums", numeric_transformer, numeric_features),
            ("cats", categorical_transformer, categorical_features)
        ],
        remainder="drop"
    )
    model = Pipeline(steps=[("prep", preprocessor), ("reg", LinearRegression())])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    log(f"train={len(X_train)} test={len(X_test)}")

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = float(mean_absolute_error(y_test, y_pred))
    r2  = float(r2_score(y_test, y_pred))

    report = {
        "model": "LinearRegression",
        "numeric_features": numeric_features,
        "categorical_features": categorical_features,
        "test_mae": round(mae, 4),
        "test_r2": round(r2, 4),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test))
    }
    with open(MODEL_REPORT, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    log(f"Saved model metrics -> {os.path.abspath(MODEL_REPORT)}")
    log(report)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback, os
        print("ERROR:", e, flush=True)
        os.makedirs(os.path.join("artifacts","reports"), exist_ok=True)
        with open(os.path.join("artifacts","reports","03_preprocess_ml_error.txt"), "w", encoding="utf-8") as f:
            f.write(type(e).__name__ + ": " + str(e) + "\n")
            f.write(traceback.format_exc())
        raise
