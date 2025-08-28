# src/MLScript.py
import json
from pathlib import Path
import logging

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error

from config import FINAL, REPORTS, PLOTS

# ------------------------------ USTAWIENIA ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

DATE_COL = "DateNo"
TARGET = "Sales"

# Kolumny kategoryczne do zakodowania
CATEGORICAL_CANDIDATES = ["StoreNo", "Brand", "DIV2", "IDX"]

# Potencjalne cechy numeryczne — zostaną wybrane tylko te, które istnieją
NUMERIC_CANDIDATES = [
    "dow", "month", "promo", "OOS",
    "SalesValue", "Price", "stock_price_net", "discount_percent"
]

# Kolumny, które należy usunąć jeśli występują (czyste ID itp.)
DROP_IF_PRESENT = ["ID", "ID_Promo"]

# ------------------------------ FUNKCJE POMOCNICZE ------------------------------
def time_split(df: pd.DataFrame, date_col: str = DATE_COL, ratio: float = 0.8):
    """Podział czasowy: część wcześniejsza -> train, późniejsza -> test."""
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.sort_values(date_col)
    if len(df) == 0:
        raise ValueError("DataFrame pusty po sortowaniu po dacie.")
    cutoff_idx = int(len(df) * ratio)
    cutoff = df[date_col].iloc[cutoff_idx]
    train = df[df[date_col] <= cutoff].copy()
    test = df[df[date_col] > cutoff].copy()
    if len(train) == 0 or len(test) == 0:
        # awaryjnie podział prosty, gdy data jest jednolita
        train = df.iloc[:cutoff_idx].copy()
        test = df.iloc[cutoff_idx:].copy()
    return train, test


def sanitize_features(df: pd.DataFrame):
    """
    Przygotowanie cech:
    - datę na datetime
    - numeryczne -> float32
    - kategorie -> codes int32
    Zwraca: (df, lista_cech)
    """
    if DATE_COL not in df.columns:
        raise ValueError(f"Brak kolumny daty: {DATE_COL}")

    if not pd.api.types.is_datetime64_any_dtype(df[DATE_COL]):
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")

    # wybierz tylko istniejące kolumny
    num_feats = [c for c in NUMERIC_CANDIDATES if c in df.columns]
    cat_feats = [c for c in CATEGORICAL_CANDIDATES if c in df.columns]

    # rzutowanie numerycznych
    for c in num_feats:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")

    # rzutowanie kategorycznych na kody (braki -> -1)
    for c in cat_feats:
        df[c] = df[c].astype("category").cat.codes.astype("int32")

    features = num_feats + cat_feats
    if not features:
        raise ValueError(
            "Lista cech jest pusta. Sprawdź czy nazwy kolumn w NUMERIC_CANDIDATES/CATEGORICAL_CANDIDATES "
            "występują w danych."
        )
    return df, features


def safe_rmse(y_true, y_pred):
    """RMSE kompatybilny ze starszym sklearn (bez parametru squared)."""
    try:
        return mean_squared_error(y_true, y_pred, squared=False)
    except TypeError:
        return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def safe_mape(y_true, y_pred, eps=1e-9):
    """
    Bezpieczny MAPE: jeżeli w y_true są zera, nie wybucha.
    Jeśli wiesz, że zer nie ma, możesz użyć sklearnowego MAPE.
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    denom = np.where(np.abs(y_true) < eps, eps, y_true)
    return float(np.mean(np.abs((y_true - y_pred) / denom)))


# ------------------------------ GŁÓWNA FUNKCJA ------------------------------
def run():
    data_path = FINAL / "MergedDataAfter.parquet"
    if not data_path.exists():
        raise FileNotFoundError(f"Nie znaleziono pliku/katalogu Parquet: {data_path}")

    logging.info(f"Wczytywanie danych z: {data_path}")
    # Pandas potrafi wczytać zarówno pojedynczy plik Parquet, jak i katalog (dataset)
    df = pd.read_parquet(data_path)

    # Porządki: usuń zbędne kolumny, jeśli są
    drop_actual = [c for c in DROP_IF_PRESENT if c in df.columns]
    if drop_actual:
        logging.info(f"Usuwam kolumny: {drop_actual}")
        df = df.drop(columns=drop_actual)

    # Kontrola targetu
    if TARGET not in df.columns:
        raise ValueError(f"Brak kolumny targetu: {TARGET}")

    # Przygotowanie cech
    df, features = sanitize_features(df)

    # y jako float32
    df[TARGET] = pd.to_numeric(df[TARGET], errors="coerce").astype("float32")

    # Usuwamy wiersze bez targetu
    before = len(df)
    df = df.dropna(subset=[TARGET])
    after = len(df)
    if after < before:
        logging.info(f"Usunięto {before - after} wierszy z NaN w {TARGET}")

    # Podział czasowy
    train, test = time_split(df, DATE_COL, 0.8)
    X_train, y_train = train[features], train[TARGET].values
    X_test,  y_test  = test[features],  test[TARGET].values

    logging.info(f"Train: {X_train.shape}, Test: {X_test.shape}, liczba cech: {len(features)}")

    # Model XGBoost
    model = xgb.XGBRegressor(
        objective="reg:squarederror",
        n_estimators=400,
        max_depth=8,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        tree_method="hist",
    )

    logging.info("Trenowanie modelu XGBoost...")
    model.fit(X_train, y_train)

    logging.info("Predykcja na zbiorze testowym...")
    y_pred = model.predict(X_test)

    # Metryki
    rmse = safe_rmse(y_test, y_pred)
    try:
        # jeśli masz nowszy sklearn i brak zer – możesz woleć tę wersję:
        mape = float(mean_absolute_percentage_error(y_test, y_pred))
    except Exception:
        mape = safe_mape(y_test, y_pred)

    # Zapisy artefaktów
    REPORTS.mkdir(parents=True, exist_ok=True)
    PLOTS.mkdir(parents=True, exist_ok=True)

    metrics = {
        "xgboost": {
            "RMSE": round(float(rmse), 3),
            "MAPE": round(float(mape) * 100.0, 2)  # w %
        },
        "meta": {
            "n_features": len(features),
            "features": features
        }
    }
    (REPORTS / "metrics.json").write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")

    # Wykres True vs Pred (pierwsze 500 pkt po dacie)
    import matplotlib.pyplot as plt
    s = test.sort_values(DATE_COL).head(500).copy()
    plt.figure(figsize=(10, 4))
    plt.plot(s[DATE_COL], s[TARGET], label="y_true")
    plt.plot(s[DATE_COL], model.predict(s[features]), label="y_pred")
    plt.title("True vs Pred (pierwsze 500 po dacie)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOTS / "true_vs_pred.png", dpi=150)

    logging.info(f"Zapisano: {REPORTS / 'metrics.json'} oraz {PLOTS / 'true_vs_pred.png'}")
    print("Zapisano:", REPORTS / "metrics.json", "oraz", PLOTS / "true_vs_pred.png")


# ------------------------------ URUCHOMIENIE ------------------------------
if __name__ == "__main__":
    run()
