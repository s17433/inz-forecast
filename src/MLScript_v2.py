import json
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error

from config import FINAL, REPORTS, PLOTS

logging.basicConfig(
    level = logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

DATE_COL = "DateNo"
TARGET = "Sales"
GROUP_COLS = ["StoreNo", "IDX"]
LAGS = [1, 7, 14, 28]



#W tej wersji już nie biorę pod uwagę kilku cech
#OOS (skupiamy się na warunkach dostępności produktu), SalesValues  i stock_price_net
NUMERIC_CANDIDATES = [
    "dow",
    "month",
    "day_of_month",
    "week_of_year",
    "promo",
    "Price",
    "discount_percent",
    "lag_1",
    "lag_7",
    "lag_14",
    "lag_28",
    "history_mean_4lags"
]

CATEGORICAL_CANDIDATES = ["StoreNo", "Brand", "DIV2", "IDX"]
DROP_IF_PRESENT = ["ID", "ID_Promo", "SalesValue", "stock_price_net"]

def safe_rmse(y_true, y_pred) -> float:
    try:
        return float(mean_squared_error(y_true, y_pred, squared=False))
    except TypeError:
        return float(np.sqrt(mean_squared_error(y_true, y_pred)))
    
def wape(y_true, y_pred) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    denominator = np.abs(y_true).sum()
    if denominator == 0:
        return float("nan")
    return float(np.abs(y_true - y_pred).sum() / denominator * 100.0)

def calculate_metrics(y_true, y_pred) -> dict:
    return {
        "MAE": round(float(mean_absolute_error(y_true, y_pred)), 4),
        "RMSE": round(safe_rmse(y_true, y_pred), 4),
        "WAPE_percent": round(wape(y_true, y_pred), 2),
    }

def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df["dow"] = df[DATE_COL].dt.dayofweek
    df["month"] = df[DATE_COL].dt.month
    df["day_of_month"] = df[DATE_COL].dt.day
    df["week_of_year"] = df[DATE_COL].dt.isocalendar().week.astype("int16")
    return df

def add_exact_date_lags(df):
    result = df.copy()

    duplicated = result.duplicated(
        GROUP_COLS + [DATE_COL],
        keep=False
    )

    if duplicated.any():
        raise ValueError(
            f"W danych znaleziono {duplicated.sum()} wierszy "
            "należących do zduplikowanych kombinacji "
            "StoreNo + IDX + DateNo."
        )

    for lag in LAGS:
        lag_df = result[
            GROUP_COLS + [DATE_COL, "SalesForHistory"]
        ].copy()

        lag_df[DATE_COL] = (
            lag_df[DATE_COL] + pd.Timedelta(days=lag)
        )

        lag_df = lag_df.rename(
            columns={"SalesForHistory": f"lag_{lag}"}
        )

        result = result.merge(
            lag_df,
            on=GROUP_COLS + [DATE_COL],
            how="left"
        )

    return result


def split_by_unique_dates(df: pd.DataFrame, ratio: float = 0.8):
    
    #Podział po unikalnych datach, a nie po numerze wiersza.
    #Zapobiega sytuacji, w której ta sama data trafia częściowo do train i test.

    dates = np.array(sorted(df[DATE_COL].dropna().unique()))
    if len(dates) < 2:
        raise ValueError("Za mało unikalnych dat do podziału train/test.")

    cutoff_idx = max(1, min(len(dates) - 1, int(len(dates) * ratio)))
    cutoff_date = pd.Timestamp(dates[cutoff_idx - 1])

    train = df[df[DATE_COL] <= cutoff_date].copy()
    test = df[df[DATE_COL] > cutoff_date].copy()

    if train.empty or test.empty:
        raise ValueError("Po podziale czasowym train lub test jest pusty.")

    return train, test, cutoff_date


def encode_categories_from_train(train: pd.DataFrame, test: pd.DataFrame, columns: list[str]):
    #Mapowanie kategorii budowane wyłącznie na train. Nieznane w test -> -1
    train = train.copy()
    test = test.copy()

    for col in columns:
        train_values = train[col].fillna("__MISSING__").astype(str)
        test_values = test[col].fillna("__MISSING__").astype(str)

        categories = pd.Index(train_values.unique())
        mapping = {value: i for i, value in enumerate(categories)}

        train[col] = train_values.map(mapping).fillna(-1).astype("int32")
        test[col] = test_values.map(mapping).fillna(-1).astype("int32")

    return train, test


def prepare_features(train: pd.DataFrame, test: pd.DataFrame):
    numeric_features = [c for c in NUMERIC_CANDIDATES if c in train.columns]
    categorical_features = [c for c in CATEGORICAL_CANDIDATES if c in train.columns]

    for col in numeric_features:
        train[col] = pd.to_numeric(train[col], errors="coerce").astype("float32")
        test[col] = pd.to_numeric(test[col], errors="coerce").astype("float32")

    train, test = encode_categories_from_train(train, test, categorical_features)

    features = numeric_features + categorical_features
    if not features:
        raise ValueError("Brak cech do trenowania modelu.")

    return train, test, features


def make_baselines(test: pd.DataFrame) -> dict[str, pd.Series]:
    baselines = {}
    if "lag_1" in test.columns:
        baselines["naive_t_minus_1"] = test["lag_1"]
    if "lag_7" in test.columns:
        baselines["seasonal_naive_t_minus_7"] = test["lag_7"]
    return baselines


def save_feature_importance(model, features: list[str]):
    importance = pd.DataFrame({
        "feature": features,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)

    importance.to_csv(REPORTS / "feature_importance.csv", index=False)

    top = importance.head(20).sort_values("importance", ascending=True)
    plt.figure(figsize=(9, 6))
    plt.barh(top["feature"], top["importance"])
    plt.xlabel("Feature importance")
    plt.title("XGBoost - najważniejsze cechy")
    plt.tight_layout()
    plt.savefig(PLOTS / "feature_importance.png", dpi=160)
    plt.close()


def save_model_comparison(metrics: dict):
    rows = []
    for model_name, values in metrics.items():
        if model_name == "meta":
            continue
        rows.append({"model": model_name, **values})

    comparison = pd.DataFrame(rows)
    comparison.to_csv(REPORTS / "model_comparison.csv", index=False)

    if not comparison.empty and "WAPE_percent" in comparison.columns:
        plot_df = comparison.dropna(subset=["WAPE_percent"]).copy()
        plt.figure(figsize=(8, 4))
        plt.bar(plot_df["model"], plot_df["WAPE_percent"])
        plt.ylabel("WAPE [%]")
        plt.title("Porównanie modeli")
        plt.xticks(rotation=20, ha="right")
        plt.tight_layout()
        plt.savefig(PLOTS / "model_comparison.png", dpi=160)
        plt.close()


def save_example_prediction_plot(test_eval: pd.DataFrame):
    # Wybierz serię StoreNo-IDX z największą liczbą obserwacji w teście.
    counts = (
        test_eval.groupby(["StoreNo_original", "IDX_original"])
        .size()
        .sort_values(ascending=False)
    )
    if counts.empty:
        return

    store, idx = counts.index[0]
    sample = test_eval[
        (test_eval["StoreNo_original"] == store)
        & (test_eval["IDX_original"] == idx)
    ].sort_values(DATE_COL)

    if sample.empty:
        return

    plt.figure(figsize=(11, 4))
    plt.plot(sample[DATE_COL], sample[TARGET], label="sprzedaż rzeczywista")
    plt.plot(sample[DATE_COL], sample["prediction_xgboost"], label="prognoza XGBoost")
    if "lag_7" in sample.columns:
        plt.plot(sample[DATE_COL], sample["lag_7"], label="seasonal naive t-7", alpha=0.7)
    plt.title(f"Przykład prognozy: sklep {store}, IDX {idx}")
    plt.xlabel("Data")
    plt.ylabel("Sprzedaż")
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOTS / "prediction_example.png", dpi=160)
    plt.close()


def run():
    data_path = FINAL / "MergedDataAfter.parquet"
    if not data_path.exists():
        raise FileNotFoundError(f"Nie znaleziono danych: {data_path}")

    REPORTS.mkdir(parents=True, exist_ok=True)
    PLOTS.mkdir(parents=True, exist_ok=True)

    logging.info("Wczytywanie danych: %s", data_path)
    df = pd.read_parquet(data_path)

    print("\n" + "=" * 60)
    print("DIAGNOSTYKA DANYCH WEJŚCIOWYCH")
    print("=" * 60)

    # ---------------------------------------------------------
    # 1. Sprzedaż zerowa
    # ---------------------------------------------------------
    sales_numeric = pd.to_numeric(df[TARGET], errors="coerce")

    zero_sales = (sales_numeric == 0).sum()
    positive_sales = (sales_numeric > 0).sum()
    missing_sales = sales_numeric.isna().sum()

    print("\n=== SALES ===")
    print(f"Wszystkie rekordy: {len(df)}")
    print(f"Sales = 0: {zero_sales} ({zero_sales / len(df) * 100:.2f}%)")
    print(
        f"Sales > 0: {positive_sales} "
        f"({positive_sales / len(df) * 100:.2f}%)"
    )
    print(f"Sales NaN: {missing_sales}")

    # ---------------------------------------------------------
    # 2. OOS
    # ---------------------------------------------------------
    if "OOS" in df.columns:
        print("\n=== OOS ===")

        print(
            df["OOS"]
            .value_counts(dropna=False)
            .sort_index()
        )

    # ---------------------------------------------------------
    # 3. Promocje
    # ---------------------------------------------------------
    if "promo" in df.columns:
        print("\n=== PROMO ===")

        print(
            df["promo"]
            .value_counts(dropna=False)
            .sort_index()
        )

        print(
            f"promo NaN: {df['promo'].isna().sum()} "
            f"({df['promo'].isna().mean() * 100:.2f}%)"
        )

    # ---------------------------------------------------------
    # 4. Rabat
    # ---------------------------------------------------------
    if "discount_percent" in df.columns:
        print("\n=== DISCOUNT_PERCENT ===")

        print(
            f"NaN: {df['discount_percent'].isna().sum()} "
            f"({df['discount_percent'].isna().mean() * 100:.2f}%)"
        )

        print(
            f"= 0: {(df['discount_percent'] == 0).sum()}"
        )

        print(
            f"> 0: {(df['discount_percent'] > 0).sum()}"
        )

    # ---------------------------------------------------------
    # 5. Kolumny związane z ceną
    # ---------------------------------------------------------
    print("\n=== PRICE COLUMNS ===")

    price_cols = [
        col for col in df.columns
        if "price" in col.lower()
    ]

    print(price_cols)

    for col in price_cols:
        print(f"\n{col}:")
        print(f"  dtype: {df[col].dtype}")
        print(f"  NaN: {df[col].isna().sum()}")

        numeric_price = pd.to_numeric(
            df[col],
            errors="coerce"
        )

        print(
            f"  wartości numeryczne: "
            f"{numeric_price.notna().sum()}"
        )

    # ---------------------------------------------------------
    # 6. Promocje w dniach bez sprzedaży
    # ---------------------------------------------------------
    if "promo" in df.columns:
        print("\n=== PROMO A SALES = 0 ===")

        zero_df = df.loc[sales_numeric == 0]

        print(
            zero_df["promo"]
            .value_counts(dropna=False)
            .sort_index()
        )

    # ---------------------------------------------------------
    # 7. Discount w dniach bez sprzedaży
    # ---------------------------------------------------------
    if "discount_percent" in df.columns:
        print("\n=== DISCOUNT A SALES = 0 ===")

        zero_df = df.loc[sales_numeric == 0]

        print(
            f"NaN: "
            f"{zero_df['discount_percent'].isna().sum()}"
        )

        print(
            f"> 0: "
            f"{(zero_df['discount_percent'] > 0).sum()}"
        )

    required = [DATE_COL, TARGET] + GROUP_COLS
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Brak wymaganych kolumn: {missing}")

    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df[TARGET] = pd.to_numeric(df[TARGET], errors="coerce")
    df = df.dropna(subset=[DATE_COL, TARGET] + GROUP_COLS).copy()

    # Zachowujemy oryginalne identyfikatory do wykresów przed kodowaniem kategorii.
    df["StoreNo"] = df["StoreNo"].astype(str)
    df["IDX"] = df["IDX"].astype(str)

    logging.info("Liczba wierszy przed feature engineering: %s", len(df))

    df = add_calendar_features(df)

    df[TARGET] = pd.to_numeric(df[TARGET], errors="coerce")

    df["SalesForHistory"] = df[TARGET]

    if "OOS" in df.columns:
        oos_numeric = pd.to_numeric(
            df["OOS"],
            errors="coerce"
        ).fillna(0)

        df.loc[
            oos_numeric == 1,
            "SalesForHistory"
        ] = np.nan

    df = add_exact_date_lags(df)

    df["history_mean_4lags"] = df[
        ["lag_1", "lag_7", "lag_14", "lag_28"]
    ].mean(axis=1, skipna=True)

    print("\n=== ANALIZA BRAKU LAG_7 ===")

    first_dates = (
        df.groupby(GROUP_COLS)[DATE_COL]
        .transform("min")
    )

    df["days_from_series_start"] = (
        df[DATE_COL] - first_dates
    ).dt.days

    missing_lag7_mask = df["lag_7"].isna()

    missing_in_first_7_days = (
        missing_lag7_mask
        & (df["days_from_series_start"] < 7)
    ).sum()

    missing_after_first_7_days = (
        missing_lag7_mask
        & (df["days_from_series_start"] >= 7)
    ).sum()

    print(
        f"Brak lag_7 w pierwszych 7 dniach historii: "
        f"{missing_in_first_7_days}"
    )

    print(
        f"Brak lag_7 po pierwszych 7 dniach historii: "
        f"{missing_after_first_7_days}"
    )

    print(
        f"Serie StoreNo+IDX: "
        f"{df[GROUP_COLS].drop_duplicates().shape[0]}"
    )

  
    before = len(df)
    df = df.dropna(subset=["lag_7"]).copy()
    logging.info("Usunięto %s wierszy bez lag_7", before - len(df))

    # Usunięcie oczywistych pól powodujących leakage / niedostępnych prognostycznie.
    drop_actual = [c for c in DROP_IF_PRESENT if c in df.columns]
    if drop_actual:
        logging.info("Usuwam z wejścia pola: %s", drop_actual)
        df = df.drop(columns=drop_actual)

    train, test, cutoff_date = split_by_unique_dates(df, ratio=0.8)
    logging.info("Cutoff train/test: %s", cutoff_date.date())

    # Dni OOS wyłączamy również z treningu: obserwowana sprzedaż podczas braku
    # towaru jest sprzedażą ograniczoną dostępnością, a nie pełnym popytem.
    if "OOS" in train.columns:
        train_oos = pd.to_numeric(train["OOS"], errors="coerce").fillna(0)
        before_train = len(train)
        train = train.loc[train_oos != 1].copy()
        logging.info("Wyłączono z treningu %s wierszy OOS", before_train - len(train))

    logging.info("Train: %s wierszy | Test: %s wierszy", len(train), len(test))

    # Ocena na dniach, gdy produkt był dostępny. Przy OOS rzeczywista sprzedaż
    # nie reprezentuje pełnego popytu. Jeśli OOS nie istnieje, oceniamy wszystkie dni.
    if "OOS" in test.columns:
        oos_numeric = pd.to_numeric(test["OOS"], errors="coerce").fillna(0)
        test_eval_mask = oos_numeric != 1
    else:
        test_eval_mask = pd.Series(True, index=test.index)

    # Oryginalne identyfikatory potrzebne do czytelnego wykresu.
    test["StoreNo_original"] = test["StoreNo"].astype(str)
    test["IDX_original"] = test["IDX"].astype(str)

    train, test, features = prepare_features(train, test)

    X_train = train[features]
    y_train = train[TARGET].astype("float32")
    X_test = test[features]

    model = xgb.XGBRegressor(
        objective="reg:squarederror",
        n_estimators=500,
        max_depth=8,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        tree_method="hist",
    )

    logging.info("Trenowanie XGBoost na %s cechach...", len(features))
    model.fit(X_train, y_train)
    test["prediction_xgboost"] = np.clip(model.predict(X_test), a_min=0, a_max=None)

    # Po prepare_features indeks został zachowany, więc maskę można wyrównać po indeksie.
    eval_mask = test_eval_mask.reindex(test.index).fillna(False)
    eval_df = test.loc[eval_mask].copy()

    if eval_df.empty:
        raise ValueError("Po wyłączeniu OOS zbiór testowy do ewaluacji jest pusty.")

    metrics = {
        "xgboost": calculate_metrics(eval_df[TARGET], eval_df["prediction_xgboost"]),
    }

    # Baseline'y liczymy tylko na rekordach, gdzie dany lag istnieje.
    for baseline_name, baseline_pred in make_baselines(eval_df).items():
        valid = baseline_pred.notna()
        if valid.any():
            metrics[baseline_name] = calculate_metrics(
                eval_df.loc[valid, TARGET],
                baseline_pred.loc[valid],
            )

    metrics["meta"] = {
        "cutoff_date": str(cutoff_date.date()),
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "evaluation_rows": int(len(eval_df)),
        "excluded_oos_rows": int(len(test) - len(eval_df)),
        "n_features": len(features),
        "features": features,
        "notes": [
            "SalesValue i stock_price_net nie są używane jako cechy modelu.",
            "OOS nie jest używane jako cecha; dni OOS=1 są wyłączone z treningu i ewaluacji.",
            "Lagi są liczone po dokładnych datach kalendarzowych.",
            "Podział train/test jest czasowy i wykonywany po unikalnych datach.",
        ],
    }

    (REPORTS / "metrics_v2.json").write_text(
        json.dumps(metrics, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    save_feature_importance(model, features)
    save_model_comparison(metrics)

    # Przywróć czytelne ID na potrzeby wykresu.
    eval_df["StoreNo_original"] = test.loc[eval_df.index, "StoreNo_original"]
    eval_df["IDX_original"] = test.loc[eval_df.index, "IDX_original"]
    save_example_prediction_plot(eval_df)

    # Predykcje przydadzą się później do analizy błędów w pracy.
    prediction_cols = [
        DATE_COL,
        "StoreNo_original",
        "IDX_original",
        TARGET,
        "prediction_xgboost",
    ] + [c for c in ["lag_1", "lag_7", "lag_14", "lag_28", "promo", "discount_percent", "OOS"] if c in eval_df.columns]
    eval_df[prediction_cols].to_csv(REPORTS / "predictions_test.csv", index=False)

    logging.info("Gotowe. Wyniki zapisano w %s", REPORTS)
    print(json.dumps(metrics, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    run()