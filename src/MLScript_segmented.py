
import json
import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xgboost as xgb

from sklearn.compose import ColumnTransformer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import OneHotEncoder

from config import FINAL, REPORTS, PLOTS


# ============================================================
# LOGOWANIE
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ============================================================
# KONFIGURACJA
# ============================================================

DATE_COL = "DateNo"
TARGET = "Sales"

GROUP_COLS = [
    "StoreNo",
    "IDX",
]

LAGS = [
    1,
    7,
    14,
    28,
]


# ============================================================
# CECHY
# ============================================================

# Najlepszy dotychczas wariant:
# - promo zostaje
# - discount_percent nie jest używane
NUMERIC_CANDIDATES = [
    "dow",
    "month",
    "day_of_month",
    "week_of_year",
    "promo",

    "lag_1",
    "lag_7",
    "lag_14",
    "lag_28",

    "history_mean_4lags",
]


CATEGORICAL_CANDIDATES = [
    "StoreNo",
    "Brand",
    "DIV2",
    "IDX",
]


DROP_IF_PRESENT = [
    "ID",
    "ID_Promo",
    "SalesValue",
    "stock_price_net",
]


ROTATION_GROUPS = [
    "low",
    "medium",
    "high",
]


# ============================================================
# MODEL
# ============================================================

def build_xgboost_model():
    """
    Tworzy zawsze model z tymi samymi parametrami.

    Dzięki temu porównanie modelu globalnego
    i modeli segmentowych jest uczciwe.
    """

    return xgb.XGBRegressor(
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


# ============================================================
# METRYKI
# ============================================================

def safe_rmse(y_true, y_pred):

    try:
        return float(
            mean_squared_error(
                y_true,
                y_pred,
                squared=False,
            )
        )

    except TypeError:
        return float(
            np.sqrt(
                mean_squared_error(
                    y_true,
                    y_pred,
                )
            )
        )


def wape(y_true, y_pred):

    y_true = np.asarray(
        y_true,
        dtype=float,
    )

    y_pred = np.asarray(
        y_pred,
        dtype=float,
    )

    denominator = np.abs(
        y_true
    ).sum()

    if denominator == 0:
        return float("nan")

    return float(
        np.abs(
            y_true - y_pred
        ).sum()
        / denominator
        * 100
    )


def calculate_metrics(
    y_true,
    y_pred,
):

    return {
        "MAE": round(
            float(
                mean_absolute_error(
                    y_true,
                    y_pred,
                )
            ),
            4,
        ),

        "RMSE": round(
            safe_rmse(
                y_true,
                y_pred,
            ),
            4,
        ),

        "WAPE_percent": round(
            wape(
                y_true,
                y_pred,
            ),
            2,
        ),
    }


# ============================================================
# CECHY KALENDARZOWE
# ============================================================

def add_calendar_features(df):

    df = df.copy()

    df[DATE_COL] = pd.to_datetime(
        df[DATE_COL],
        errors="coerce",
    )

    df["dow"] = (
        df[DATE_COL]
        .dt.dayofweek
    )

    df["month"] = (
        df[DATE_COL]
        .dt.month
    )

    df["day_of_month"] = (
        df[DATE_COL]
        .dt.day
    )

    df["week_of_year"] = (
        df[DATE_COL]
        .dt.isocalendar()
        .week
        .astype("int16")
    )

    return df


# ============================================================
# OOS I HISTORIA
# ============================================================

def add_sales_for_history(df):

    df = df.copy()

    df["SalesForHistory"] = pd.to_numeric(
        df[TARGET],
        errors="coerce",
    )

    if "OOS" in df.columns:

        oos = pd.to_numeric(
            df["OOS"],
            errors="coerce",
        ).fillna(0)

        # OOS oznacza brak wiarygodnej obserwacji popytu.
        df.loc[
            oos == 1,
            "SalesForHistory",
        ] = np.nan

    return df


# ============================================================
# LAGI
# ============================================================

def add_exact_date_lags(
    df,
    lags=LAGS,
):

    result = df.copy()

    duplicated = result.duplicated(
        GROUP_COLS + [DATE_COL],
        keep=False,
    )

    if duplicated.any():

        raise ValueError(
            f"Znaleziono "
            f"{int(duplicated.sum())} "
            f"wierszy należących do "
            f"zduplikowanych kombinacji "
            f"StoreNo + IDX + DateNo."
        )

    history = result[
        GROUP_COLS
        + [
            DATE_COL,
            "SalesForHistory",
        ]
    ].copy()

    for lag in lags:

        lag_df = history.copy()

        lag_df[DATE_COL] = (
            lag_df[DATE_COL]
            + pd.Timedelta(
                days=lag
            )
        )

        lag_df = lag_df.rename(
            columns={
                "SalesForHistory":
                    f"lag_{lag}"
            }
        )

        result = result.merge(
            lag_df,
            on=GROUP_COLS + [DATE_COL],
            how="left",
            validate="one_to_one",
        )

    lag_cols = [
        f"lag_{lag}"
        for lag in lags
    ]

    result[
        "history_mean_4lags"
    ] = result[
        lag_cols
    ].mean(
        axis=1,
        skipna=True,
    )

    return result


# ============================================================
# TRAIN / TEST
# ============================================================

def split_by_unique_dates(
    df,
    ratio=0.8,
):

    dates = np.array(
        sorted(
            df[
                DATE_COL
            ]
            .dropna()
            .unique()
        )
    )

    if len(dates) < 2:

        raise ValueError(
            "Za mało dat do podziału train/test."
        )

    cutoff_idx = int(
        len(dates)
        * ratio
    )

    cutoff_idx = max(
        1,
        min(
            cutoff_idx,
            len(dates) - 1,
        )
    )

    cutoff = pd.Timestamp(
        dates[
            cutoff_idx - 1
        ]
    )

    train = df[
        df[DATE_COL]
        <= cutoff
    ].copy()

    test = df[
        df[DATE_COL]
        > cutoff
    ].copy()

    return (
        train,
        test,
        cutoff,
    )


# ============================================================
# SEGMENTACJA ROTACJI
# ============================================================

def build_rotation_groups(
    train
):
    """
    Segmentację tworzymy WYŁĄCZNIE z danych treningowych.

    Podstawą jest średnia dzienna sprzedaż
    dla pary StoreNo + IDX.
    """

    series_stats = (
        train
        .groupby(
            GROUP_COLS
        )
        .agg(
            days=(
                TARGET,
                "count",
            ),

            mean_daily_sales=(
                TARGET,
                "mean",
            ),

            median_daily_sales=(
                TARGET,
                "median",
            ),

            positive_sales_days=(
                TARGET,
                lambda x: (
                    x > 0
                ).sum()
            ),

            zero_sales_days=(
                TARGET,
                lambda x: (
                    x == 0
                ).sum()
            ),
        )
        .reset_index()
    )

    series_stats[
        "positive_day_share"
    ] = (
        series_stats[
            "positive_sales_days"
        ]
        / series_stats[
            "days"
        ]
    )

    series_stats[
        "zero_day_share"
    ] = (
        series_stats[
            "zero_sales_days"
        ]
        / series_stats[
            "days"
        ]
    )

    q_low = (
        series_stats[
            "mean_daily_sales"
        ]
        .quantile(
            1 / 3
        )
    )

    q_high = (
        series_stats[
            "mean_daily_sales"
        ]
        .quantile(
            2 / 3
        )
    )

    series_stats[
        "rotation_group"
    ] = np.select(
        [
            series_stats[
                "mean_daily_sales"
            ] <= q_low,

            series_stats[
                "mean_daily_sales"
            ] <= q_high,
        ],
        [
            "low",
            "medium",
        ],
        default="high",
    )

    print(
        "\n=== SEGMENTACJA ROTACJI ==="
    )

    print(
        f"Próg low / medium: "
        f"{q_low:.4f} szt./dzień"
    )

    print(
        f"Próg medium / high: "
        f"{q_high:.4f} szt./dzień"
    )

    print(
        "\nLiczba serii:"
    )

    print(
        series_stats[
            "rotation_group"
        ]
        .value_counts()
        .reindex(
            ROTATION_GROUPS
        )
    )

    return (
        series_stats,
        q_low,
        q_high,
    )


def attach_rotation_groups(
    df,
    series_stats,
):

    lookup = (
        series_stats[
            GROUP_COLS
            + [
                "rotation_group"
            ]
        ]
        .drop_duplicates(
            subset=GROUP_COLS
        )
    )

    return df.merge(
        lookup,
        on=GROUP_COLS,
        how="left",
        validate="many_to_one",
    )


# ============================================================
# ONE HOT
# ============================================================

def prepare_one_hot(
    train,
    test,
):

    train = train.copy()
    test = test.copy()

    numeric_features = [
        c
        for c in NUMERIC_CANDIDATES
        if c in train.columns
    ]

    categorical_features = [
        c
        for c in CATEGORICAL_CANDIDATES
        if c in train.columns
    ]

    for col in numeric_features:

        train[col] = pd.to_numeric(
            train[col],
            errors="coerce",
        ).astype(
            "float32"
        )

        test[col] = pd.to_numeric(
            test[col],
            errors="coerce",
        ).astype(
            "float32"
        )

    for col in categorical_features:

        train[col] = (
            train[col]
            .fillna(
                "__MISSING__"
            )
            .astype(str)
        )

        test[col] = (
            test[col]
            .fillna(
                "__MISSING__"
            )
            .astype(str)
        )

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "numeric",
                "passthrough",
                numeric_features,
            ),

            (
                "categorical",
                OneHotEncoder(
                    handle_unknown="ignore",
                    sparse_output=True,
                    dtype=np.float32,
                ),
                categorical_features,
            ),
        ],

        remainder="drop",
        sparse_threshold=0.3,
    )

    columns = (
        numeric_features
        + categorical_features
    )

    X_train = (
        preprocessor
        .fit_transform(
            train[
                columns
            ]
        )
    )

    X_test = (
        preprocessor
        .transform(
            test[
                columns
            ]
        )
    )

    feature_names = (
        preprocessor
        .get_feature_names_out()
        .tolist()
    )

    return (
        X_train,
        X_test,
        feature_names,
        preprocessor,
    )


# ============================================================
# BASELINE
# ============================================================

def add_baseline_metrics(
    metrics,
    eval_df,
):

    for (
        model_name,
        col
    ) in [
        (
            "naive_t_minus_1",
            "lag_1",
        ),

        (
            "seasonal_naive_t_minus_7",
            "lag_7",
        ),
    ]:

        if col not in eval_df.columns:
            continue

        valid = (
            eval_df[
                col
            ]
            .notna()
        )

        if not valid.any():
            continue

        metrics[
            model_name
        ] = calculate_metrics(
            eval_df.loc[
                valid,
                TARGET,
            ],

            eval_df.loc[
                valid,
                col,
            ],
        )


# ============================================================
# WYNIKI SEGMENTÓW
# ============================================================

def calculate_rotation_metrics(
    eval_df
):

    rows = []

    model_columns = {
        "global_xgboost":
            "prediction_global",

        "segmented_xgboost":
            "prediction_segmented",

        "naive_t_minus_1":
            "lag_1",

        "seasonal_naive_t_minus_7":
            "lag_7",
    }

    for group in ROTATION_GROUPS:

        group_df = eval_df[
            eval_df[
                "rotation_group"
            ]
            == group
        ].copy()

        if group_df.empty:
            continue

        for (
            model_name,
            prediction_col
        ) in model_columns.items():

            if (
                prediction_col
                not in group_df.columns
            ):
                continue

            valid = (
                group_df[
                    prediction_col
                ]
                .notna()
            )

            if not valid.any():
                continue

            metrics = calculate_metrics(
                group_df.loc[
                    valid,
                    TARGET,
                ],

                group_df.loc[
                    valid,
                    prediction_col,
                ],
            )

            rows.append(
                {
                    "rotation_group":
                        group,

                    "model":
                        model_name,

                    "rows":
                        int(
                            valid.sum()
                        ),

                    "actual_mean_sales":
                        round(
                            float(
                                group_df.loc[
                                    valid,
                                    TARGET,
                                ].mean()
                            ),
                            4,
                        ),

                    "zero_sales_percent":
                        round(
                            float(
                                (
                                    group_df.loc[
                                        valid,
                                        TARGET,
                                    ]
                                    == 0
                                ).mean()
                                * 100
                            ),
                            2,
                        ),

                    **metrics,
                }
            )

    return pd.DataFrame(
        rows
    )


# ============================================================
# WYKRES PORÓWNANIA
# ============================================================

def save_segment_comparison_plot(
    rotation_metrics
):

    if rotation_metrics.empty:
        return

    plot_df = rotation_metrics[
        rotation_metrics[
            "model"
        ].isin(
            [
                "global_xgboost",
                "segmented_xgboost",
            ]
        )
    ].copy()

    if plot_df.empty:
        return

    pivot = plot_df.pivot(
        index="rotation_group",
        columns="model",
        values="WAPE_percent",
    )

    pivot = pivot.reindex(
        ROTATION_GROUPS
    )

    ax = pivot.plot(
        kind="bar",
        figsize=(8, 5),
    )

    ax.set_ylabel(
        "WAPE [%]"
    )

    ax.set_xlabel(
        "Grupa rotacji"
    )

    ax.set_title(
        "Globalny vs segmentowany XGBoost"
    )

    plt.xticks(
        rotation=0
    )

    plt.tight_layout()

    plt.savefig(
        PLOTS
        / "global_vs_segmented_rotation.png",
        dpi=160,
    )

    plt.close()


# ============================================================
# MAIN
# ============================================================

def run():

    data_path = (
        FINAL
        / "MergedDataAfter.parquet"
    )

    REPORTS.mkdir(
        parents=True,
        exist_ok=True,
    )

    PLOTS.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # WCZYTANIE
    # --------------------------------------------------------

    logging.info(
        "Wczytywanie danych: %s",
        data_path,
    )

    df = pd.read_parquet(
        data_path
    )

    df[DATE_COL] = pd.to_datetime(
        df[DATE_COL],
        errors="coerce",
    )

    df[TARGET] = pd.to_numeric(
        df[TARGET],
        errors="coerce",
    )

    df = df.dropna(
        subset=
        GROUP_COLS
        + [
            DATE_COL,
            TARGET,
        ]
    ).copy()

    df["StoreNo"] = (
        df["StoreNo"]
        .astype(str)
    )

    df["IDX"] = (
        df["IDX"]
        .astype(str)
    )

    logging.info(
        "Liczba rekordów wejściowych: %s",
        len(df),
    )

    # --------------------------------------------------------
    # CECHY
    # --------------------------------------------------------

    df = add_calendar_features(
        df
    )

    df = add_sales_for_history(
        df
    )

    df = add_exact_date_lags(
        df
    )

    # --------------------------------------------------------
    # WYMAGAMY LAG_7
    # --------------------------------------------------------

    before = len(df)

    df = df.dropna(
        subset=[
            "lag_7"
        ]
    ).copy()

    logging.info(
        "Usunięto %s rekordów bez lag_7",
        before - len(df),
    )

    # --------------------------------------------------------
    # POLA ZABRONIONE
    # --------------------------------------------------------

    drop_actual = [
        c
        for c in DROP_IF_PRESENT
        if c in df.columns
    ]

    if drop_actual:

        logging.info(
            "Usuwam pola: %s",
            drop_actual,
        )

        df = df.drop(
            columns=drop_actual
        )

    # --------------------------------------------------------
    # TRAIN TEST
    # --------------------------------------------------------

    train, test, cutoff = (
        split_by_unique_dates(
            df,
            ratio=0.8,
        )
    )

    logging.info(
        "Cutoff: %s",
        cutoff.date(),
    )

    # --------------------------------------------------------
    # OOS Z TRAIN
    # --------------------------------------------------------

    if "OOS" in train.columns:

        oos = pd.to_numeric(
            train["OOS"],
            errors="coerce",
        ).fillna(0)

        before_train = len(
            train
        )

        train = train[
            oos != 1
        ].copy()

        logging.info(
            "Wyłączono z train %s rekordów OOS",
            before_train
            - len(train),
        )

    # --------------------------------------------------------
    # SEGMENTACJA
    # --------------------------------------------------------

    (
        series_stats,
        q_low,
        q_high,
    ) = build_rotation_groups(
        train
    )

    train = attach_rotation_groups(
        train,
        series_stats,
    )

    test = attach_rotation_groups(
        test,
        series_stats,
    )

    logging.info(
        "Train: %s | Test: %s",
        len(train),
        len(test),
    )

    # ========================================================
    # 1. MODEL GLOBALNY
    # ========================================================

    logging.info(
        "Trenowanie GLOBALNEGO XGBoost..."
    )

    (
        X_train_global,
        X_test_global,
        global_feature_names,
        global_preprocessor,
    ) = prepare_one_hot(
        train,
        test,
    )

    global_model = (
        build_xgboost_model()
    )

    global_model.fit(
        X_train_global,
        train[
            TARGET
        ].astype(
            "float32"
        ),
    )

    test[
        "prediction_global"
    ] = np.clip(
        global_model.predict(
            X_test_global
        ),
        0,
        None,
    )

    logging.info(
        "Globalny model: %s cech po OneHot.",
        len(
            global_feature_names
        ),
    )

    # ========================================================
    # 2. MODELE SEGMENTOWE
    # ========================================================

    test[
        "prediction_segmented"
    ] = np.nan

    segment_meta = {}

    for group in ROTATION_GROUPS:

        logging.info(
            "Trenowanie modelu segmentu: %s",
            group,
        )

        train_group = train[
            train[
                "rotation_group"
            ]
            == group
        ].copy()

        test_group = test[
            test[
                "rotation_group"
            ]
            == group
        ].copy()

        if (
            train_group.empty
            or test_group.empty
        ):

            logging.warning(
                "Pomijam grupę %s - pusty train/test.",
                group,
            )

            continue

        (
            X_train_group,
            X_test_group,
            feature_names_group,
            preprocessor_group,
        ) = prepare_one_hot(
            train_group,
            test_group,
        )

        segment_model = (
            build_xgboost_model()
        )

        segment_model.fit(
            X_train_group,

            train_group[
                TARGET
            ].astype(
                "float32"
            ),
        )

        group_prediction = np.clip(
            segment_model.predict(
                X_test_group
            ),
            0,
            None,
        )

        test.loc[
            test_group.index,
            "prediction_segmented",
        ] = group_prediction

        segment_meta[
            group
        ] = {
            "train_rows":
                int(
                    len(
                        train_group
                    )
                ),

            "test_rows":
                int(
                    len(
                        test_group
                    )
                ),

            "features_after_one_hot":
                int(
                    len(
                        feature_names_group
                    )
                ),
        }

        logging.info(
            "%s: train=%s test=%s features=%s",
            group,
            len(
                train_group
            ),
            len(
                test_group
            ),
            len(
                feature_names_group
            ),
        )

    # ========================================================
    # EWALUACJA - BEZ OOS
    # ========================================================

    if "OOS" in test.columns:

        test_oos = pd.to_numeric(
            test["OOS"],
            errors="coerce",
        ).fillna(0)

        eval_df = test[
            test_oos != 1
        ].copy()

    else:

        eval_df = (
            test.copy()
        )

    # ========================================================
    # GLOBALNE METRYKI
    # ========================================================

    metrics = {
        "global_xgboost":
            calculate_metrics(
                eval_df[
                    TARGET
                ],

                eval_df[
                    "prediction_global"
                ],
            ),
    }

    segmented_valid = (
        eval_df[
            "prediction_segmented"
        ]
        .notna()
    )

    metrics[
        "segmented_xgboost"
    ] = calculate_metrics(
        eval_df.loc[
            segmented_valid,
            TARGET,
        ],

        eval_df.loc[
            segmented_valid,
            "prediction_segmented",
        ],
    )

    add_baseline_metrics(
        metrics,
        eval_df,
    )

    # ========================================================
    # WYNIKI WG ROTACJI
    # ========================================================

    rotation_metrics = (
        calculate_rotation_metrics(
            eval_df
        )
    )

    print(
        "\n"
        + "=" * 80
    )

    print(
        "WYNIKI WG ROTACJI"
    )

    print(
        "=" * 80
    )

    print(
        rotation_metrics.to_string(
            index=False
        )
    )

    # ========================================================
    # META
    # ========================================================

    metrics[
        "meta"
    ] = {
        "cutoff_date":
            str(
                cutoff.date()
            ),

        "train_rows":
            int(
                len(train)
            ),

        "test_rows":
            int(
                len(test)
            ),

        "evaluation_rows":
            int(
                len(
                    eval_df
                )
            ),

        "rotation_thresholds": {
            "low_medium":
                float(
                    q_low
                ),

            "medium_high":
                float(
                    q_high
                ),
        },

        "global_features_after_one_hot":
            int(
                len(
                    global_feature_names
                )
            ),

        "segment_models":
            segment_meta,

        "numeric_features":
            [
                c
                for c
                in NUMERIC_CANDIDATES
                if c
                in train.columns
            ],

        "categorical_features":
            [
                c
                for c
                in CATEGORICAL_CANDIDATES
                if c
                in train.columns
            ],

        "notes": [
            "Model globalny i modele segmentowe używają identycznych hiperparametrów XGBoost.",

            "Grupy low, medium i high są wyznaczane wyłącznie na podstawie danych treningowych.",

            "Podstawą segmentacji jest średnia dzienna sprzedaż dla StoreNo + IDX.",

            "OOS jest wyłączone z treningu i ewaluacji.",

            "Sprzedaż z dni OOS nie jest wykorzystywana do budowania lagów.",

            "SalesValue i stock_price_net nie są używane jako cechy modelu.",

            "Promo jest używane jako cecha.",

            "discount_percent nie jest wykorzystywany po wcześniejszym eksperymencie ablation.",

            "Zmienne StoreNo, IDX, Brand i DIV2 są kodowane przez OneHotEncoder.",
        ],
    }

    # ========================================================
    # ZAPIS
    # ========================================================

    (
        REPORTS
        / "segmented_metrics.json"
    ).write_text(
        json.dumps(
            metrics,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    rotation_metrics.to_csv(
        REPORTS
        / "segmented_rotation_metrics.csv",
        index=False,
    )

    series_stats.to_csv(
        REPORTS
        / "segmented_series_stats.csv",
        index=False,
    )

    prediction_cols = [
        DATE_COL,
        "StoreNo",
        "IDX",
        "rotation_group",
        TARGET,
        "prediction_global",
        "prediction_segmented",
        "lag_1",
        "lag_7",
        "lag_14",
        "lag_28",
        "history_mean_4lags",
        "promo",
        "OOS",
    ]

    prediction_cols = [
        c
        for c in prediction_cols
        if c in eval_df.columns
    ]

    eval_df[
        prediction_cols
    ].to_csv(
        REPORTS
        / "segmented_predictions.csv",
        index=False,
    )

    save_segment_comparison_plot(
        rotation_metrics
    )

    # ========================================================
    # OUTPUT
    # ========================================================

    print(
        "\n"
        + "=" * 80
    )

    print(
        "WYNIKI GLOBALNE"
    )

    print(
        "=" * 80
    )

    print(
        json.dumps(
            metrics,
            indent=2,
            ensure_ascii=False,
        )
    )

    logging.info(
        "Gotowe. Wyniki zapisane w %s",
        REPORTS,
    )


if __name__ == "__main__":
    run()

