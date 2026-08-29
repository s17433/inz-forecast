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

# Każda seria = jeden produkt w jednym sklepie.
GROUP_COLS = [
    "StoreNo",
    "IDX",
]

# Historyczne okresy sprzedaży.
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
# promo zostawiamy,
# discount_percent wyłączamy.
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
# OOS I HISTORIA SPRZEDAŻY
# ============================================================

def add_sales_for_history(df):

    df = df.copy()

    df["SalesForHistory"] = pd.to_numeric(
        df[TARGET],
        errors="coerce",
    )

    if "OOS" in df.columns:

        oos_numeric = pd.to_numeric(
            df["OOS"],
            errors="coerce",
        ).fillna(0)

        # Dzień OOS nie reprezentuje rzeczywistego popytu.
        df.loc[
            oos_numeric == 1,
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

    required = (
        GROUP_COLS
        + [
            DATE_COL,
            "SalesForHistory",
        ]
    )

    missing = [
        col
        for col in required
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Brak kolumn potrzebnych do lagów: {missing}"
        )

    result = df.copy()

    duplicated = result.duplicated(
        GROUP_COLS + [DATE_COL],
        keep=False,
    )

    if duplicated.any():
        raise ValueError(
            f"W danych znaleziono "
            f"{int(duplicated.sum())} "
            f"wierszy należących do zduplikowanych kombinacji "
            f"{GROUP_COLS + [DATE_COL]}"
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

        # Rekord z dnia t-lag przesuwamy do dnia t.
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

    cutoff_date = pd.Timestamp(
        dates[
            cutoff_idx - 1
        ]
    )

    train = df[
        df[DATE_COL]
        <= cutoff_date
    ].copy()

    test = df[
        df[DATE_COL]
        > cutoff_date
    ].copy()

    return (
        train,
        test,
        cutoff_date,
    )


# ============================================================
# SEGMENTACJA ROTACJI
# ============================================================

def build_rotation_groups(
    train: pd.DataFrame
):
    """
    Wyznacza low / medium / high rotation
    wyłącznie na podstawie zbioru treningowego.

    Podstawą jest średnia dzienna sprzedaż
    konkretnej pary StoreNo + IDX.
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

    # --------------------------------------------------------
    # Progi wyznaczamy z danych treningowych.
    # --------------------------------------------------------

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
            [
                "low",
                "medium",
                "high",
            ]
        )
    )

    print(
        "\nStatystyki grup:"
    )

    rotation_summary = (
        series_stats
        .groupby(
            "rotation_group"
        )[
            [
                "mean_daily_sales",
                "positive_day_share",
                "zero_day_share",
            ]
        ]
        .mean()
        .reindex(
            [
                "low",
                "medium",
                "high",
            ]
        )
    )

    print(
        rotation_summary
    )

    return (
        series_stats,
        q_low,
        q_high,
    )


def attach_rotation_groups(
    df: pd.DataFrame,
    series_stats: pd.DataFrame,
):
    """
    Dołącza grupę rotacji wyznaczoną wcześniej na train.
    """

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

    result = df.merge(
        lookup,
        on=GROUP_COLS,
        how="left",
        validate="many_to_one",
    )

    return result


# ============================================================
# ONE HOT ENCODING
# ============================================================

def prepare_features_one_hot(
    train,
    test,
):

    train = train.copy()
    test = test.copy()

    numeric_features = [
        col
        for col in NUMERIC_CANDIDATES
        if col in train.columns
    ]

    categorical_features = [
        col
        for col in CATEGORICAL_CANDIDATES
        if col in train.columns
    ]

    # --------------------------------------------------------
    # NUMERYCZNE
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # KATEGORIE
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # OneHotEncoder
    # --------------------------------------------------------

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
        train,
        test,
        X_train,
        X_test,
        feature_names,
        preprocessor,
    )


# ============================================================
# BASELINE
# ============================================================

def make_baselines(test):

    baselines = {}

    if "lag_1" in test.columns:

        baselines[
            "naive_t_minus_1"
        ] = test[
            "lag_1"
        ]

    if "lag_7" in test.columns:

        baselines[
            "seasonal_naive_t_minus_7"
        ] = test[
            "lag_7"
        ]

    return baselines


# ============================================================
# ANALIZA WEDŁUG ROTACJI
# ============================================================

def calculate_rotation_metrics(
    eval_df: pd.DataFrame
):
    """
    Liczy osobne metryki dla:
    low / medium / high rotation.
    """

    rows = []

    group_order = [
        "low",
        "medium",
        "high",
    ]

    for rotation_group in group_order:

        group_df = eval_df[
            eval_df[
                "rotation_group"
            ]
            == rotation_group
        ].copy()

        if group_df.empty:
            continue

        # ====================================================
        # XGBOOST
        # ====================================================

        xgb_metrics = calculate_metrics(
            group_df[
                TARGET
            ],

            group_df[
                "prediction_xgboost"
            ],
        )

        rows.append(
            {
                "rotation_group":
                    rotation_group,

                "model":
                    "xgboost",

                "rows":
                    int(
                        len(
                            group_df
                        )
                    ),

                "actual_mean_sales":
                    round(
                        float(
                            group_df[
                                TARGET
                            ].mean()
                        ),
                        4,
                    ),

                "zero_sales_percent":
                    round(
                        float(
                            (
                                group_df[
                                    TARGET
                                ]
                                == 0
                            ).mean()
                            * 100
                        ),
                        2,
                    ),

                **xgb_metrics,
            }
        )

        # ====================================================
        # NAIVE T-1
        # ====================================================

        if "lag_1" in group_df.columns:

            valid = (
                group_df[
                    "lag_1"
                ]
                .notna()
            )

            if valid.any():

                baseline_metrics = (
                    calculate_metrics(
                        group_df.loc[
                            valid,
                            TARGET,
                        ],

                        group_df.loc[
                            valid,
                            "lag_1",
                        ],
                    )
                )

                rows.append(
                    {
                        "rotation_group":
                            rotation_group,

                        "model":
                            "naive_t_minus_1",

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

                        **baseline_metrics,
                    }
                )

        # ====================================================
        # SEASONAL NAIVE T-7
        # ====================================================

        if "lag_7" in group_df.columns:

            valid = (
                group_df[
                    "lag_7"
                ]
                .notna()
            )

            if valid.any():

                baseline_metrics = (
                    calculate_metrics(
                        group_df.loc[
                            valid,
                            TARGET,
                        ],

                        group_df.loc[
                            valid,
                            "lag_7",
                        ],
                    )
                )

                rows.append(
                    {
                        "rotation_group":
                            rotation_group,

                        "model":
                            "seasonal_naive_t_minus_7",

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

                        **baseline_metrics,
                    }
                )

    return pd.DataFrame(
        rows
    )


# ============================================================
# FEATURE IMPORTANCE
# ============================================================

def save_feature_importance(
    model,
    feature_names,
):

    importance = pd.DataFrame(
        {
            "feature":
                feature_names,

            "importance":
                model.feature_importances_,
        }
    )

    importance = (
        importance
        .sort_values(
            "importance",
            ascending=False,
        )
    )

    importance.to_csv(
        REPORTS
        / "feature_importance.csv",
        index=False,
    )

    top = (
        importance
        .head(25)
        .sort_values(
            "importance",
            ascending=True,
        )
    )

    plt.figure(
        figsize=(10, 7)
    )

    plt.barh(
        top[
            "feature"
        ],
        top[
            "importance"
        ],
    )

    plt.xlabel(
        "Feature importance"
    )

    plt.title(
        "XGBoost - najważniejsze cechy"
    )

    plt.tight_layout()

    plt.savefig(
        PLOTS
        / "feature_importance.png",
        dpi=160,
    )

    plt.close()


# ============================================================
# MODEL COMPARISON
# ============================================================

def save_model_comparison(
    metrics
):

    rows = []

    for (
        model_name,
        values
    ) in metrics.items():

        if model_name == "meta":
            continue

        rows.append(
            {
                "model":
                    model_name,

                **values,
            }
        )

    comparison = pd.DataFrame(
        rows
    )

    comparison.to_csv(
        REPORTS
        / "model_comparison.csv",
        index=False,
    )

    if comparison.empty:
        return

    plt.figure(
        figsize=(8, 4)
    )

    plt.bar(
        comparison[
            "model"
        ],
        comparison[
            "WAPE_percent"
        ],
    )

    plt.ylabel(
        "WAPE [%]"
    )

    plt.title(
        "Porównanie modeli"
    )

    plt.xticks(
        rotation=20,
        ha="right",
    )

    plt.tight_layout()

    plt.savefig(
        PLOTS
        / "model_comparison.png",
        dpi=160,
    )

    plt.close()


# ============================================================
# WYKRES ROTACJI
# ============================================================

def save_rotation_comparison_plot(
    rotation_metrics: pd.DataFrame
):

    if rotation_metrics.empty:
        return

    plot_df = rotation_metrics[
        rotation_metrics[
            "model"
        ]
        == "xgboost"
    ].copy()

    if plot_df.empty:
        return

    order = [
        "low",
        "medium",
        "high",
    ]

    plot_df[
        "rotation_group"
    ] = pd.Categorical(
        plot_df[
            "rotation_group"
        ],
        categories=order,
        ordered=True,
    )

    plot_df = (
        plot_df
        .sort_values(
            "rotation_group"
        )
    )

    plt.figure(
        figsize=(7, 4)
    )

    plt.bar(
        plot_df[
            "rotation_group"
        ].astype(str),

        plot_df[
            "WAPE_percent"
        ],
    )

    plt.ylabel(
        "WAPE [%]"
    )

    plt.xlabel(
        "Grupa rotacji"
    )

    plt.title(
        "XGBoost - WAPE według rotacji"
    )

    plt.tight_layout()

    plt.savefig(
        PLOTS
        / "rotation_wape.png",
        dpi=160,
    )

    plt.close()


# ============================================================
# PRZYKŁADOWY WYKRES PROGNOZY
# ============================================================

def save_example_prediction_plot(
    test_eval,
):

    counts = (
        test_eval
        .groupby(
            [
                "StoreNo",
                "IDX",
            ]
        )
        .size()
        .sort_values(
            ascending=False
        )
    )

    if counts.empty:
        return

    store, idx = (
        counts.index[0]
    )

    sample = (
        test_eval[
            (
                test_eval[
                    "StoreNo"
                ]
                == store
            )
            &
            (
                test_eval[
                    "IDX"
                ]
                == idx
            )
        ]
        .sort_values(
            DATE_COL
        )
    )

    if sample.empty:
        return

    plt.figure(
        figsize=(11, 4)
    )

    plt.plot(
        sample[
            DATE_COL
        ],

        sample[
            TARGET
        ],

        label=
            "sprzedaż rzeczywista",
    )

    plt.plot(
        sample[
            DATE_COL
        ],

        sample[
            "prediction_xgboost"
        ],

        label=
            "prognoza XGBoost",
    )

    if "lag_7" in sample.columns:

        plt.plot(
            sample[
                DATE_COL
            ],

            sample[
                "lag_7"
            ],

            label=
                "seasonal naive t-7",

            alpha=0.7,
        )

    plt.title(
        f"Przykład prognozy: "
        f"sklep {store}, "
        f"IDX {idx}"
    )

    plt.xlabel(
        "Data"
    )

    plt.ylabel(
        "Sprzedaż"
    )

    plt.legend()

    plt.tight_layout()

    plt.savefig(
        PLOTS
        / "prediction_example.png",
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

    if not data_path.exists():
        raise FileNotFoundError(
            f"Nie znaleziono: {data_path}"
        )

    REPORTS.mkdir(
        parents=True,
        exist_ok=True,
    )

    PLOTS.mkdir(
        parents=True,
        exist_ok=True,
    )


    # ========================================================
    # WCZYTANIE
    # ========================================================

    logging.info(
        "Wczytywanie danych: %s",
        data_path,
    )

    df = pd.read_parquet(
        data_path
    )

    required = (
        GROUP_COLS
        + [
            DATE_COL,
            TARGET,
        ]
    )

    missing = [
        col
        for col in required
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Brak wymaganych kolumn: {missing}"
        )

    df[
        DATE_COL
    ] = pd.to_datetime(
        df[
            DATE_COL
        ],
        errors="coerce",
    )

    df[
        TARGET
    ] = pd.to_numeric(
        df[
            TARGET
        ],
        errors="coerce",
    )

    df = df.dropna(
        subset=required
    ).copy()

    df[
        "StoreNo"
    ] = (
        df[
            "StoreNo"
        ]
        .astype(str)
    )

    df[
        "IDX"
    ] = (
        df[
            "IDX"
        ]
        .astype(str)
    )


    # ========================================================
    # DIAGNOSTYKA DANYCH
    # ========================================================

    print(
        "\n"
        + "=" * 60
    )

    print(
        "DIAGNOSTYKA DANYCH WEJŚCIOWYCH"
    )

    print(
        "=" * 60
    )

    sales_numeric = pd.to_numeric(
        df[
            TARGET
        ],
        errors="coerce",
    )

    zero_sales = (
        sales_numeric
        == 0
    ).sum()

    positive_sales = (
        sales_numeric
        > 0
    ).sum()

    print(
        "\n=== SALES ==="
    )

    print(
        f"Wszystkie rekordy: "
        f"{len(df)}"
    )

    print(
        f"Sales = 0: "
        f"{zero_sales} "
        f"({zero_sales / len(df) * 100:.2f}%)"
    )

    print(
        f"Sales > 0: "
        f"{positive_sales} "
        f"({positive_sales / len(df) * 100:.2f}%)"
    )

    print(
        f"Sales NaN: "
        f"{sales_numeric.isna().sum()}"
    )


    if "OOS" in df.columns:

        print(
            "\n=== OOS ==="
        )

        print(
            df[
                "OOS"
            ]
            .value_counts(
                dropna=False
            )
            .sort_index()
        )


    if "promo" in df.columns:

        print(
            "\n=== PROMO ==="
        )

        print(
            df[
                "promo"
            ]
            .value_counts(
                dropna=False
            )
            .sort_index()
        )


    if "discount_percent" in df.columns:

        print(
            "\n=== DISCOUNT_PERCENT ==="
        )

        print(
            "NaN:",
            df[
                "discount_percent"
            ]
            .isna()
            .sum()
        )

        print(
            "= 0:",
            (
                df[
                    "discount_percent"
                ]
                == 0
            ).sum()
        )

        print(
            "> 0:",
            (
                df[
                    "discount_percent"
                ]
                > 0
            ).sum()
        )


    # ========================================================
    # FEATURE ENGINEERING
    # ========================================================

    logging.info(
        "Liczba wierszy przed feature engineering: %s",
        len(df),
    )

    df = add_calendar_features(
        df
    )

    df = add_sales_for_history(
        df
    )

    df = add_exact_date_lags(
        df
    )


    # ========================================================
    # DIAGNOSTYKA LAG_7
    # ========================================================

    print(
        "\n=== ANALIZA BRAKU LAG_7 ==="
    )

    first_dates = (
        df.groupby(
            GROUP_COLS
        )[
            DATE_COL
        ]
        .transform(
            "min"
        )
    )

    df[
        "days_from_series_start"
    ] = (
        df[
            DATE_COL
        ]
        - first_dates
    ).dt.days

    missing_lag7 = (
        df[
            "lag_7"
        ]
        .isna()
    )

    missing_first_week = (
        missing_lag7
        &
        (
            df[
                "days_from_series_start"
            ]
            < 7
        )
    ).sum()

    missing_later = (
        missing_lag7
        &
        (
            df[
                "days_from_series_start"
            ]
            >= 7
        )
    ).sum()

    print(
        "Brak lag_7 "
        "w pierwszych 7 dniach historii:",
        missing_first_week,
    )

    print(
        "Brak lag_7 "
        "po pierwszych 7 dniach historii:",
        missing_later,
    )

    print(
        "Serie StoreNo+IDX:",
        df[
            GROUP_COLS
        ]
        .drop_duplicates()
        .shape[0],
    )


    # ========================================================
    # USUNIĘCIE BRAKU LAG_7
    # ========================================================

    before = len(df)

    df = df.dropna(
        subset=[
            "lag_7"
        ]
    ).copy()

    logging.info(
        "Usunięto %s wierszy bez lag_7",
        before - len(df),
    )


    # ========================================================
    # POLA NIEUŻYWANE
    # ========================================================

    drop_actual = [
        col
        for col in DROP_IF_PRESENT
        if col in df.columns
    ]

    if drop_actual:

        logging.info(
            "Usuwam z wejścia pola: %s",
            drop_actual,
        )

        df = df.drop(
            columns=drop_actual
        )


    # ========================================================
    # TRAIN / TEST
    # ========================================================

    train, test, cutoff_date = (
        split_by_unique_dates(
            df,
            ratio=0.8,
        )
    )

    logging.info(
        "Cutoff train/test: %s",
        cutoff_date.date(),
    )


    # ========================================================
    # OOS W TRAIN
    # ========================================================

    if "OOS" in train.columns:

        train_oos = pd.to_numeric(
            train[
                "OOS"
            ],
            errors="coerce",
        ).fillna(0)

        before_train = len(
            train
        )

        train = train.loc[
            train_oos != 1
        ].copy()

        logging.info(
            "Wyłączono z treningu %s wierszy OOS",
            before_train
            - len(train),
        )


    logging.info(
        "Train: %s wierszy | Test: %s wierszy",
        len(train),
        len(test),
    )


    # ========================================================
    # SEGMENTACJA ROTACJI
    # ========================================================

    (
        series_stats,
        rotation_q_low,
        rotation_q_high,
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


    # ========================================================
    # MASKA EWALUACJI
    # ========================================================

    if "OOS" in test.columns:

        test_oos = pd.to_numeric(
            test[
                "OOS"
            ],
            errors="coerce",
        ).fillna(0)

        eval_mask = (
            test_oos
            != 1
        )

    else:

        eval_mask = pd.Series(
            True,
            index=test.index,
        )


    # ========================================================
    # ONE HOT
    # ========================================================

    (
        train,
        test,
        X_train,
        X_test,
        feature_names,
        preprocessor,
    ) = prepare_features_one_hot(
        train,
        test,
    )


    y_train = (
        train[
            TARGET
        ]
        .astype(
            "float32"
        )
    )


    logging.info(
        "Po OneHotEncoder liczba cech: %s",
        len(
            feature_names
        ),
    )


    # ========================================================
    # MODEL
    # ========================================================

    model = xgb.XGBRegressor(

        objective=
            "reg:squarederror",

        n_estimators=500,

        max_depth=8,

        learning_rate=0.05,

        subsample=0.8,

        colsample_bytree=0.8,

        random_state=42,

        n_jobs=-1,

        tree_method="hist",
    )


    logging.info(
        "Trenowanie XGBoost "
        "na %s cechach po OneHotEncoder...",
        len(
            feature_names
        ),
    )


    model.fit(
        X_train,
        y_train,
    )


    # ========================================================
    # PREDYKCJA
    # ========================================================

    prediction = model.predict(
        X_test
    )

    prediction = np.clip(
        prediction,
        a_min=0,
        a_max=None,
    )

    test[
        "prediction_xgboost"
    ] = prediction


    # ========================================================
    # EWALUACJA
    # ========================================================

    eval_df = test.loc[
        eval_mask
    ].copy()


    if eval_df.empty:
        raise ValueError(
            "Brak rekordów do ewaluacji."
        )


    metrics = {

        "xgboost":
            calculate_metrics(
                eval_df[
                    TARGET
                ],

                eval_df[
                    "prediction_xgboost"
                ],
            )
    }


    # ========================================================
    # BASELINE
    # ========================================================

    baselines = make_baselines(
        eval_df
    )


    for (
        baseline_name,
        prediction_series
    ) in baselines.items():

        valid = (
            prediction_series
            .notna()
        )

        if not valid.any():
            continue

        metrics[
            baseline_name
        ] = calculate_metrics(

            eval_df.loc[
                valid,
                TARGET
            ],

            prediction_series.loc[
                valid
            ],
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
        "\n=== WYNIKI WG ROTACJI ==="
    )

    print(
        rotation_metrics.to_string(
            index=False
        )
    )


    rotation_metrics.to_csv(
        REPORTS
        / "rotation_metrics.csv",
        index=False,
    )


    series_stats.to_csv(
        REPORTS
        / "rotation_series_stats.csv",
        index=False,
    )


    save_rotation_comparison_plot(
        rotation_metrics
    )


    # ========================================================
    # META
    # ========================================================

    metrics[
        "meta"
    ] = {

        "cutoff_date":
            str(
                cutoff_date.date()
            ),

        "train_rows":
            int(
                len(
                    train
                )
            ),

        "test_rows":
            int(
                len(
                    test
                )
            ),

        "evaluation_rows":
            int(
                len(
                    eval_df
                )
            ),

        "excluded_oos_rows":
            int(
                len(
                    test
                )
                - len(
                    eval_df
                )
            ),

        "n_features_after_one_hot":
            len(
                feature_names
            ),

        "numeric_features":
            [
                col
                for col
                in NUMERIC_CANDIDATES
                if col in train.columns
            ],

        "categorical_features":
            [
                col
                for col
                in CATEGORICAL_CANDIDATES
                if col in train.columns
            ],

        "rotation_thresholds": {

            "low_medium":
                float(
                    rotation_q_low
                ),

            "medium_high":
                float(
                    rotation_q_high
                ),
        },

        "notes": [

            "SalesValue i stock_price_net "
            "nie są używane jako cechy modelu.",

            "OOS nie jest używane jako cecha.",

            "Rekordy OOS są wyłączone "
            "z treningu i ewaluacji.",

            "Sprzedaż podczas OOS "
            "nie jest używana do lagów.",

            "Lagi są liczone "
            "po dokładnych datach kalendarzowych.",

            "StoreNo, Brand, DIV2 i IDX "
            "są kodowane przez OneHotEncoder.",

            "OneHotEncoder jest dopasowany "
            "wyłącznie na zbiorze treningowym.",

            "Promo jest wykorzystywane "
            "jako cecha modelu.",

            "discount_percent został wyłączony "
            "po eksperymencie ablation, "
            "ponieważ nie poprawiał jakości prognozy.",

            "Grupy low, medium i high rotation "
            "są wyznaczane wyłącznie "
            "na podstawie zbioru treningowego.",

            "Podstawą segmentacji jest "
            "średnia dzienna sprzedaż "
            "dla StoreNo + IDX.",
        ],
    }


    # ========================================================
    # ZAPIS WYNIKÓW
    # ========================================================

    (
        REPORTS
        / "metrics_v2.json"
    ).write_text(

        json.dumps(
            metrics,
            indent=2,
            ensure_ascii=False,
        ),

        encoding="utf-8",
    )


    save_feature_importance(
        model,
        feature_names,
    )


    save_model_comparison(
        metrics
    )


    save_example_prediction_plot(
        eval_df
    )


    # ========================================================
    # PREDYKCJE CSV
    # ========================================================

    prediction_cols = [

        DATE_COL,
        "StoreNo",
        "IDX",

        "rotation_group",

        TARGET,

        "prediction_xgboost",

    ] + [

        col
        for col in [

            "lag_1",
            "lag_7",
            "lag_14",
            "lag_28",

            "history_mean_4lags",

            "promo",

            "OOS",

        ]

        if col in eval_df.columns
    ]


    eval_df[
        prediction_cols
    ].to_csv(

        REPORTS
        / "predictions_test.csv",

        index=False,
    )


    logging.info(
        "Gotowe. "
        "Wyniki zapisano w %s",
        REPORTS,
    )


    print(
        "\n=== WYNIKI GLOBALNE ==="
    )

    print(
        json.dumps(
            metrics,
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    run()