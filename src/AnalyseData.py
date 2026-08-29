"""Basic validation and descriptive analysis of the merged forecasting dataset."""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from config import FINAL, REPORTS, PLOTS


INPUT_PATH = FINAL / "MergedData.parquet"
OUTPUT_PATH = FINAL / "MergedDataAfter.parquet"
STATS_PATH = REPORTS / "descriptive_statistics.csv"
SALES_PLOT_PATH = PLOTS / "sales_distribution.png"


def prepare_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Apply final deterministic cleaning before modelling."""
    df = df.copy()

    if "ID" in df.columns:
        df = df.drop(columns=["ID"])

    for col in ["Sales", "SalesValue", "discount_percent"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            df.loc[df[col] < 0, col] = 0

    if "OOS" in df.columns:
        df["OOS"] = pd.to_numeric(df["OOS"], errors="coerce").fillna(0).astype(int)

    if "promo" in df.columns:
        df["promo"] = pd.to_numeric(df["promo"], errors="coerce").fillna(0).astype(int)

    if "DateNo" in df.columns:
        df["DateNo"] = pd.to_datetime(df["DateNo"], errors="coerce")

    return df


def save_sales_distribution(df: pd.DataFrame) -> None:
    if "Sales" not in df.columns:
        return

    plt.figure(figsize=(8, 5))
    plt.hist(df["Sales"].dropna(), bins=30)
    plt.title("Rozkład dziennej sprzedaży")
    plt.xlabel("Sales")
    plt.ylabel("Liczba obserwacji")
    plt.tight_layout()
    plt.savefig(SALES_PLOT_PATH, dpi=160)
    plt.close()


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Nie znaleziono pliku wejściowego: {INPUT_PATH}")

    REPORTS.mkdir(parents=True, exist_ok=True)
    PLOTS.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(INPUT_PATH)
    df = prepare_dataset(df)

    df.to_parquet(OUTPUT_PATH, engine="pyarrow", compression="snappy", index=False)
    df.describe(include="all").transpose().to_csv(STATS_PATH)
    save_sales_distribution(df)

    print(f"Liczba rekordów: {len(df)}")
    print(f"Liczba kolumn: {len(df.columns)}")
    print("\nBrakujące wartości:")
    print(df.isna().sum().sort_values(ascending=False).head(20))

    if "Sales" in df.columns:
        zero_sales = int((df["Sales"] == 0).sum())
        positive_sales = int((df["Sales"] > 0).sum())
        print(f"\nSales = 0: {zero_sales} ({zero_sales / len(df) * 100:.2f}%)")
        print(f"Sales > 0: {positive_sales} ({positive_sales / len(df) * 100:.2f}%)")

    print(f"\nZapisano: {OUTPUT_PATH}")
    print(f"Statystyki: {STATS_PATH}")
    print(f"Wykres: {SALES_PLOT_PATH}")


if __name__ == "__main__":
    main()
