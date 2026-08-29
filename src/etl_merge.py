# src/etl_merge_v2.py
"""
ETL pamięciooszczędny (bez Dask): czyta FINAL/* partiami (po sklepach) przez pyarrow.dataset,
łączy w pandas tylko wycinki i dopisuje wynik do jednego pliku Parquet.

Konfiguracja ograniczeń:
- TOP_N_STORES: ile sklepów wziąć (None = wszystkie)
- TOP_N_IDXS:   ile top IDX na sklep (None = wszystkie)

Uruchomienie:
    python src/etl_merge_v2.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from config import FINAL


# -------------------------------------------------------------------------
# USTAWIENIA
# -------------------------------------------------------------------------

TOP_N_STORES: int | None = 2
TOP_N_IDXS: int | None = 200

OUTPUT_NAME = "MergedData.parquet"


def _to_dt(s: pd.Series) -> pd.Series:
    """
    Bezpieczna konwersja do datetime.
    Obsługuje m.in. wartości zapisane jako YYYYMMDD.
    """
    return (
        s
        if pd.api.types.is_datetime64_any_dtype(s)
        else pd.to_datetime(s.astype(str), errors="coerce")
    )


def _dataset(path: Path, format_hint: str = "parquet") -> ds.Dataset:
    """
    Tworzy PyArrow Dataset.
    """
    return ds.dataset(str(path), format=format_hint)


def _distinct_values(
    ds_obj: ds.Dataset,
    column: str,
    limit: int | None = None,
) -> list[str]:
    """
    Zbiera wartości kolumny i sortuje je według częstości występowania.
    """
    scanner = ds.Scanner.from_dataset(
        ds_obj,
        columns=[column],
    )

    counts: dict[str, int] = {}

    for batch in scanner.to_batches():
        ser = (
            batch
            .to_pandas()[column]
            .dropna()
            .astype(str)
        )

        for value in ser:
            counts[value] = counts.get(value, 0) + 1

    ordered = sorted(
        counts.items(),
        key=lambda kv: kv[1],
        reverse=True,
    )

    values = [key for key, _ in ordered]

    return values[:limit] if limit else values


def build_merged_streaming() -> str:

    # ---------------------------------------------------------------------
    # 1. Duże zbiory jako PyArrow Dataset
    # ---------------------------------------------------------------------

    sales_ds = _dataset(
        FINAL / "Sales_cleaned.parquet"
    )

    plano_ds = _dataset(
        FINAL / "PlanogramChStores.parquet"
    )

    oos_ds = _dataset(
        FINAL / "OutOfStock.parquet"
    )

    promos_ds = _dataset(
        FINAL / "Promotions.parquet"
    )

    # ---------------------------------------------------------------------
    # 2. Małe zbiory do RAM
    # ---------------------------------------------------------------------

    idxs = (
        pd.read_parquet(
            FINAL / "IDXs_cleaned.parquet"
        )[
            ["IDX", "Brand", "DIV2"]
        ]
        .dropna(
            subset=["Brand", "DIV2"]
        )
    )

    chosen = (
        pd.read_parquet(
            FINAL / "ChosenIDXs.parquet"
        )[
            ["IDX"]
        ]
        .drop_duplicates()
    )

    idxs["IDX"] = idxs["IDX"].astype(str)
    chosen["IDX"] = chosen["IDX"].astype(str)

    # ---------------------------------------------------------------------
    # 3. Wybór sklepów
    # ---------------------------------------------------------------------

    stores_all = _distinct_values(
        sales_ds,
        "StoreNo",
        limit=TOP_N_STORES,
    )

    print(
        f"[INFO] Stores to process: "
        f"{len(stores_all)} -> "
        f"{stores_all[:10]}"
        f"{' ...' if len(stores_all) > 10 else ''}"
    )

    # ---------------------------------------------------------------------
    # 4. Writer Parquet
    # ---------------------------------------------------------------------

    out_path = FINAL / OUTPUT_NAME

    if out_path.exists():
        out_path.unlink()

    writer: Optional[pq.ParquetWriter] = None

    try:

        for store in stores_all:

            print("\n" + "=" * 70)
            print(f"SKLEP: {store}")
            print("=" * 70)

            # =============================================================
            # 4.1 SALES
            # =============================================================

            sales_cols = [
                c
                for c in [
                    "StoreNo",
                    "IDX",
                    "DateNo",
                    "Sales",
                    "SalesValue",
                    "Price",
                    "ID",
                ]
                if c in sales_ds.schema.names
            ]

            sales_tbl = sales_ds.to_table(
                columns=sales_cols,
                filter=(
                    ds.field("StoreNo") == store
                ),
            )

            if sales_tbl.num_rows == 0:
                continue

            sales = sales_tbl.to_pandas(
                use_threads=True
            )

            if "StoreNo" in sales.columns:
                sales["StoreNo"] = (
                    sales["StoreNo"]
                    .astype(str)
                )

            if "IDX" in sales.columns:
                sales["IDX"] = (
                    sales["IDX"]
                    .astype(str)
                )

            if "DateNo" in sales.columns:
                sales["DateNo"] = _to_dt(
                    sales["DateNo"]
                )

            # =============================================================
            # 4.2 CHOSEN IDX
            # =============================================================

            before = len(sales)

            sales = sales.merge(
                chosen,
                on="IDX",
                how="inner",
            )

            print(
                f"[{store}] sales chosen: "
                f"{before} -> {len(sales)}"
            )

            if sales.empty:
                continue

            # =============================================================
            # 4.3 TOP IDX
            # =============================================================

            if (
                TOP_N_IDXS is not None
                and "IDX" in sales.columns
            ):

                top_idx = (
                    sales["IDX"]
                    .value_counts()
                    .head(TOP_N_IDXS)
                    .index
                )

                sales = sales[
                    sales["IDX"].isin(top_idx)
                ]

                print(
                    f"[{store}] sales limit idx: "
                    f"-> {len(sales)} rows"
                )

            # =============================================================
            # Kontrola duplikatów sprzedaży
            # =============================================================

            sales_key_cols = [
                "StoreNo",
                "IDX",
                "DateNo",
            ]

            sales_dupes = sales.duplicated(
                sales_key_cols,
                keep=False,
            )

            if sales_dupes.any():

                sample = sales.loc[
                    sales_dupes,
                    sales_key_cols,
                ].head(10)

                raise ValueError(
                    f"[{store}] Sales zawiera "
                    f"{int(sales_dupes.sum())} wierszy "
                    f"w zduplikowanych "
                    f"StoreNo+IDX+DateNo.\n"
                    f"Przykład:\n{sample}"
                )

            # =============================================================
            # 4.4 PLANOGRAM
            # =============================================================

            plano_cols = [
                c
                for c in [
                    "source_stock_date",
                    "location_id",
                    "product_id",
                    "stock_price_net",
                ]
                if c in plano_ds.schema.names
            ]

            plano_filter = None

            if "location_id" in plano_ds.schema.names:
                plano_filter = (
                    ds.field("location_id") == store
                )

            plano_tbl = plano_ds.to_table(
                columns=plano_cols,
                filter=plano_filter,
            )

            planogram = plano_tbl.to_pandas(
                use_threads=True
            )

            if not planogram.empty:

                planogram = planogram.rename(
                    columns={
                        "source_stock_date": "DateNo",
                        "location_id": "StoreNo",
                        "product_id": "IDX",
                    }
                )

                planogram["StoreNo"] = (
                    planogram["StoreNo"]
                    .astype(str)
                )

                planogram["IDX"] = (
                    planogram["IDX"]
                    .astype(str)
                )

                planogram["DateNo"] = _to_dt(
                    planogram["DateNo"]
                )

                keep = [
                    "DateNo",
                    "StoreNo",
                    "IDX",
                ]

                if (
                    "stock_price_net"
                    in planogram.columns
                ):
                    keep.append(
                        "stock_price_net"
                    )

                planogram = planogram[keep]

            # =============================================================
            # 4.5 OOS
            # =============================================================

            oos_cols = [
                c
                for c in [
                    "StoreNo",
                    "IDX",
                    "DateNo",
                    "Ous",
                    "OOS",
                    "dateno",
                ]
                if c in oos_ds.schema.names
            ]

            oos_filter = None

            if "StoreNo" in oos_ds.schema.names:
                oos_filter = (
                    ds.field("StoreNo") == store
                )

            oos_tbl = oos_ds.to_table(
                columns=oos_cols,
                filter=oos_filter,
            )

            oos = oos_tbl.to_pandas(
                use_threads=True
            )

            if not oos.empty:

                if "dateno" in oos.columns:
                    oos = oos.rename(
                        columns={
                            "dateno": "DateNo"
                        }
                    )

                if "Ous" in oos.columns:
                    oos = oos.rename(
                        columns={
                            "Ous": "OOS"
                        }
                    )

                oos["StoreNo"] = (
                    oos["StoreNo"]
                    .astype(str)
                )

                oos["IDX"] = (
                    oos["IDX"]
                    .astype(str)
                )

                oos["DateNo"] = _to_dt(
                    oos["DateNo"]
                )

                oos = oos[
                    [
                        "StoreNo",
                        "IDX",
                        "DateNo",
                    ]
                ].copy()

                oos["OOS"] = 1

            # =============================================================
            # 4.6 PEŁNY PANEL DZIENNY
            # =============================================================

            key_cols = [
                "StoreNo",
                "IDX",
                "DateNo",
            ]

            selected_idxs = set(
                sales["IDX"]
                .astype(str)
                .unique()
            )

            panel_parts = []

            if (
                not planogram.empty
                and all(
                    key in planogram.columns
                    for key in key_cols
                )
            ):

                planogram = planogram[
                    planogram["IDX"].isin(
                        selected_idxs
                    )
                ].copy()

                panel_parts.append(
                    planogram[
                        key_cols
                    ].drop_duplicates()
                )

            panel_parts.append(
                sales[
                    key_cols
                ].drop_duplicates()
            )

            panel = (
                pd.concat(
                    panel_parts,
                    ignore_index=True,
                )
                .drop_duplicates()
                .sort_values(key_cols)
                .reset_index(drop=True)
            )

            if panel.duplicated(
                key_cols
            ).any():
                raise ValueError(
                    f"[{store}] "
                    f"Duplikaty w panelu "
                    f"StoreNo+IDX+DateNo"
                )

            # =============================================================
            # Dołączenie sprzedaży
            # =============================================================

            chunk = panel.merge(
                sales,
                on=key_cols,
                how="left",
                validate="one_to_one",
            )

            chunk["Sales"] = (
                pd.to_numeric(
                    chunk["Sales"],
                    errors="coerce",
                )
                .fillna(0)
            )

            if "SalesValue" in chunk.columns:
                chunk["SalesValue"] = (
                    pd.to_numeric(
                        chunk["SalesValue"],
                        errors="coerce",
                    )
                    .fillna(0)
                )

            # =============================================================
            # IDX metadata
            # =============================================================

            if not idxs.empty:

                chunk = chunk.merge(
                    idxs,
                    on="IDX",
                    how="left",
                    validate="many_to_one",
                )

            # =============================================================
            # Planogram / stock_price_net
            # =============================================================

            if (
                not planogram.empty
                and all(
                    key in planogram.columns
                    for key in key_cols
                )
            ):

                planogram_for_merge = (
                    planogram
                    .drop_duplicates(
                        subset=key_cols
                    )
                )

                chunk = chunk.merge(
                    planogram_for_merge,
                    on=key_cols,
                    how="left",
                    validate="one_to_one",
                )

            # =============================================================
            # OOS
            # =============================================================

            if (
                not oos.empty
                and all(
                    key in oos.columns
                    for key in key_cols
                )
            ):

                chunk = chunk.merge(
                    oos,
                    on=key_cols,
                    how="left",
                )

                chunk["OOS"] = (
                    chunk["OOS"]
                    .fillna(0)
                    .astype(int)
                )

            else:

                chunk["OOS"] = 0

            # =============================================================
            # 4.7 PROMOCJE
            # =============================================================

            chunk["promo"] = 0
            chunk["discount_percent"] = 0.0

            promo_cols = [
                c
                for c in [
                    "IDX",
                    "ID_Promo",
                    "TypeExtention",
                    "ProductFunction",
                    "discount_percent",
                    "MechanismType",
                    "DateStart",
                    "DateEnd",
                    "SellingPrice",
                    "SellingPricePromoEs",
                ]
                if c in promos_ds.schema.names
            ]

            if all(
                c in promos_ds.schema.names
                for c in [
                    "IDX",
                    "DateStart",
                    "DateEnd",
                ]
            ):

                promo_tbl = promos_ds.to_table(
                    columns=promo_cols,
                    filter=(
                        ds.field("IDX").isin(
                            pa.array(
                                list(selected_idxs)
                            )
                        )
                    ),
                )

                promos = promo_tbl.to_pandas(
                    use_threads=True
                )

                if not promos.empty:

                    promos["IDX"] = (
                        promos["IDX"]
                        .astype(str)
                    )

                    promos["DateStart"] = _to_dt(
                        promos["DateStart"]
                    )

                    promos["DateEnd"] = _to_dt(
                        promos["DateEnd"]
                    )

                    if (
                        "discount_percent"
                        in promos.columns
                    ):

                        promos[
                            "discount_percent"
                        ] = (
                            pd.to_numeric(
                                promos[
                                    "discount_percent"
                                ],
                                errors="coerce",
                            )
                            .fillna(0.0)
                        )

                    else:

                        promos[
                            "discount_percent"
                        ] = 0.0

                    # -----------------------------------------------------
                    # DIAGNOSTYKA PROMOCJI PRZED MERGE
                    # -----------------------------------------------------

                    print(
                        f"\n[{store}] "
                        f"=== PROMO DIAGNOSTYKA ==="
                    )

                    print(
                        "Liczba rekordów promocji "
                        "dla wybranych IDX:",
                        len(promos),
                    )

                    print(
                        "Liczba produktów z promocją:",
                        promos["IDX"].nunique(),
                    )

                    print(
                        "Zakres promocji:",
                        promos["DateStart"].min(),
                        "->",
                        promos["DateEnd"].max(),
                    )

                    print(
                        "Braki DateStart:",
                        promos[
                            "DateStart"
                        ].isna().sum(),
                    )

                    print(
                        "Braki DateEnd:",
                        promos[
                            "DateEnd"
                        ].isna().sum(),
                    )

                    # -----------------------------------------------------
                    # Połączenie dni z kandydatami promocji po IDX
                    # -----------------------------------------------------

                    promo_match = (
                        chunk[
                            key_cols
                        ]
                        .merge(
                            promos,
                            on="IDX",
                            how="left",
                        )
                    )

                    active_mask = (
                        promo_match[
                            "DateStart"
                        ].notna()
                        &
                        promo_match[
                            "DateEnd"
                        ].notna()
                        &
                        (
                            promo_match[
                                "DateNo"
                            ]
                            >=
                            promo_match[
                                "DateStart"
                            ]
                        )
                        &
                        (
                            promo_match[
                                "DateNo"
                            ]
                            <=
                            promo_match[
                                "DateEnd"
                            ]
                        )
                    )

                    active_promos = (
                        promo_match
                        .loc[
                            active_mask
                        ]
                        .copy()
                    )

                    # -----------------------------------------------------
                    # DIAGNOSTYKA DOPASOWAŃ
                    # -----------------------------------------------------

                    print(
                        f"[{store}] "
                        f"Dopasowania "
                        f"dzień-produkt-promocja "
                        f"przed deduplikacją: "
                        f"{len(active_promos)}"
                    )

                    if not active_promos.empty:

                        active_keys = (
                            active_promos[
                                key_cols
                            ]
                            .drop_duplicates()
                        )

                        print(
                            f"[{store}] "
                            f"Unikalne dni "
                            f"produkt-sklep "
                            f"z promocją: "
                            f"{len(active_keys)}"
                        )

                        # -------------------------------------------------
                        # Jeśli kilka promocji pokrywa ten sam dzień,
                        # wybieramy najwyższy discount_percent.
                        # -------------------------------------------------

                        active_promos = (
                            active_promos
                            .sort_values(
                                key_cols
                                +
                                [
                                    "discount_percent"
                                ],
                                ascending=[
                                    True,
                                    True,
                                    True,
                                    False,
                                ],
                            )
                        )

                        active_promos = (
                            active_promos
                            .drop_duplicates(
                                subset=key_cols,
                                keep="first",
                            )
                        )

                        active_promos[
                            "promo"
                        ] = 1

                        keep_promo_cols = (
                            key_cols
                            +
                            [
                                "promo",
                                "discount_percent",
                            ]
                        )

                        optional_cols = [
                            c
                            for c in [
                                "ID_Promo",
                                "TypeExtention",
                                "ProductFunction",
                                "MechanismType",
                                "SellingPrice",
                                "SellingPricePromoEs",
                                "DateStart",
                                "DateEnd",
                            ]
                            if c
                            in active_promos.columns
                        ]

                        active_promos = (
                            active_promos[
                                keep_promo_cols
                                +
                                optional_cols
                            ]
                        )

                        chunk = (
                            chunk
                            .drop(
                                columns=[
                                    "promo",
                                    "discount_percent",
                                ],
                                errors="ignore",
                            )
                            .merge(
                                active_promos,
                                on=key_cols,
                                how="left",
                                validate="one_to_one",
                            )
                        )

                        chunk["promo"] = (
                            chunk["promo"]
                            .fillna(0)
                            .astype(int)
                        )

                        chunk[
                            "discount_percent"
                        ] = (
                            pd.to_numeric(
                                chunk[
                                    "discount_percent"
                                ],
                                errors="coerce",
                            )
                            .fillna(0.0)
                        )

            # =============================================================
            # DIAGNOSTYKA PROMOCJI PO MERGE
            # =============================================================

            print(
                f"\n[{store}] "
                f"=== PROMO PO MERGE ==="
            )

            print(
                chunk["promo"]
                .value_counts(
                    dropna=False
                )
                .sort_index()
            )

            promo_zero_sales = (
                (
                    chunk["promo"] == 1
                )
                &
                (
                    chunk["Sales"] == 0
                )
            ).sum()

            promo_positive_sales = (
                (
                    chunk["promo"] == 1
                )
                &
                (
                    chunk["Sales"] > 0
                )
            ).sum()

            print(
                f"[{store}] "
                f"Promo + Sales=0: "
                f"{promo_zero_sales}"
            )

            print(
                f"[{store}] "
                f"Promo + Sales>0: "
                f"{promo_positive_sales}"
            )

            # =============================================================
            # 4.8 CECHY KALENDARZOWE
            # =============================================================

            if "DateNo" in chunk.columns:

                chunk["dow"] = (
                    chunk["DateNo"]
                    .dt.dayofweek
                )

                chunk["month"] = (
                    chunk["DateNo"]
                    .dt.month
                )

            # =============================================================
            # 4.9 ZAPIS
            # =============================================================

            table = pa.Table.from_pandas(
                chunk,
                preserve_index=False,
            )

            if writer is None:

                writer = pq.ParquetWriter(
                    out_path,
                    table.schema,
                    compression="snappy",
                )

            writer.write_table(
                table
            )

            print(
                f"[WRITE] "
                f"store={store} "
                f"rows={len(chunk)}"
            )

    finally:

        if writer is not None:
            writer.close()

    result = str(out_path)

    print(
        f"\nSaved: {result}"
    )

    return result


if __name__ == "__main__":
    build_merged_streaming()
