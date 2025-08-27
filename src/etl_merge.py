# src/etl_merge.py
"""
ETL pamięciooszczędny (bez Dask): czyta FINAL/* partiami (po sklepach) przez pyarrow.dataset,
łączy w pandas tylko wycinki i dopisuje wynik do jednego pliku Parquet.

Konfiguracja ograniczeń:
- TOP_N_STORES: ile sklepów wziąć (None = wszystkie)
- TOP_N_IDXS:   ile top IDX na sklep (None = wszystkie)

Uruchomienie:
    python -m src.etl_merge
"""

from __future__ import annotations
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from typing import Iterable, Optional
from config import FINAL
from pathlib import Path

# --- USTAWIENIA ----------------------------------------------------------
TOP_N_STORES: int | None = 2          # None = wszystkie; np. 2 dla lekkiej próbki
TOP_N_IDXS:   int | None = 200        # None = wszystkie; np. 200 top IDX w danym sklepie
OUTPUT_NAME = "MergedData.parquet"    # wynik w data/final/
# ------------------------------------------------------------------------


def _to_dt(s: pd.Series) -> pd.Series:
    """Bezpieczna konwersja do datetime (obsługa int/str YYYYMMDD)."""
    return s if pd.api.types.is_datetime64_any_dtype(s) else pd.to_datetime(s.astype(str), errors="coerce")


def _dataset(path: Path, format_hint: str = "parquet") -> ds.Dataset:
    """Zwróć PyArrow Dataset (obsługuje partycje/row-groups)."""
    return ds.dataset(str(path), format=format_hint)


def _distinct_values(ds_obj: ds.Dataset, column: str, limit: int | None = None) -> list[str]:
    """
    Strumieniowo (batchami) zbiera wartości kolumny posortowane wg częstości (TOP-N).
    Brak 'copy=' w to_pandas() (działa na starszych wersjach pyarrow).
    """
    scanner = ds.Scanner.from_dataset(ds_obj, columns=[column])
    counts: dict[str, int] = {}
    for batch in scanner.to_batches():
        ser = batch.to_pandas()[column].dropna().astype(str)
        for v in ser:
            counts[v] = counts.get(v, 0) + 1
    ordered = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    values = [k for k, _ in ordered]
    return values[:limit] if limit else values


def build_merged_streaming() -> str:
    # --- 1) Duże zbiory jako Dataset (czytane partiami) ---
    sales_ds  = _dataset(FINAL / "Sales_cleaned.parquet")
    plano_ds  = _dataset(FINAL / "PlanogramChStores.parquet")
    oos_ds    = _dataset(FINAL / "OutOfStock.parquet")
    promos_ds = _dataset(FINAL / "Promotions.parquet")

    # --- 2) Małe zbiory do RAM ---
    idxs = pd.read_parquet(FINAL / "IDXs_cleaned.parquet")[["IDX", "Brand", "DIV2"]].dropna(subset=["Brand", "DIV2"])
    chosen = pd.read_parquet(FINAL / "ChosenIDXs.parquet")[["IDX"]].drop_duplicates()

    # normalizacje lekkich
    idxs["IDX"] = idxs["IDX"].astype(str)
    chosen["IDX"] = chosen["IDX"].astype(str)

    # --- 3) Zakres: sklepy do przetworzenia (TOP-N po częstości w Sales) ---
    stores_all = _distinct_values(sales_ds, "StoreNo", limit=TOP_N_STORES)
    print(f"[INFO] Stores to process: {len(stores_all)} -> {stores_all[:10]}{' ...' if len(stores_all)>10 else ''}")

    # --- 4) Wyjściowy writer Parquet (inkrementalny zapis) ---
    out_path = FINAL / OUTPUT_NAME
    if out_path.exists():
        out_path.unlink()
    writer: Optional[pq.ParquetWriter] = None

    try:
        for store in stores_all:
            # ====== PĘTLA PO SKLEPACH ======
            # 4.1) Sales dla danego sklepu (wąskie kolumny + filter pushdown)
            sales_cols = [c for c in ["StoreNo", "IDX", "DateNo", "Sales", "SalesValue", "Price", "ID"]
                          if c in sales_ds.schema.names]
            sales_tbl = sales_ds.to_table(columns=sales_cols,
                                          filter=(ds.field("StoreNo") == store))
            if sales_tbl.num_rows == 0:
                continue

            sales = sales_tbl.to_pandas(use_threads=True)
            # typy kluczy
            if "StoreNo" in sales.columns: sales["StoreNo"] = sales["StoreNo"].astype(str)
            if "IDX" in sales.columns:     sales["IDX"]     = sales["IDX"].astype(str)
            if "DateNo" in sales.columns:  sales["DateNo"]  = _to_dt(sales["DateNo"])

            # 4.2) Filtr do ChosenIDXs
            before = len(sales)
            sales = sales.merge(chosen, on="IDX", how="inner")
            print(f"[{store}] sales chosen: {before} -> {len(sales)}")
            if sales.empty:
                continue

            # 4.3) TOP_N_IDXS w ramach sklepu (opcjonalnie)
            if TOP_N_IDXS is not None and "IDX" in sales.columns:
                top_idx = sales["IDX"].value_counts().head(TOP_N_IDXS).index
                sales = sales[sales["IDX"].isin(top_idx)]
                print(f"[{store}] sales limit idx: -> {len(sales)} rows")

            # 4.4) Planogram dla sklepu
            plano_cols = [c for c in ["source_stock_date", "location_id", "product_id", "stock_price_net"]
                          if c in plano_ds.schema.names]
            plano_tbl = plano_ds.to_table(columns=plano_cols,
                                          filter=(ds.field("location_id") == store)
                                          if "location_id" in plano_ds.schema.names else None)
            planogram = plano_tbl.to_pandas(use_threads=True)
            if not planogram.empty:
                planogram = planogram.rename(columns={
                    "source_stock_date": "DateNo",
                    "location_id": "StoreNo",
                    "product_id": "IDX",
                })
                planogram["StoreNo"] = planogram["StoreNo"].astype(str)
                planogram["IDX"]     = planogram["IDX"].astype(str)
                planogram["DateNo"]  = _to_dt(planogram["DateNo"])
                keep = ["DateNo", "StoreNo", "IDX"] + (["stock_price_net"] if "stock_price_net" in planogram.columns else [])
                planogram = planogram[keep]

            # 4.5) OOS dla sklepu
            oos_cols = [c for c in ["StoreNo", "IDX", "DateNo", "Ous", "OOS", "dateno"]
                        if c in oos_ds.schema.names]
            oos_tbl = oos_ds.to_table(columns=oos_cols,
                                      filter=(ds.field("StoreNo") == store)
                                      if "StoreNo" in oos_ds.schema.names else None)
            oos = oos_tbl.to_pandas(use_threads=True)
            if not oos.empty:
                if "dateno" in oos.columns: oos = oos.rename(columns={"dateno": "DateNo"})
                if "Ous"   in oos.columns:  oos = oos.rename(columns={"Ous": "OOS"})
                oos["StoreNo"] = oos["StoreNo"].astype(str)
                oos["IDX"]     = oos["IDX"].astype(str)
                oos["DateNo"]  = _to_dt(oos["DateNo"])
                oos = oos[["StoreNo", "IDX", "DateNo"]].copy()
                oos["OOS"] = 1

            # 4.6) MERGE (baza = sales sklepu)
            chunk = sales.copy()

            # + idxs (małe)
            if not idxs.empty:
                chunk = chunk.merge(idxs, on="IDX", how="left")

            # + planogram
            if not planogram.empty and all(k in planogram.columns for k in ["StoreNo", "IDX", "DateNo"]):
                chunk = chunk.merge(planogram, on=["StoreNo", "IDX", "DateNo"], how="left")

            # + oos
            if not oos.empty and all(k in oos.columns for k in ["StoreNo", "IDX", "DateNo"]):
                chunk = chunk.merge(oos, on=["StoreNo", "IDX", "DateNo"], how="left")
                chunk["OOS"] = chunk["OOS"].fillna(0).astype(int)
            else:
                chunk["OOS"] = 0

            # 4.7) Promocje → flaga i merge po ID (tylko użyte ID)
            if "ID" in chunk.columns and "ID" in promos_ds.schema.names:
                chunk["promo"] = chunk["ID"].notna().astype(int)
                used_ids = pd.Series(chunk["ID"].dropna().unique())
                if not used_ids.empty:
                    promos_cols = [c for c in ["ID", "ID_Promo", "TypeExtention", "ProductFunction",
                                               "discount_percent", "MechanismType", "DateStart", "DateEnd"]
                                   if c in promos_ds.schema.names]
                    promos_tbl = promos_ds.to_table(columns=promos_cols,
                                                    filter=(ds.field("ID").isin(pa.array(used_ids))))
                    promos = promos_tbl.to_pandas(use_threads=True)
                    if not promos.empty:
                        for col in ("DateStart", "DateEnd"):
                            if col in promos.columns:
                                promos[col] = _to_dt(promos[col])
                        chunk = chunk.merge(promos, on="ID", how="left")
            else:
                chunk["promo"] = 0

            # 4.8) Cechy kalendarzowe
            if "DateNo" in chunk.columns:
                chunk["dow"]   = chunk["DateNo"].dt.dayofweek
                chunk["month"] = chunk["DateNo"].dt.month

            # 4.9) Zapisz partię do Parquet (inkrementalnie)
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")
            writer.write_table(table)

            print(f"[WRITE] store={store} rows={len(chunk)}")

    finally:
        if writer is not None:
            writer.close()

    result = str(out_path)
    print(f"Saved: {result}")
    return result


if __name__ == "__main__":
    build_merged_streaming()
