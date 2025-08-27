from os import write

import dask.dataframe as dd
import logging

from config import PROCESSED, LOGS, FINAL

# Konfiguracja logowania
logging.basicConfig(filename = LOGS/"conversion.log", level=logging.INFO)

def write_file(source_file_path):
    # Wczytaj dane z poprawnym separatorem
    try:
        df = dd.read_parquet(source_file_path)
        logging.info(f"Poprawnie wczytano dane z pliku {source_file_path}")
        print(f"Poprawnie wczytano dane z pliku {source_file_path}")
        return df
    except Exception as e:
        logging.error(f"Błąd przy wczytywaniu danych: {e}")
        print(f"Błąd przy wczytywaniu danych: {e}")
        raise


# 1. Usunięcie brakujących wartości tylko w wybranych kolumnach
def drop_missing_values(df, required_columns):
    try:
        # Zapisz wiersze z brakami w istotnych kolumnach do osobnego DataFrame
        missing_rows = df[df[required_columns].isnull().any(axis=1)]

        # Zapisz wiersze z brakami do pliku
        sample = missing_rows.head(1000)
        sample.to_csv(PROCESSED/"missing_rows_sample.csv", index=False)
        logging.info("Zapisano wiersze z brakami w istotnych kolumnach do pliku.")

        # Usuń wiersze z brakami w istotnych kolumnach
        df = df.dropna(subset=required_columns)
        logging.info("Usunięto wiersze z brakami w istotnych kolumnach.")
        return df
    except Exception as e:
        logging.error(f"Błąd przy usuwaniu braków danych: {e}")
        raise


# 2. Filtrowanie danych dla wybranych IDX
def filter_chosen_idxs(df):
    try:
        # Wczytaj wybrane IDX z pliku
        chosen_idxs = dd.read_parquet(PROCESSED/"ChosenIDXs.parquet", header=None, names=['IDX', 'Sales'])
        if 'IDX' in df.columns:
            df = df[df['IDX'].isin(chosen_idxs['IDX'])]
        elif 'product_id' in df.columns:
            print('będę filtrował')
            df = df[df['product_id'].isin(chosen_idxs['IDX'])]
        else:
            print("Nie istnieje ani kolmna IDX ani product_id")
        logging.info("Przefiltrowano dane na podstawie wybranych IDX.")
        return df
    except Exception as e:
        logging.error(f"Błąd przy filtrowaniu IDX: {e}")
        raise

def filter_promotion(df):
    try:
        promotion = dd.read_parquet(PROCESSED/"SalesChStores.parquet",
                                      header=None, names=['ID'])

        promotion = promotion[promotion['ID'].notnull()]

        promotion = promotion.compute()

        df = df[df['ID'].isin(promotion['ID'])]

        return df
    except Exception as e:
        logging.error(f"Błąd przy filtrowaniu promocji: {e}")
        raise

def cleaning_file(source_file_path, required_columns, final_file_path):

    df = write_file(source_file_path)
    df = drop_missing_values(df, required_columns)
    df = filter_chosen_idxs(df)

    if source_file_path == PROCESSED/"Promotions.parquet":
        df = filter_promotion(df)

    try:
        df.to_parquet(final_file_path, engine='pyarrow', compression='snappy')
        logging.info("Zapisano dane po czyszczeniu do pliku.")
    except Exception as e:
        logging.error(f"Błąd przy zapisie danych do pliku: {e}")
        raise

    try:
        preview = df.head(20)
        print(preview)
    except Exception as e:
        logging.error(f"Błąd przy wyświetlaniu całego pliku: {e}")
        raise

required_columns_SalesChStores = ['DateNo', 'StoreNo', 'IDX', 'Sales']
SalesChStores_source_path = PROCESSED/"SalesChStores.parquet"
SalesChStores_final_path = FINAL/"Sales_cleaned.parquet"

required_columns_IDXs = ['IDX', 'Brand', 'DIV2', 'DIV3', 'DIV4']
IDXs_source_path = PROCESSED/"IDXs.parquet"
IDXs_final_path = FINAL/"IDXs_cleaned.parquet"

required_columns_OutOfStock = ['StoreNo', 'IDX', 'DateNo']
OutOfStock_source_path = PROCESSED/"OutOfStock.parquet"
OutOfStock_final_path = FINAL/"OutOfStock.parquet"

required_columns_PlanogramChStores = ['source_stock_date','location_id','product_id','stock_price_net']
PlanogramChStores_source_path = PROCESSED/"PlanogramChStores.parquet"
PlanogramChStores_final_path = FINAL/"PlanogramChStores.parquet"

required_columns_Promotions = ['ID', 'ID_Promo', 'TypeExtention', 'IDX', 'SellingPrice', 'SellingPricePromoEs', 'discount_percent', 'MechanismType', 'MechanismSubtype', 'DateStart', 'DateEnd']
Promotions_source_path = PROCESSED/"Promotions.parquet"
Promotions_final_path = FINAL/"Promotions.parquet"

if __name__ == "__main__":
    cleaning_file(SalesChStores_source_path, required_columns_SalesChStores, SalesChStores_final_path)
    cleaning_file(IDXs_source_path, required_columns_IDXs, IDXs_final_path)
    cleaning_file(OutOfStock_source_path, required_columns_OutOfStock, OutOfStock_final_path)
    cleaning_file(PlanogramChStores_source_path, required_columns_PlanogramChStores, PlanogramChStores_final_path)
    cleaning_file(Promotions_source_path, required_columns_Promotions, Promotions_final_path)
    cleaning_file(Promotions_source_path, required_columns_Promotions, Promotions_final_path)