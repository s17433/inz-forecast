import dask.dataframe as dd
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import numpy as np
import logging

logging.basicConfig(filename='../logs/data_standardization.log', level=logging.INFO)

# Wczytaj dane
df = dd.read_parquet('data/processed/Sales_cleaned.parquet')


# Skalowanie wewnątrz okien czasowych
def scale_within_windows(df, columns, window='30D', scaler_type='minmax'):
    try:
        df['DateNo'] = dd.to_datetime(df['DateNo'])
        # Grupowanie po oknach czasowych
        grouped = df.groupby(['IDX', df['DateNo'].dt.to_period(window)])

        # Skalery dla każdego okna czasowego
        def scale_partition(partition):
            scaler = MinMaxScaler() if scaler_type == 'minmax' else StandardScaler()
            partition[columns] = scaler.fit_transform(partition[columns])
            return partition

        df_scaled = grouped.apply(lambda x: scale_partition(x), meta=df)
        logging.info(f"Skalowanie zakończone dla okien czasowych: {window}")
        return df_scaled
    except Exception as e:
        logging.error(f"Błąd podczas skalowania danych: {e}")
        raise


# Zastosowanie skalowania
numeric_cols = ['Sales', 'Price']
df = scale_within_windows(df, numeric_cols, window='30D', scaler_type='minmax')

# Zapis danych po standaryzacji
df.to_parquet('data/processed/Sales_standardized.parquet', engine='pyarrow', compression='snappy')
logging.info("Dane zapisane po standaryzacji.")
