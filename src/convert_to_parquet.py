import dask.dataframe as dd
import logging

from config import RAW, PROCESSED, LOGS

#Logging some information
logging.basicConfig(filename = LOGS/"conversion.log", level=logging.INFO)

def convert_to_parquet(input_path, output_path, sep=',', header=True, columns=None, dtypes=None, decimal='.'):
    try:
        if header:
            df = dd.read_csv(
                input_path,
                sep=sep,
                assume_missing=True,
                dtype=dtypes,
                decimal=decimal,
            )
        else:
            df = dd.read_csv(
                input_path,
                sep=sep,
                header=None,
                names=columns,
                assume_missing=True,
                dtype=dtypes
            )

        df.to_parquet(output_path, engine='pyarrow', compression='snappy')
        logging.info(f"End of conversion: {input_path} -> {output_path}")
    except Exception as e:
        logging.error(f"File conversion error {input_path}: {e}")
        raise


#File conversion
if __name__ == "__main__":
        convert_to_parquet(
                RAW/"ChosenIDXs.csv",
                PROCESSED/"ChosenIDXs.parquet",
                sep=';',
                header=False,
                columns=['IDX', 'Sales']
        )

        convert_to_parquet(
                RAW/"ChosenStores.csv",
                PROCESSED/"ChosenStores.parquet",
                sep=';',
                header=True
        )

        convert_to_parquet(
                RAW/"IDXs.csv",
                PROCESSED/"IDXs.parquet",
                sep=';',
                header=False,
                columns=['IDX', 'Brand', 'DIV2', 'DIV3', 'DIV4'],
                dtypes={'Brand': 'object'},
        )

        convert_to_parquet(
                RAW/"OutOfStock.csv",
                PROCESSED/"OutOfStock.parquet",
                sep=';',
                header=False,
                columns=['StoreNo', 'IDX', 'DateNo']
        )

        convert_to_parquet(
                RAW/"PlanogramChStores_fixed_sep.txt",
                PROCESSED/"PlanogramChStores.parquet",
                sep=';',
                header=True,
                decimal = ','
        )

        convert_to_parquet(
                RAW/"Promotions.csv",
                PROCESSED/"Promotions.parquet",
                sep=';',
                header=True,
                dtypes={'ProductFunction': 'object'}
        )

        convert_to_parquet(
                RAW/"SalesChStores.csv",
                PROCESSED/"SalesChStores.parquet",
                sep=';',
                header=False,
                columns=['DateNo', 'StoreNo', 'IDX', 'Sales', 'SalesValue', 'ID']
        )