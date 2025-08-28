import pyarrow.parquet as pq


table = pq.ParquetDataset("data/final/MergedDataAfter.parquet")

print(table._dataset.schema)