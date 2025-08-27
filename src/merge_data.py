import dask.dataframe as dd
from dask.distributed import Client, LocalCluster


planogram_source_path = 'C:/Users/dell/Desktop/Szkoła/PROJ/Lab3/data/cleaned/PlanogramChStores.parquet'
IDXs_source_path = 'C:/Users/dell/Desktop/Szkoła/PROJ/Lab3/data/cleaned/IDXs_cleaned.parquet'
SalesChStores_source_path = 'C:/Users/dell/Desktop/Szkoła/PROJ/Lab3/data/cleaned/Sales_cleaned.parquet'
OutOfStock_source_path = 'C:/Users/dell/Desktop/Szkoła/PROJ/Lab3/data/cleaned/OutOfStock.parquet'
Promotions_source_path = 'C:/Users/dell/Desktop/Szkoła/PROJ/Lab3/data/cleaned/Promotions.parquet'
MergedData_final_path = 'C:/Users/dell/Desktop/Szkoła/PROJ/Lab3/data/final/MergedData.parquet'


planogram = dd.read_parquet(planogram_source_path)
planogram = planogram.rename(columns={'source_stock_date': 'DateNo', 'location_id': 'StoreNo', 'product_id': 'IDX'})
filtered_planogram = planogram[planogram["StoreNo"] == "R393"]
planogram = planogram[['DateNo', 'StoreNo', 'IDX', 'stock_price_net']]
planogram = planogram.astype({"StoreNo": "category", "IDX": "float32", "stock_price_net": "float32"})

print("Mam planogram")

idxs = dd.read_parquet(IDXs_source_path)
idxs = idxs[['IDX', 'Brand', 'DIV2']]
idxs = idxs.dropna(subset=['DIV2', 'Brand'])
idxs = idxs.astype({"IDX": "float32"})

print("Mam IDX")

merged_df = planogram.merge(idxs, on=['IDX'], how='left')
print("Marge IDX")





sales = dd.read_parquet(SalesChStores_source_path)
sales = sales[['StoreNo', 'IDX', 'DateNo', 'Sales', 'SalesValue', 'ID']]
sales = sales.astype({"StoreNo": "category", "IDX": "float32", "Sales": "float32", "SalesValue": "float32", "ID": "Int64"})
print("Mam Sales")

merged_df = merged_df.merge(sales, on=['StoreNo', 'IDX', 'DateNo'], how='left')
print("Marge Sales")
merged_df.to_parquet(MergedData_final_path , engine="pyarrow")
"""
merged_df = dd.read_parquet(MergedData_final_path)
"""
merged_df = merged_df.astype({'StoreNo': 'string'})

outofstock = dd.read_parquet(OutOfStock_source_path)
outofstock['Ous'] = 1
outofstock = outofstock[['StoreNo', 'IDX', 'DateNo', 'Ous']]
outofstock=outofstock.astype({"IDX": "float32","Ous": "Int64",})
print("Mam ous")

merged_df = merged_df.merge(outofstock, on=['StoreNo', 'IDX', 'DateNo'], how='left')
print("Marge ous")
print(outofstock['StoreNo'].dtype)
print(merged_df['StoreNo'].dtype)


promotions = dd.read_parquet(Promotions_source_path)
promotions = promotions[['ID', 'TypeExtention', 'ProductFunction', 'discount_percent', 'MechanismType']]
promotions = promotions.astype({"ID": "Int64","TypeExtention": "category", "ProductFunction": "category","discount_percent": "float32", "MechanismType": "category",})
print("Mam prom")

merged_df = merged_df.merge(promotions, on=['ID'], how='left')
print("Marge prom")



merged_df.to_parquet(MergedData_final_path , engine="pyarrow")


