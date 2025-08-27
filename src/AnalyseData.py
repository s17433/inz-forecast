import pandas as pd
from dask.distributed import Client, LocalCluster
import matplotlib.pyplot as plt
import seaborn as sns


MergedData_final_path = 'C:/Users/dell/Desktop/Szkoła/PROJ/Lab3/data/final/MergedData.parquet'
MergedData_final_path_after = 'C:/Users/dell/Desktop/Szkoła/PROJ/Lab3/data/final/MergedDataAfter.parquet'


df = pd.read_parquet(MergedData_final_path)
df = df.drop('ID', axis=1)


df['Sales'] = df['Sales'].fillna(0)
df.loc[df['Sales'] < 0, 'Sales'] = 0
df['SalesValue'] = df['SalesValue'].fillna(0)
df.loc[df['SalesValue'] < 0, 'SalesValue'] = 0
df['Ous'] = df['Ous'].fillna(0)
df['discount_percent'] = df['discount_percent'].fillna(0)
df.loc[df['discount_percent'] < 0, 'discount_percent'] = 0


df.to_parquet(MergedData_final_path_after, engine="pyarrow")

pd.set_option('display.max_columns', None)
print(df.head())



print("Podstawowe informacje o danych: ")
print(df.info())

print("\nPierwsze wiersze danych:")
print(df.head())

# 3. Sprawdzenie brakujących wartości
print("\nBrakujące wartości w danych:")
print(df.isnull().sum())

# 4. Opis statystyczny zmiennych numerycznych
print("\nStatystyki opisowe:")
df.describe().to_csv("statystyki.csv", index=True)

# 5. Wizualizacja rozkładu zmiennej docelowej (Sales)
plt.figure(figsize=(8, 5))
sns.histplot(df["Sales"], bins=20, kde=True)
plt.title("Rozkład wartości Sales")
plt.xlabel("Sales")
plt.ylabel("Liczność")
plt.show()


