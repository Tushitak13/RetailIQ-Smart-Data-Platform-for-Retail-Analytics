import pandas as pd
import os

# Paths
CLEANED_DIR = 'data/cleaned/'
WAREHOUSE_DIR = 'data/warehouse/'
os.makedirs(WAREHOUSE_DIR, exist_ok=True)

# Load cleaned orders
orders = pd.read_parquet(os.path.join(CLEANED_DIR, 'orders_cleaned.parquet'))

# -------------------------
# DIM_CUSTOMER
# -------------------------
dim_customer = orders[['Customer ID', 'Customer Name', 'Segment', 'Region']].drop_duplicates()
dim_customer.to_parquet(os.path.join(WAREHOUSE_DIR, 'dim_customer.parquet'), index=False)
print(f"dim_customer created: {dim_customer.shape[0]} rows")

# -------------------------
# DIM_PRODUCT
# -------------------------
dim_product = orders[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates()
dim_product.to_parquet(os.path.join(WAREHOUSE_DIR, 'dim_product.parquet'), index=False)
print(f"dim_product created: {dim_product.shape[0]} rows")

# -------------------------
# DIM_STORE
# -------------------------
dim_store = orders[['State', 'City', 'Region', 'Postal Code']].drop_duplicates()
dim_store = dim_store.reset_index(drop=True)
dim_store['store_id'] = dim_store.index + 1  # generate unique ID
dim_store.to_parquet(os.path.join(WAREHOUSE_DIR, 'dim_store.parquet'), index=False)
print(f"dim_store created: {dim_store.shape[0]} rows")

# -------------------------
# DIM_DATE
# -------------------------
dates = pd.concat([orders['Order Date'], orders['Ship Date']]).dropna().drop_duplicates()
dim_date = pd.DataFrame({'Date': pd.to_datetime(dates)})
dim_date['date_id'] = dim_date.index + 1
dim_date['Year'] = dim_date['Date'].dt.year
dim_date['Quarter'] = dim_date['Date'].dt.quarter
dim_date['Month'] = dim_date['Date'].dt.month
dim_date['Day'] = dim_date['Date'].dt.day
dim_date.to_parquet(os.path.join(WAREHOUSE_DIR, 'dim_date.parquet'), index=False)
print(f"dim_date created: {dim_date.shape[0]} rows")
