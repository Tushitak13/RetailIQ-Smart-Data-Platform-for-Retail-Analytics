import pandas as pd
import os

CLEANED_DIR = 'data/cleaned/'
WAREHOUSE_DIR = 'data/warehouse/'

# Load cleaned orders and dimensions
orders = pd.read_parquet(os.path.join(CLEANED_DIR, 'orders_cleaned.parquet'))
dim_customer = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_customer.parquet'))
dim_product = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_product.parquet'))
dim_store = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_store.parquet'))
dim_date = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_date.parquet'))

# -------------------------
# Map keys for joins
# -------------------------
orders = orders.merge(dim_customer, on=['Customer ID', 'Customer Name', 'Segment', 'Region'])
orders = orders.merge(dim_product, on=['Product ID', 'Product Name', 'Category', 'Sub-Category'])
orders = orders.merge(dim_store, on=['State', 'City', 'Region', 'Postal Code'])
orders = orders.merge(dim_date, left_on='Order Date', right_on='Date', how='left')

# -------------------------
# FACT_SALES
# -------------------------
fact_sales = orders[['Customer ID', 'Product ID', 'store_id', 'date_id', 'Sales', 'Quantity', 'Discount', 'Profit']]
fact_sales.to_parquet(os.path.join(WAREHOUSE_DIR, 'fact_sales.parquet'), index=False)
print(f"fact_sales created: {fact_sales.shape[0]} rows")

# -------------------------
# FACT_SHIPMENTS
# -------------------------
orders = orders.merge(dim_date, left_on='Ship Date', right_on='Date', how='left', suffixes=('', '_ship'))
fact_shipments = orders[['Product ID', 'store_id', 'date_id_ship', 'Ship Mode', 'Returned']]
fact_shipments = fact_shipments.rename(columns={'date_id_ship':'date_id'})
fact_shipments.to_parquet(os.path.join(WAREHOUSE_DIR, 'fact_shipments.parquet'), index=False)
print(f"fact_shipments created: {fact_shipments.shape[0]} rows")
