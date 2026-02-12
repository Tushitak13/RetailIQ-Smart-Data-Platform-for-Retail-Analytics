import pandas as pd
import os

# ----------------------------
# Paths
# ----------------------------
STAGING_DIR = 'data/staging/'
OUTPUT_DIR = 'data/cleaned/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

orders_file = os.path.join(STAGING_DIR, 'orders_clean.parquet')

# ----------------------------
# Load Orders Data
# ----------------------------
orders = pd.read_parquet(orders_file)
print(f"Original Orders: {orders.shape[0]} rows, {orders.shape[1]} columns")

# ----------------------------
# Handle nulls
# ----------------------------
# Drop rows where critical IDs are missing
critical_columns = ['Customer ID', 'Product ID', 'Order Date', 'Ship Date']
orders = orders.dropna(subset=critical_columns)
print(f"After dropping critical nulls: {orders.shape[0]} rows")

# Fill optional columns with defaults if needed
optional_columns = ['Discount', 'Profit', 'Sales', 'Quantity']
for col in optional_columns:
    if col in orders.columns:
        orders[col] = orders[col].fillna(0)

# ----------------------------
# Handle duplicates
# ----------------------------
orders = orders.drop_duplicates()
print(f"After dropping duplicates: {orders.shape[0]} rows")

# ----------------------------
# Standardize dates
# ----------------------------
orders['Order Date'] = pd.to_datetime(orders['Order Date'], errors='coerce')
orders['Ship Date'] = pd.to_datetime(orders['Ship Date'], errors='coerce')

# Remove rows where dates could not be parsed
orders = orders.dropna(subset=['Order Date', 'Ship Date'])
print(f"After date parsing: {orders.shape[0]} rows")

# ----------------------------
# Save cleaned orders
# ----------------------------
orders_clean_file = os.path.join(OUTPUT_DIR, 'orders_cleaned.parquet')
orders.to_parquet(orders_clean_file, index=False)
print(f"Cleaned Orders saved: {orders_clean_file}")
