import pandas as pd
import os

# ----------------------------
# Paths
# ----------------------------
STAGING_DIR = 'data/staging/'
OUTPUT_DIR = 'data/cleaned/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

sales_file = os.path.join(STAGING_DIR, 'sales_clean.parquet')

# ----------------------------
# Load Sales Data
# ----------------------------
sales = pd.read_parquet(sales_file)
print(f"Original Sales: {sales.shape[0]} rows, {sales.shape[1]} columns")

# ----------------------------
# Handle nulls
# ----------------------------
critical_columns = ['Customer ID', 'Product Category', 'Total Amount']
sales = sales.dropna(subset=critical_columns)
print(f"After dropping critical nulls: {sales.shape[0]} rows")

# Fill optional numeric columns
numeric_cols = ['Quantity', 'Price per Unit', 'Total Price']
for col in numeric_cols:
    if col in sales.columns:
        sales[col] = sales[col].fillna(0)

# ----------------------------
# Handle duplicates
# ----------------------------
sales = sales.drop_duplicates()
print(f"After dropping duplicates: {sales.shape[0]} rows")

# ----------------------------
# Standardize dates (if any)
# ----------------------------
# Some sales datasets may have a 'Date' column
if 'Date' in sales.columns:
    sales['Date'] = pd.to_datetime(sales['Date'], errors='coerce')
    sales = sales.dropna(subset=['Date'])
    print(f"After date parsing: {sales.shape[0]} rows")

# ----------------------------
# Save cleaned sales
# ----------------------------
sales_clean_file = os.path.join(OUTPUT_DIR, 'sales_cleaned.parquet')
sales.to_parquet(sales_clean_file, index=False)
print(f"Cleaned Sales saved: {sales_clean_file}")
