# src/01_ingestion/load_data.py
import pandas as pd
import os
import time

print("=" * 70)
print("DATA INGESTION MODULE")
print("=" * 70)

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 2

# Create directories
os.makedirs('data/staging', exist_ok=True)
os.makedirs('data/cleaned', exist_ok=True)

print("\nStep 1/4: Loading raw CSV files...")

# Encodings to try
encodings_to_try = ['latin-1', 'cp1252', 'ISO-8859-1', 'utf-8']

# Loader with retry + encoding fallback
def load_with_retry(filepath, encodings, max_retries=MAX_RETRIES):
    for enc in encodings:
        for attempt in range(1, max_retries + 1):
            try:
                df = pd.read_csv(filepath, encoding=enc)
                print(f"File loaded: {filepath}")
                print(f"Rows: {len(df):,}, Columns: {len(df.columns)} (encoding={enc}, attempt={attempt})")
                return df
            except Exception:
                print(f"Attempt {attempt} failed with encoding {enc}")
                if attempt < max_retries:
                    time.sleep(RETRY_DELAY)
                else:
                    print("Switching encoding...")
    return None

# Load Supply Chain dataset
df_orders = load_with_retry(
    'data/raw/Copy of Retail-Supply-Chain-Sales-Dataset.csv',
    encodings_to_try
)
if df_orders is None:
    print("Could not load Supply Chain dataset.")
    exit(1)

# Load Retail Sales dataset
df_sales = load_with_retry(
    'data/raw/Retail Sales Dataset.csv',
    encodings_to_try
)
if df_sales is None:
    print("Could not load Retail Sales dataset.")
    exit(1)

print("\nStep 1.5/4: Adding source tracking...")
df_orders['Source_System'] = 'Supply_Chain'
df_sales['Source_System'] = 'Retail_Sales'
print("Source tracking columns added")

print("\nStep 2/4: Standardizing column names...")

def clean_columns(df):
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('-', '_')
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
    return df

df_orders = clean_columns(df_orders)
df_sales = clean_columns(df_sales)
print("Column names standardized")

print("\nStep 2.5/4: Mapping retail columns...")

column_mapping = {
    'Transaction_ID': 'Order_ID',
    'Customer_ID': 'Customer_ID',
    'Product_ID': 'Product_ID',
    'Product_Category': 'Category',
    'Sub_Category': 'Sub_Category',
    'Total_Amount': 'Sales',
    'Date': 'Order_Date',
    'Region': 'Region',
    'Quantity': 'Quantity'
}

rename_dict = {k: v for k, v in column_mapping.items() if k in df_sales.columns}
df_sales = df_sales.rename(columns=rename_dict)
print("Retail columns mapped")

print("\nStep 2.75/4: Adding missing columns to retail dataset...")

supply_columns = set(df_orders.columns)
retail_columns = set(df_sales.columns)
missing_in_retail = supply_columns - retail_columns

for col in missing_in_retail:
    if col in ['Profit', 'Discount', 'Ship_Date', 'Ship_Mode', 'Returned', 'Postal_Code']:
        df_sales[col] = None
    elif col in ['Customer_Name', 'Segment', 'City', 'State']:
        df_sales[col] = 'Unknown'
    elif col == 'Product_Name':
        df_sales[col] = df_sales['Product_ID'] if 'Product_ID' in df_sales.columns else 'Unknown'
    else:
        df_sales[col] = None

print(f"Added {len(missing_in_retail)} missing columns to retail dataset")

print("\nStep 2.8/4: Adding missing columns to supply chain dataset...")

missing_in_supply = retail_columns - supply_columns
for col in missing_in_supply:
    df_orders[col] = None

print(f"Added {len(missing_in_supply)} missing columns to supply chain dataset")

print("\nStep 3/4: Combining datasets...")

common_columns = list(set(df_orders.columns) & set(df_sales.columns))
df_orders = df_orders[common_columns]
df_sales = df_sales[common_columns]

df_combined = pd.concat([df_orders, df_sales], ignore_index=True)
print(f"Combined dataset rows: {len(df_combined):,}")

print("\nStep 3.5/4: Basic data cleaning...")

dupes = df_combined.duplicated().sum()
df_combined = df_combined.drop_duplicates()
print(f"Removed duplicate rows: {dupes:,}")

if 'Order_Date' in df_combined.columns:
    df_combined['Order_Date'] = pd.to_datetime(df_combined['Order_Date'], errors='coerce')
    print("Converted Order_Date")

if 'Ship_Date' in df_combined.columns:
    df_combined['Ship_Date'] = pd.to_datetime(df_combined['Ship_Date'], errors='coerce')
    print("Converted Ship_Date")

critical_cols = ['Customer_ID', 'Product_ID', 'Order_Date']
critical_cols = [c for c in critical_cols if c in df_combined.columns]

before = len(df_combined)
df_combined = df_combined.dropna(subset=critical_cols)
print(f"Removed rows missing critical data: {before - len(df_combined):,}")

numeric_cols = ['Sales', 'Quantity', 'Discount', 'Profit']
for col in numeric_cols:
    if col in df_combined.columns:
        df_combined[col] = pd.to_numeric(df_combined[col], errors='coerce').fillna(0)

print("Numeric columns cleaned")

if 'Quantity' in df_combined.columns:
    df_combined.loc[df_combined['Quantity'] < 1, 'Quantity'] = 1
    print("Quantity values corrected")

# ------------------------------------------------
# Negative value handling (user choice)
# ------------------------------------------------
print("\nStep 3.75/4: Handling negative values...")

def handle_negative_values(df):
    columns_to_check = ['Sales', 'Profit', 'Quantity']
    user_choice = None
    total_removed = 0
    total_fixed = 0

    for col in columns_to_check:
        if col in df.columns:
            negative_count = (df[col] < 0).sum()

            if negative_count > 0:
                print(f"\nNegative values found in {col}: {negative_count}")

                if user_choice is None:
                    print("\nChoose handling method:")
                    print("1. Convert to absolute and add to Profit")
                    print("2. Remove rows with negative values")

                    while True:
                        choice = input("Enter choice (1 or 2): ").strip()
                        if choice in ['1', '2']:
                            user_choice = choice
                            break
                        else:
                            print("Invalid choice.")

                if user_choice == '1':
                    negative_rows = df[df[col] < 0]

                    if 'Profit' in df.columns and col != 'Profit':
                        df.loc[df[col] < 0, 'Profit'] += negative_rows[col].abs()

                    df.loc[df[col] < 0, col] = df.loc[df[col] < 0, col].abs()
                    total_fixed += negative_count
                    print(f"Converted negatives in {col}")

                else:
                    rows_before = len(df)
                    df = df[df[col] >= 0]
                    removed = rows_before - len(df)
                    total_removed += removed
                    print(f"Removed rows with negative {col}: {removed}")

    print("\nNegative value handling summary")
    print(f"Values corrected: {total_fixed}")
    print(f"Rows removed: {total_removed}")

    return df

df_combined = handle_negative_values(df_combined)

# ------------------------------------------------
# Save outputs
# ------------------------------------------------
print("\nStep 4/4: Saving staging files...")

df_combined.to_parquet('data/cleaned/orders_cleaned.parquet', index=False)
print("Saved data/cleaned/orders_cleaned.parquet")

df_orders.to_parquet('data/staging/orders_clean.parquet', index=False)
print("Saved data/staging/orders_clean.parquet")

df_sales.to_parquet('data/staging/sales_clean.parquet', index=False)
print("Saved data/staging/sales_clean.parquet")

# ------------------------------------------------
# Data quality report
# ------------------------------------------------
print("\n" + "=" * 70)
print("DATA QUALITY REPORT")
print("=" * 70)

print("\nIntegrated Dataset")
print(f"Total Rows: {len(df_combined):,}")
print(f"Total Columns: {len(df_combined.columns)}")

print("\nSource Breakdown")
source_counts = df_combined['Source_System'].value_counts()
for source, count in source_counts.items():
    print(f"{source}: {count:,} ({count/len(df_combined)*100:.1f}%)")

if 'Customer_ID' in df_combined.columns:
    print(f"\nUnique Customers: {df_combined['Customer_ID'].nunique():,}")

if 'Product_ID' in df_combined.columns:
    print(f"Unique Products: {df_combined['Product_ID'].nunique():,}")

if 'Sales' in df_combined.columns:
    print(f"\nTotal Sales: ${df_combined['Sales'].sum():,.2f}")

if 'Profit' in df_combined.columns:
    print(f"Total Profit: ${df_combined['Profit'].sum():,.2f}")

if 'Sales' in df_combined.columns and 'Profit' in df_combined.columns:
    total_sales = df_combined['Sales'].sum()
    margin = (df_combined['Profit'].sum() / total_sales * 100) if total_sales > 0 else 0
    print(f"Profit Margin: {margin:.1f}%")

print("\n" + "=" * 70)
print("DATA INGESTION COMPLETE")
print("=" * 70)

print("\nOutput files ready:")
print("data/cleaned/orders_cleaned.parquet")
print("data/staging/orders_clean.parquet")
print("data/staging/sales_clean.parquet")

print("\nNext step: python src/03_transformation/build_dimensions.py")