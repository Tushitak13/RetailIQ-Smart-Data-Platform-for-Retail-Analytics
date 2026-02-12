import pandas as pd
import os

print("=" * 60)
print("DATA INGESTION MODULE")
print("=" * 60)

# Create staging directory
os.makedirs('data/staging', exist_ok=True)

# Load raw data files
print("\nStep 1/4: Loading raw CSV files...")

try:
    df_sales = pd.read_csv('data/raw/Retail Sales Dataset.csv')
    print(f"File 1 loaded: {len(df_sales):,} rows, {len(df_sales.columns)} columns")
except Exception as e:
    print(f"Error loading File 1: {e}")
    exit()

try:
    df_orders = pd.read_csv('data/raw/Copy of Retail-Supply-Chain-Sales-Dataset.csv', encoding='utf-8')
    print(f"File 2 loaded: {len(df_orders):,} rows, {len(df_orders.columns)} columns")
except Exception as e:
    print(f"Error with utf-8 encoding: {e}")
    print("Trying latin-1 encoding...")
    try:
        df_orders = pd.read_csv('data/raw/Copy of Retail-Supply-Chain-Sales-Dataset.csv', encoding='latin-1')
        print(f"File 2 loaded: {len(df_orders):,} rows, {len(df_orders.columns)} columns")
    except Exception as e2:
        print(f"Error with latin-1: {e2}")
        print("Trying cp1252 encoding...")
        try:
            df_orders = pd.read_csv('data/raw/Copy of Retail-Supply-Chain-Sales-Dataset.csv', encoding='cp1252')
            print(f"File 2 loaded: {len(df_orders):,} rows, {len(df_orders.columns)} columns")
        except Exception as e3:
            print(f"Error with cp1252: {e3}")
            exit()

# Data cleaning
print("\nStep 2/4: Cleaning data...")

if 'Order Date' in df_orders.columns:
    df_orders['Order Date'] = pd.to_datetime(df_orders['Order Date'], errors='coerce')
    print("Order Date converted to datetime")

if 'Ship Date' in df_orders.columns:
    df_orders['Ship Date'] = pd.to_datetime(df_orders['Ship Date'], errors='coerce')
    print("Ship Date converted to datetime")

df_orders = df_orders.dropna(how='all')
df_sales = df_sales.dropna(how='all')
print(f"Data cleaned: {len(df_orders):,} orders, {len(df_sales):,} sales records retained")

# Save staging files
print("\nStep 3/4: Saving staging files...")

df_orders.to_parquet('data/staging/orders_clean.parquet', index=False)
print("Saved: data/staging/orders_clean.parquet")

df_sales.to_parquet('data/staging/sales_clean.parquet', index=False)
print("Saved: data/staging/sales_clean.parquet")

# Generate data quality report
print("\nStep 4/4: Generating data quality report...")
print("\n" + "=" * 60)
print("DATA QUALITY REPORT")
print("=" * 60)

print(f"\nOrders Dataset: {len(df_orders):,} rows, {len(df_orders.columns)} columns")
print(f"Sales Dataset: {len(df_sales):,} rows, {len(df_sales.columns)} columns")

if 'Sales' in df_orders.columns:
    print(f"Total Revenue: ${df_orders['Sales'].sum():,.2f}")
if 'Profit' in df_orders.columns:
    print(f"Total Profit: ${df_orders['Profit'].sum():,.2f}")
if 'Customer ID' in df_orders.columns:
    print(f"Unique Customers: {df_orders['Customer ID'].nunique():,}")
if 'Product ID' in df_orders.columns:
    print(f"Unique Products: {df_orders['Product ID'].nunique():,}")

print("\n" + "=" * 60)
print("DATA INGESTION COMPLETE")
print("=" * 60)
print("\nOutput files ready in data/staging/")
print("Next step: Build dimensional model")