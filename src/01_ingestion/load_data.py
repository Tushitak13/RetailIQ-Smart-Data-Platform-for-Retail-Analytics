import pandas as pd
import os
import numpy as np

print("=" * 70)
print("DATA INGESTION MODULE")
print("=" * 70)

os.makedirs('data/staging', exist_ok=True)
os.makedirs('data/cleaned', exist_ok=True)

print("\nStep 1/4: Loading raw CSV files...")

encodings_to_try = ['latin-1', 'cp1252', 'ISO-8859-1', 'utf-8']

def load_csv_with_retry(filepath, encodings):
    for i, enc in enumerate(encodings, 1):
        try:
            df = pd.read_csv(filepath, encoding=enc)
            print(f"File loaded successfully: {filepath}")
            print(f"Rows: {len(df):,}, Columns: {len(df.columns)}, Encoding: {enc}, Attempt: {i}")
            return df
        except UnicodeDecodeError:
            print(f"Attempt {i} with {enc}: Encoding error, trying next...")
            continue
        except FileNotFoundError:
            print(f"ERROR: File not found: {filepath}")
            return None
        except Exception as e:
            print(f"Attempt {i} with {enc}: {type(e).__name__} - {str(e)[:50]}")
            continue
    
    print(f"CRITICAL ERROR: Could not load file with any encoding: {filepath}")
    return None

df_orders = load_csv_with_retry('data/raw/Copy of Retail-Supply-Chain-Sales-Dataset.csv', encodings_to_try)
if df_orders is None:
    print("Terminating: Supply Chain dataset could not be loaded")
    exit(1)

df_sales = load_csv_with_retry('data/raw/Retail Sales Dataset.csv', encodings_to_try)
if df_sales is None:
    print("Terminating: Retail Sales dataset could not be loaded")
    exit(1)

print("\nStep 1.5/4: Adding source tracking...")
df_orders['Source_System'] = 'Supply_Chain'
df_sales['Source_System'] = 'Retail_Sales'

print(f"Supply Chain source: {df_orders['Source_System'].iloc[0]}")
print(f"Retail source: {df_sales['Source_System'].iloc[0]}")
print(f"Supply Chain rows: {len(df_orders):,}")
print(f"Retail rows: {len(df_sales):,}")

print("\nStep 1.75/4: Ensuring retail dataset has required IDs...")

if 'Customer_ID' not in df_sales.columns:
    df_sales['Customer_ID'] = ['RETAIL-CUST-' + str(i).zfill(6) for i in range(1, len(df_sales) + 1)]
    print(f"Created Customer_ID for {len(df_sales):,} retail records")
else:
    null_customers = df_sales['Customer_ID'].isnull().sum()
    if null_customers > 0:
        df_sales.loc[df_sales['Customer_ID'].isnull(), 'Customer_ID'] = \
            ['RETAIL-CUST-' + str(i).zfill(6) for i in range(1, null_customers + 1)]
        print(f"Filled {null_customers} null Customer_ID values")

if 'Product_ID' not in df_sales.columns:
    df_sales['Product_ID'] = ['RETAIL-PROD-' + str(i).zfill(6) for i in range(1, len(df_sales) + 1)]
    print(f"Created Product_ID for {len(df_sales):,} retail records")
else:
    null_products = df_sales['Product_ID'].isnull().sum()
    if null_products > 0:
        df_sales.loc[df_sales['Product_ID'].isnull(), 'Product_ID'] = \
            ['RETAIL-PROD-' + str(i).zfill(6) for i in range(1, null_products + 1)]
        print(f"Filled {null_products} null Product_ID values")

if 'Order_Date' not in df_sales.columns:
    if 'Date' in df_sales.columns:
        df_sales['Order_Date'] = df_sales['Date']
        print("Mapped Date column to Order_Date")
    else:
        df_sales['Order_Date'] = pd.Timestamp.now()
        print("WARNING: Created default Order_Date for retail records")

if 'Customer_Name' not in df_sales.columns:
    df_sales['Customer_Name'] = 'Retail Customer'
    print("Created default Customer_Name for retail records")

if 'Product_Name' not in df_sales.columns:
    df_sales['Product_Name'] = df_sales['Product_ID']
    print("Created Product_Name from Product_ID")

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

print("\nStep 2.5/4: Mapping retail columns to match supply chain...")

column_mapping = {
    'Transaction_ID': 'Order_ID',
    'Invoice_No': 'Order_ID',
    'Customer_ID': 'Customer_ID',
    'Customer_Id': 'Customer_ID',
    'Product_ID': 'Product_ID',
    'Product_Id': 'Product_ID',
    'Product_Category': 'Category',
    'Sub_Category': 'Sub_Category',
    'Total_Amount': 'Sales',
    'Amount': 'Sales',
    'Revenue': 'Sales',
    'Date': 'Order_Date',
    'Transaction_Date': 'Order_Date',
    'Region': 'Region',
    'Quantity': 'Quantity',
    'Qty': 'Quantity',
    'Customer_Name': 'Customer_Name',
    'CustomerName': 'Customer_Name',
    'Product_Name': 'Product_Name',
    'ProductName': 'Product_Name',
    'Ship_Date': 'Ship_Date',
    'ShipDate': 'Ship_Date',
    'Ship_Mode': 'Ship_Mode',
    'ShipMode': 'Ship_Mode',
    'Postal_Code': 'Postal_Code',
    'PostalCode': 'Postal_Code',
    'Discount': 'Discount',
    'Profit': 'Profit',
    'Returned': 'Returned',
    'State': 'State',
    'City': 'City',
    'Segment': 'Segment'
}

rename_dict = {k: v for k, v in column_mapping.items() if k in df_sales.columns}
if rename_dict:
    df_sales = df_sales.rename(columns=rename_dict)
    print(f"Mapped {len(rename_dict)} columns: {list(rename_dict.values())}")
else:
    print("No columns needed mapping")

print("\nStep 2.6/4: Removing duplicate columns...")

if df_orders.columns.duplicated().any():
    duplicates = df_orders.columns[df_orders.columns.duplicated()].unique()
    df_orders = df_orders.loc[:, ~df_orders.columns.duplicated()]
    print(f"Removed duplicate columns from Supply Chain: {list(duplicates)}")

if df_sales.columns.duplicated().any():
    duplicates = df_sales.columns[df_sales.columns.duplicated()].unique()
    df_sales = df_sales.loc[:, ~df_sales.columns.duplicated()]
    print(f"Removed duplicate columns from Retail: {list(duplicates)}")

print("\nStep 2.75/4: Aligning columns across datasets...")

all_columns = set(df_orders.columns) | set(df_sales.columns)
all_columns = sorted(list(all_columns))

missing_in_retail = set(all_columns) - set(df_sales.columns)
for col in missing_in_retail:
    if col in ['Profit', 'Discount', 'Ship_Date', 'Postal_Code']:
        df_sales[col] = 0
    elif col in ['Ship_Mode', 'Returned']:
        df_sales[col] = 'Standard'
    elif col in ['Customer_Name', 'Segment', 'City', 'State']:
        df_sales[col] = 'Unknown'
    elif col == 'Product_Name':
        df_sales[col] = df_sales['Product_ID'] if 'Product_ID' in df_sales.columns else 'Unknown'
    elif col == 'Order_ID':
        df_sales[col] = ['RETAIL-ORD-' + str(i).zfill(7) for i in range(1, len(df_sales) + 1)]
    elif col == 'Sub_Category':
        df_sales[col] = 'General'
    else:
        df_sales[col] = None

print(f"Added {len(missing_in_retail)} missing columns to retail dataset")

missing_in_supply = set(all_columns) - set(df_orders.columns)
for col in missing_in_supply:
    df_orders[col] = None

print(f"Added {len(missing_in_supply)} missing columns to supply chain dataset")

print("\nStep 2.8/4: Ensuring consistent column order...")

df_orders = df_orders[all_columns]
df_sales = df_sales[all_columns]

print(f"Both datasets have {len(all_columns)} columns in same order")

print("\nStep 3/4: Combining datasets...")

df_combined = pd.concat([df_orders, df_sales], ignore_index=True)
print(f"Combined dataset rows: {len(df_combined):,}")
print(f"Supply Chain contribution: {len(df_orders):,} rows")
print(f"Retail contribution: {len(df_sales):,} rows")

print("\nStep 3.5/4: Basic data cleaning...")

duplicates = df_combined.duplicated().sum()
df_combined = df_combined.drop_duplicates()
print(f"Removed duplicate rows: {duplicates}")

if 'Order_Date' in df_combined.columns:
    df_combined['Order_Date'] = pd.to_datetime(df_combined['Order_Date'], errors='coerce')
    print("Converted Order_Date")

if 'Ship_Date' in df_combined.columns:
    df_combined['Ship_Date'] = pd.to_datetime(df_combined['Ship_Date'], errors='coerce')
    print("Converted Ship_Date")

critical_cols = ['Customer_ID', 'Product_ID', 'Order_Date']
critical_cols = [col for col in critical_cols if col in df_combined.columns]
before = len(df_combined)
df_combined = df_combined.dropna(subset=critical_cols)
removed = before - len(df_combined)
print(f"Removed rows missing critical data: {removed:,}")

numeric_cols = ['Sales', 'Quantity', 'Discount', 'Profit']
for col in numeric_cols:
    if col in df_combined.columns:
        df_combined[col] = pd.to_numeric(df_combined[col], errors='coerce').fillna(0)
print("Numeric columns cleaned")

if 'Quantity' in df_combined.columns:
    df_combined.loc[df_combined['Quantity'] < 1, 'Quantity'] = 1
    print("Quantity values corrected")

print("\nStep 3.75/4: Handling negative values...")

def handle_negative_values(df, column):
    if column not in df.columns:
        return df
    
    neg_count = (df[column] < 0).sum()
    
    if neg_count == 0:
        print(f"No negative values found in {column}")
        return df
    
    print(f"\nWARNING: Found {neg_count:,} negative values in {column}")
    print(f"Total {column} (with negatives): ${df[column].sum():,.2f}")
    print(f"Negative {column} sum: ${df[df[column] < 0][column].sum():,.2f}")
    print(f"Positive {column} sum: ${df[df[column] >= 0][column].sum():,.2f}")
    
    print(f"\nHow would you like to handle negative {column} values?")
    print("1 - Convert to absolute values (make them positive)")
    print("2 - Remove rows with negative values")
    
    while True:
        try:
            choice = input(f"\nEnter your choice (1 or 2): ").strip()
            
            if choice == '1':
                df[column] = df[column].abs()
                print(f"Converted {neg_count:,} negative {column} values to absolute")
                print(f"New total {column}: ${df[column].sum():,.2f}")
                break
            elif choice == '2':
                before_count = len(df)
                df = df[df[column] >= 0]
                after_count = len(df)
                print(f"Removed {before_count - after_count:,} rows with negative {column}")
                print(f"New total {column}: ${df[column].sum():,.2f}")
                break
            else:
                print("Invalid choice. Please enter 1 or 2.")
        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
            exit(1)
        except Exception as e:
            print(f"Error: {e}. Please enter 1 or 2.")
    
    return df

if 'Profit' in df_combined.columns:
    df_combined = handle_negative_values(df_combined, 'Profit')

if 'Sales' in df_combined.columns:
    df_combined = handle_negative_values(df_combined, 'Sales')

if 'Discount' in df_combined.columns:
    neg_count = (df_combined['Discount'] < 0).sum()
    if neg_count > 0:
        df_combined['Discount'] = df_combined['Discount'].clip(lower=0)
        print(f"Set {neg_count:,} negative Discount values to 0")

print("\nStep 4/4: Saving staging files...")

df_combined.to_parquet('data/cleaned/orders_cleaned.parquet', index=False)
print("Saved: data/cleaned/orders_cleaned.parquet")

df_orders.to_parquet('data/staging/orders_clean.parquet', index=False)
print("Saved: data/staging/orders_clean.parquet")

df_sales.to_parquet('data/staging/sales_clean.parquet', index=False)
print("Saved: data/staging/sales_clean.parquet")

print("\n" + "=" * 70)
print("DATA QUALITY REPORT")
print("=" * 70)

print(f"\nINTEGRATED DATASET")
print(f"Total Rows: {len(df_combined):,}")
print(f"Total Columns: {len(df_combined.columns)}")

print(f"\nSOURCE BREAKDOWN:")
if 'Source_System' in df_combined.columns:
    source_counts = df_combined['Source_System'].value_counts()
    for source, count in source_counts.items():
        percentage = (count / len(df_combined)) * 100
        print(f"{source}: {count:,} ({percentage:.1f}%)")

if 'Customer_ID' in df_combined.columns:
    print(f"\nUNIQUE CUSTOMERS: {df_combined['Customer_ID'].nunique():,}")
if 'Product_ID' in df_combined.columns:
    print(f"UNIQUE PRODUCTS: {df_combined['Product_ID'].nunique():,}")
if 'Order_Date' in df_combined.columns:
    print(f"\nDATE RANGE:")
    print(f"From: {df_combined['Order_Date'].min()}")
    print(f"To: {df_combined['Order_Date'].max()}")
if 'Sales' in df_combined.columns:
    print(f"\nTOTAL SALES: ${df_combined['Sales'].sum():,.2f}")
if 'Profit' in df_combined.columns:
    print(f"TOTAL PROFIT: ${df_combined['Profit'].sum():,.2f}")
if 'Sales' in df_combined.columns and 'Profit' in df_combined.columns:
    margin = (df_combined['Profit'].sum() / df_combined['Sales'].sum() * 100) if df_combined['Sales'].sum() > 0 else 0
    print(f"PROFIT MARGIN: {margin:.1f}%")

print("\n" + "=" * 70)
print("DATA INGESTION COMPLETE")
print("=" * 70)

print("\nOutput files ready:")
print("- data/cleaned/orders_cleaned.parquet (for dimensions & facts)")
print("- data/staging/orders_clean.parquet (backup)")
print("- data/staging/sales_clean.parquet (backup)")

print("\nNext step: python src/03_transformation/build_dimensions.py")