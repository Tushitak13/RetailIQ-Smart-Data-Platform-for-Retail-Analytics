# src/01_ingestion/load_data.py
import pandas as pd
import os
import numpy as np

print("=" * 70)
print("DATA INGESTION MODULE")
print("=" * 70)

# Create directories
os.makedirs('data/staging', exist_ok=True)
os.makedirs('data/cleaned', exist_ok=True)

# -------------------------
# LOAD RAW DATA FILES WITH ENCODING FIX
# -------------------------
print("\nStep 1/4: Loading raw CSV files...")

# Try different encodings
encodings_to_try = ['latin-1', 'cp1252', 'ISO-8859-1', 'utf-8']

# Load Supply Chain Dataset
df_orders = None
for i, enc in enumerate(encodings_to_try, 1):
    try:
        df_orders = pd.read_csv('data/raw/Copy of Retail-Supply-Chain-Sales-Dataset.csv', encoding=enc)
        print(f"✅ File loaded: data/raw/Copy of Retail-Supply-Chain-Sales-Dataset.csv")
        print(f"   Rows: {len(df_orders):,}, Columns: {len(df_orders.columns)} (encoding={enc}, attempt={i})")
        break
    except Exception as e:
        print(f"   Attempt {i} with {enc}: Failed - {str(e)[:50]}...")
        continue

if df_orders is None:
    print("❌ CRITICAL: Could not load Supply Chain dataset with any encoding!")
    exit(1)

# Load Retail Sales Dataset
df_sales = None
for i, enc in enumerate(encodings_to_try, 1):
    try:
        df_sales = pd.read_csv('data/raw/Retail Sales Dataset.csv', encoding=enc)
        print(f"✅ File loaded: data/raw/Retail Sales Dataset.csv")
        print(f"   Rows: {len(df_sales):,}, Columns: {len(df_sales.columns)} (encoding={enc}, attempt={i})")
        break
    except Exception as e:
        print(f"   Attempt {i} with {enc}: Failed - {str(e)[:50]}...")
        continue

if df_sales is None:
    print("❌ CRITICAL: Could not load Retail Sales dataset with any encoding!")
    exit(1)

# -------------------------
# CRITICAL FIX: SET SOURCE COLUMNS IMMEDIATELY
# -------------------------
print("\nStep 1.5/4: Adding source tracking...")
df_orders['Source_System'] = 'Supply_Chain'
df_sales['Source_System'] = 'Retail_Sales'

# VERIFY source columns were set
print(f"   ✅ Supply Chain source: {df_orders['Source_System'].iloc[0]}")
print(f"   ✅ Retail source: {df_sales['Source_System'].iloc[0]}")
print(f"   📊 Supply Chain rows: {len(df_orders):,}")
print(f"   📊 Retail rows: {len(df_sales):,}")

# -------------------------
# FIX: CREATE MISSING IDs FOR RETAIL DATASET
# -------------------------
print("\n🔧 Step 1.75/4: Ensuring retail dataset has required IDs...")

# Check and create Customer_ID if missing
if 'Customer_ID' not in df_sales.columns:
    df_sales['Customer_ID'] = ['RETAIL-CUST-' + str(i).zfill(6) for i in range(1, len(df_sales) + 1)]
    print(f"   ✅ Created Customer_ID for {len(df_sales):,} retail records")
else:
    # Ensure no nulls in Customer_ID
    null_customers = df_sales['Customer_ID'].isnull().sum()
    if null_customers > 0:
        df_sales.loc[df_sales['Customer_ID'].isnull(), 'Customer_ID'] = \
            ['RETAIL-CUST-' + str(i).zfill(6) for i in range(1, null_customers + 1)]
        print(f"   ✅ Filled {null_customers} null Customer_ID values")

# Check and create Product_ID if missing
if 'Product_ID' not in df_sales.columns:
    df_sales['Product_ID'] = ['RETAIL-PROD-' + str(i).zfill(6) for i in range(1, len(df_sales) + 1)]
    print(f"   ✅ Created Product_ID for {len(df_sales):,} retail records")
else:
    # Ensure no nulls in Product_ID
    null_products = df_sales['Product_ID'].isnull().sum()
    if null_products > 0:
        df_sales.loc[df_sales['Product_ID'].isnull(), 'Product_ID'] = \
            ['RETAIL-PROD-' + str(i).zfill(6) for i in range(1, null_products + 1)]
        print(f"   ✅ Filled {null_products} null Product_ID values")

# Check and create Order_Date if missing
if 'Order_Date' not in df_sales.columns and 'Date' in df_sales.columns:
    df_sales['Order_Date'] = df_sales['Date']
    print(f"   ✅ Mapped Date column to Order_Date")
elif 'Order_Date' not in df_sales.columns:
    # Use current date as fallback
    df_sales['Order_Date'] = pd.Timestamp.now()
    print(f"   ⚠️ Created default Order_Date for retail records")

# Check and create Customer_Name if missing
if 'Customer_Name' not in df_sales.columns:
    df_sales['Customer_Name'] = 'Retail Customer'
    print(f"   ✅ Created default Customer_Name for retail records")

# Check and create Product_Name if missing
if 'Product_Name' not in df_sales.columns:
    df_sales['Product_Name'] = df_sales['Product_ID']
    print(f"   ✅ Created Product_Name from Product_ID")

# -------------------------
# STANDARDIZE COLUMN NAMES
# -------------------------
print("\nStep 2/4: Standardizing column names...")

def clean_columns(df):
    """Clean column names - remove spaces, special chars, standardize format"""
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('-', '_')
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
    return df

df_orders = clean_columns(df_orders)
df_sales = clean_columns(df_sales)
print("   ✅ Column names standardized")

# -------------------------
# MAP RETAIL COLUMNS TO MATCH SUPPLY CHAIN
# -------------------------
print("\nStep 2.5/4: Mapping retail columns to match supply chain...")

# Comprehensive column mapping
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

# Only rename columns that exist
rename_dict = {k: v for k, v in column_mapping.items() if k in df_sales.columns}
if rename_dict:
    df_sales = df_sales.rename(columns=rename_dict)
    print(f"   ✅ Mapped {len(rename_dict)} columns: {list(rename_dict.values())}")
else:
    print("   ⚠️ No columns needed mapping")

# -------------------------
# FIX: REMOVE DUPLICATE COLUMNS BEFORE ALIGNING
# -------------------------
print("\nStep 2.6/4: Removing duplicate columns...")

# Check for duplicate column names in each dataset
def remove_duplicate_columns(df, df_name):
    """Remove duplicate column names, keep first occurrence"""
    if df.columns.duplicated().any():
        duplicates = df.columns[df.columns.duplicated()].unique()
        df = df.loc[:, ~df.columns.duplicated()]
        print(f"   ✅ Removed duplicate columns from {df_name}: {list(duplicates)}")
    return df

df_orders = remove_duplicate_columns(df_orders, "Supply Chain")
df_sales = remove_duplicate_columns(df_sales, "Retail")

# -------------------------
# ADD MISSING COLUMNS TO BOTH DATASETS
# -------------------------
print("\nStep 2.75/4: Aligning columns across datasets...")

# Get all unique columns from both datasets
all_columns = set(df_orders.columns) | set(df_sales.columns)

# Convert to sorted list for consistency
all_columns = sorted(list(all_columns))

# Add missing columns to retail dataset
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

print(f"   ✅ Added {len(missing_in_retail)} missing columns to retail dataset")

# Add missing columns to supply chain dataset
missing_in_supply = set(all_columns) - set(df_orders.columns)
for col in missing_in_supply:
    if col in ['High_Value_Order', 'Delivery_Status', 'Sales_Rep_E_mail', 'Age', 'Retail_Sales_People', 'Payment_Method', 'Gender', 'Store_Type']:
        df_orders[col] = None
    else:
        df_orders[col] = None

print(f"   ✅ Added {len(missing_in_supply)} missing columns to supply chain dataset")

# -------------------------
# ENSURE SAME COLUMN ORDER
# -------------------------
print("\nStep 2.8/4: Ensuring consistent column order...")

# Sort columns alphabetically for consistency
all_columns = sorted(list(all_columns))
df_orders = df_orders[all_columns]
df_sales = df_sales[all_columns]

print(f"   ✅ Both datasets have {len(all_columns)} columns in same order")

# -------------------------
# COMBINE DATASETS
# -------------------------
print("\nStep 3/4: Combining datasets...")

try:
    # Combine datasets
    df_combined = pd.concat([df_orders, df_sales], ignore_index=True, sort=False)
    print(f"   ✅ Combined dataset rows: {len(df_combined):,}")
    print(f"   📊 Supply Chain contribution: {len(df_orders):,} rows")
    print(f"   📊 Retail contribution: {len(df_sales):,} rows")
except Exception as e:
    print(f"   ❌ Error combining: {e}")
    print("   Trying alternative method...")
    
    # Alternative: Reset index and concat
    df_orders = df_orders.reset_index(drop=True)
    df_sales = df_sales.reset_index(drop=True)
    df_combined = pd.concat([df_orders, df_sales], ignore_index=True, sort=False)
    print(f"   ✅ Combined dataset rows (alternative method): {len(df_combined):,}")

# -------------------------
# BASIC DATA CLEANING
# -------------------------
print("\nStep 3.5/4: Basic data cleaning...")

# Remove duplicates
duplicates = df_combined.duplicated().sum()
df_combined = df_combined.drop_duplicates()
print(f"   ✅ Removed duplicate rows: {duplicates}")

# Convert date columns
if 'Order_Date' in df_combined.columns:
    df_combined['Order_Date'] = pd.to_datetime(df_combined['Order_Date'], errors='coerce')
    print(f"   ✅ Converted Order_Date")

if 'Ship_Date' in df_combined.columns:
    df_combined['Ship_Date'] = pd.to_datetime(df_combined['Ship_Date'], errors='coerce')
    print(f"   ✅ Converted Ship_Date")

# Remove rows with missing critical data
critical_cols = ['Customer_ID', 'Product_ID', 'Order_Date']
critical_cols = [col for col in critical_cols if col in df_combined.columns]
before = len(df_combined)
df_combined = df_combined.dropna(subset=critical_cols)
removed = before - len(df_combined)
print(f"   ✅ Removed rows missing critical data: {removed:,}")

# Convert numeric columns
numeric_cols = ['Sales', 'Quantity', 'Discount', 'Profit']
for col in numeric_cols:
    if col in df_combined.columns:
        df_combined[col] = pd.to_numeric(df_combined[col], errors='coerce').fillna(0)
print(f"   ✅ Numeric columns cleaned")

# Fix quantity (minimum 1)
if 'Quantity' in df_combined.columns:
    df_combined.loc[df_combined['Quantity'] < 1, 'Quantity'] = 1
    print(f"   ✅ Quantity values corrected")

# -------------------------
# HANDLE NEGATIVE VALUES
# -------------------------
print("\nStep 3.75/4: Handling negative values...")

negative_cols = []
for col in ['Profit', 'Sales', 'Discount']:
    if col in df_combined.columns:
        neg_count = (df_combined[col] < 0).sum()
        if neg_count > 0:
            negative_cols.append((col, neg_count))

if negative_cols:
    print(f"\n⚠️ Negative values found:")
    for col, count in negative_cols:
        print(f"   {col}: {count:,}")
    
    # Handle Profit negatives
    profit_neg_count = (df_combined['Profit'] < 0).sum() if 'Profit' in df_combined.columns else 0
    if profit_neg_count > 0:
        print("\nChoose handling method for negative Profit:")
        print("   1. Convert to absolute value (keep as positive)")
        print("   2. Remove rows with negative values")
        
        try:
            choice = input("Enter choice (1 or 2): ").strip()
        except:
            choice = '1'  # Default to convert if input fails
        
        if choice == '2':
            before = len(df_combined)
            df_combined = df_combined[df_combined['Profit'] >= 0]
            removed = before - len(df_combined)
            print(f"   ✅ Removed rows with negative Profit: {removed:,}")
        else:
            neg_count = (df_combined['Profit'] < 0).sum()
            df_combined['Profit'] = df_combined['Profit'].abs()
            print(f"   ✅ Converted negatives in Profit: {neg_count:,}")
    
    print(f"\n✅ Negative value handling summary")
    print(f"   Values corrected: {sum(c for _,c in negative_cols if _ == 'Profit') if choice != '2' else 0}")
    print(f"   Rows removed: {removed if 'choice' in locals() and choice == '2' else 0}")
else:
    print("   ✅ No negative values found")

# -------------------------
# SAVE STAGING FILES
# -------------------------
print("\nStep 4/4: Saving staging files...")

# Save combined cleaned data (for dimensions & facts)
df_combined.to_parquet('data/cleaned/orders_cleaned.parquet', index=False)
print(f"   ✅ Saved: data/cleaned/orders_cleaned.parquet")

# Save individual files for backward compatibility
df_orders.to_parquet('data/staging/orders_clean.parquet', index=False)
print(f"   ✅ Saved: data/staging/orders_clean.parquet")

df_sales.to_parquet('data/staging/sales_clean.parquet', index=False)
print(f"   ✅ Saved: data/staging/sales_clean.parquet")

# -------------------------
# FINAL DATA QUALITY REPORT
# -------------------------
print("\n" + "=" * 70)
print("DATA QUALITY REPORT")
print("=" * 70)

print(f"\n📊 INTEGRATED DATASET")
print(f"   Total Rows: {len(df_combined):,}")
print(f"   Total Columns: {len(df_combined.columns)}")

print(f"\n🔄 SOURCE BREAKDOWN:")
if 'Source_System' in df_combined.columns:
    source_counts = df_combined['Source_System'].value_counts()
    for source, count in source_counts.items():
        percentage = (count / len(df_combined)) * 100
        print(f"   {source}: {count:,} ({percentage:.1f}%)")
else:
    print("   ⚠️ Source_System column missing!")

if 'Customer_ID' in df_combined.columns:
    print(f"\n👤 UNIQUE CUSTOMERS: {df_combined['Customer_ID'].nunique():,}")
if 'Product_ID' in df_combined.columns:
    print(f"📦 UNIQUE PRODUCTS: {df_combined['Product_ID'].nunique():,}")
if 'Order_Date' in df_combined.columns:
    print(f"\n📅 DATE RANGE:")
    print(f"   From: {df_combined['Order_Date'].min()}")
    print(f"   To: {df_combined['Order_Date'].max()}")
if 'Sales' in df_combined.columns:
    print(f"\n💰 TOTAL SALES: ${df_combined['Sales'].sum():,.2f}")
if 'Profit' in df_combined.columns:
    print(f"💵 TOTAL PROFIT: ${df_combined['Profit'].sum():,.2f}")
if 'Sales' in df_combined.columns and 'Profit' in df_combined.columns:
    margin = (df_combined['Profit'].sum() / df_combined['Sales'].sum() * 100) if df_combined['Sales'].sum() > 0 else 0
    print(f"📈 PROFIT MARGIN: {margin:.1f}%")

print("\n" + "=" * 70)
print("✅ DATA INGESTION COMPLETE")
print("=" * 70)

print("\n📁 Output files ready:")
print("   - data/cleaned/orders_cleaned.parquet (for dimensions & facts)")
print("   - data/staging/orders_clean.parquet (backup)")
print("   - data/staging/sales_clean.parquet (backup)")

print("\n👉 Next step: python src/03_transformation/build_dimensions.py")