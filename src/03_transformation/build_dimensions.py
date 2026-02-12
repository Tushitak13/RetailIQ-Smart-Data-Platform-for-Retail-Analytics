# src/03_transformation/build_dimensions.py
import pandas as pd
import os

# Paths
CLEANED_DIR = 'data/cleaned/'
WAREHOUSE_DIR = 'data/warehouse/'
os.makedirs(WAREHOUSE_DIR, exist_ok=True)

print("=" * 70)
print("BUILDING DIMENSION TABLES")
print("=" * 70)

# Load cleaned integrated data (BOTH datasets combined)
orders = pd.read_parquet(os.path.join(CLEANED_DIR, 'orders_cleaned.parquet'))
print(f"\n📊 Loaded integrated data: {len(orders):,} rows")
print(f"   Source systems: {orders['Source_System'].unique()}")
print(f"   Date range: {orders['Order_Date'].min()} to {orders['Order_Date'].max()}")

# -------------------------
# DIM_CUSTOMER
# -------------------------
print("\n👤 1. Building dim_customer...")

dim_customer = orders[['Customer_ID', 'Customer_Name', 'Segment', 'Region', 'Source_System']].drop_duplicates()

# If same customer appears in both datasets, keep the first occurrence
dim_customer = dim_customer.drop_duplicates(subset=['Customer_ID'], keep='first')

# Add surrogate key
dim_customer['customer_sk'] = range(1, len(dim_customer) + 1)

# Reorder columns
dim_customer = dim_customer[['customer_sk', 'Customer_ID', 'Customer_Name', 'Segment', 'Region', 'Source_System']]

# Save
dim_customer.to_parquet(os.path.join(WAREHOUSE_DIR, 'dim_customer.parquet'), index=False)
print(f"   ✅ dim_customer created: {dim_customer.shape[0]:,} rows")
print(f"   💡 Includes customers from all source systems")

# -------------------------
# DIM_PRODUCT
# -------------------------
print("\n📦 2. Building dim_product...")

dim_product = orders[['Product_ID', 'Product_Name', 'Category', 'Sub_Category', 'Source_System']].drop_duplicates()

# If same product appears in both datasets, keep the first occurrence
dim_product = dim_product.drop_duplicates(subset=['Product_ID'], keep='first')

# Add surrogate key
dim_product['product_sk'] = range(1, len(dim_product) + 1)

# Reorder columns
dim_product = dim_product[['product_sk', 'Product_ID', 'Product_Name', 'Category', 'Sub_Category', 'Source_System']]

# Save
dim_product.to_parquet(os.path.join(WAREHOUSE_DIR, 'dim_product.parquet'), index=False)
print(f"   ✅ dim_product created: {dim_product.shape[0]:,} rows")
print(f"   💡 Includes products from all source systems")

# -------------------------
# DIM_LOCATION (instead of dim_store)
# -------------------------
print("\n📍 3. Building dim_location...")

# Check if location columns exist
location_cols = ['State', 'City', 'Postal_Code', 'Region']
available_loc_cols = [col for col in location_cols if col in orders.columns]

if available_loc_cols:
    dim_location = orders[available_loc_cols + ['Source_System']].drop_duplicates()
    dim_location = dim_location.fillna('Unknown')
    
    # Add surrogate key
    dim_location['location_sk'] = range(1, len(dim_location) + 1)
    
    # Create composite key for easy lookup
    dim_location['location_key'] = (
        dim_location['State'].astype(str) + '|' + 
        dim_location['City'].astype(str) + '|' + 
        dim_location['Postal_Code'].astype(str)
    )
    
    # Reorder columns
    cols = ['location_sk', 'location_key'] + available_loc_cols + ['Source_System']
    dim_location = dim_location[cols]
    
    # Save
    dim_location.to_parquet(os.path.join(WAREHOUSE_DIR, 'dim_location.parquet'), index=False)
    print(f"   ✅ dim_location created: {dim_location.shape[0]:,} rows")
else:
    print("   ⚠️ No location columns found, creating placeholder")
    dim_location = pd.DataFrame({
        'location_sk': [1],
        'location_key': ['Unknown|Unknown|Unknown'],
        'State': ['Unknown'],
        'City': ['Unknown'],
        'Postal_Code': ['00000'],
        'Region': ['Unknown'],
        'Source_System': ['Integrated']
    })
    dim_location.to_parquet(os.path.join(WAREHOUSE_DIR, 'dim_location.parquet'), index=False)
    print(f"   ✅ dim_location created (placeholder)")

# -------------------------
# DIM_DATE
# -------------------------
print("\n📅 4. Building dim_date...")

# Get all dates from Order_Date and Ship_Date
order_dates = orders['Order_Date'].dropna()
ship_dates = orders['Ship_Date'].dropna() if 'Ship_Date' in orders.columns else pd.Series()
all_dates = pd.concat([order_dates, ship_dates]).drop_duplicates().sort_values().reset_index(drop=True)

dim_date = pd.DataFrame({
    'date_sk': range(1, len(all_dates) + 1),
    'date': all_dates
})

# Add date attributes
dim_date['day'] = dim_date['date'].dt.day
dim_date['month'] = dim_date['date'].dt.month
dim_date['month_name'] = dim_date['date'].dt.strftime('%B')
dim_date['quarter'] = dim_date['date'].dt.quarter
dim_date['year'] = dim_date['date'].dt.year
dim_date['year_quarter'] = dim_date['year'].astype(str) + '-Q' + dim_date['quarter'].astype(str)
dim_date['weekday'] = dim_date['date'].dt.weekday + 1
dim_date['weekday_name'] = dim_date['date'].dt.strftime('%A')
dim_date['is_weekend'] = dim_date['weekday'].isin([6, 7]).astype(int)

# Save
dim_date.to_parquet(os.path.join(WAREHOUSE_DIR, 'dim_date.parquet'), index=False)
print(f"   ✅ dim_date created: {dim_date.shape[0]:,} rows")
print(f"   📅 Date range: {dim_date['date'].min().date()} to {dim_date['date'].max().date()}")

print("\n" + "=" * 70)
print("✅ DIMENSION BUILDING COMPLETE")
print("=" * 70)

# Summary
print("\n📊 DIMENSIONS SUMMARY:")
print(f"   - Customers: {len(dim_customer):,}")
print(f"   - Products: {len(dim_product):,}")
print(f"   - Locations: {len(dim_location):,}")
print(f"   - Dates: {len(dim_date):,}")