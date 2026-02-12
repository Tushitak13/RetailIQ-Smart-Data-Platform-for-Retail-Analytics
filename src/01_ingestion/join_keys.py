import pandas as pd

print("=" * 70)
print("JOIN KEYS DOCUMENTATION")
print("=" * 70)

# Load staging data
df_orders = pd.read_parquet('data/staging/orders_clean.parquet')
df_sales = pd.read_parquet('data/staging/sales_clean.parquet')

print("\nStep 1: Analyzing available columns...")

print("\nORDERS TABLE COLUMNS:")
for i, col in enumerate(df_orders.columns, 1):
    print(f"  {i:2d}. {col}")

print("\nSALES TABLE COLUMNS:")
for i, col in enumerate(df_sales.columns, 1):
    print(f"  {i:2d}. {col}")

# Document dimension table keys
print("\n" + "=" * 70)
print("DIMENSION TABLE KEY MAPPINGS")
print("=" * 70)

print("\n1. DIMENSION: dim_customer")
print("   Primary Key: Customer ID")
if 'Customer ID' in df_orders.columns:
    print(f"   Unique Customers: {df_orders['Customer ID'].nunique():,}")
print("   Attributes: Customer ID, Customer Name, Segment, Region")

print("\n2. DIMENSION: dim_product")
print("   Primary Key: Product ID")
if 'Product ID' in df_orders.columns:
    print(f"   Unique Products: {df_orders['Product ID'].nunique():,}")
print("   Attributes: Product ID, Product Name, Category, Sub-Category")

print("\n3. DIMENSION: dim_store")
print("   Primary Key: store_id (generated)")
if 'State' in df_orders.columns and 'City' in df_orders.columns:
    unique_locations = df_orders[['State', 'City']].drop_duplicates().shape[0]
    print(f"   Unique Locations: {unique_locations:,}")
print("   Attributes: store_id, State, City, Region, Postal Code")

print("\n4. DIMENSION: dim_date")
print("   Primary Key: date_id")
if 'Order Date' in df_orders.columns:
    min_date = df_orders['Order Date'].min()
    max_date = df_orders['Order Date'].max()
    print(f"   Date Range: {min_date} to {max_date}")
print("   Attributes: date_id, Date, Year, Quarter, Month, Day")

# Document fact table joins
print("\n" + "=" * 70)
print("FACT TABLE JOIN SPECIFICATIONS")
print("=" * 70)

print("\n5. FACT TABLE: fact_sales")
print("   Source: orders_clean.parquet")
print("   Join Specifications:")
print("     - orders.Customer ID -> dim_customer.customer_id")
print("     - orders.Product ID -> dim_product.product_id")
print("     - orders.State + City -> dim_store.state + city")
print("     - orders.Order Date -> dim_date.date")
print("   Measures: Sales, Quantity, Discount, Profit")

print("\n6. FACT TABLE: fact_shipments")
print("   Source: orders_clean.parquet")
print("   Join Specifications:")
print("     - orders.Product ID -> dim_product.product_id")
print("     - orders.State + City -> dim_store.state + city")
print("     - orders.Ship Date -> dim_date.date")
print("   Measures: Ship Mode, Delivery Time, Returned")

# Data quality checks
print("\n" + "=" * 70)
print("DATA QUALITY CHECKS")
print("=" * 70)

print("\nChecking for NULL values in key columns...")
key_columns = ['Customer ID', 'Product ID', 'Order Date', 'Ship Date']
for col in key_columns:
    if col in df_orders.columns:
        null_count = df_orders[col].isnull().sum()
        null_pct = (null_count / len(df_orders)) * 100
        print(f"  {col}: {null_count:,} nulls ({null_pct:.2f}%)")

print("\nChecking for duplicate Order IDs...")
if 'Order ID' in df_orders.columns:
    duplicate_orders = df_orders['Order ID'].duplicated().sum()
    print(f"  Duplicate Order IDs: {duplicate_orders:,}")
    print("  Note: Multiple products per order is expected behavior")

print("\nDate range validation...")
if 'Order Date' in df_orders.columns:
    order_date_range = (df_orders['Order Date'].min(), df_orders['Order Date'].max())
    print(f"  Order Date Range: {order_date_range[0]} to {order_date_range[1]}")

if 'Ship Date' in df_orders.columns:
    ship_date_range = (df_orders['Ship Date'].min(), df_orders['Ship Date'].max())
    print(f"  Ship Date Range: {ship_date_range[0]} to {ship_date_range[1]}")

print("\n" + "=" * 70)
print("JOIN KEYS DOCUMENTATION COMPLETE")
print("=" * 70)
print("\nNext step: Use these specifications to build dimensional model")