import pandas as pd
import os

CLEANED_DIR = 'data/cleaned/'
WAREHOUSE_DIR = 'data/warehouse/'

print("=" * 70)
print("BUILDING FACT TABLES")
print("=" * 70)

orders = pd.read_parquet(os.path.join(CLEANED_DIR, 'orders_cleaned.parquet'))
print(f"\nLoaded integrated data: {len(orders):,} rows")

dim_customer = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_customer.parquet'))
dim_product = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_product.parquet'))
dim_location = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_location.parquet'))
dim_date = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_date.parquet'))

print("\nCreating lookup dictionaries...")

customer_dict = dim_customer.set_index('Customer_ID')['customer_sk'].to_dict()
product_dict = dim_product.set_index('Product_ID')['product_sk'].to_dict()
date_dict = dim_date.set_index('date')['date_sk'].to_dict()

if 'location_key' in dim_location.columns:
    location_dict = dim_location.set_index('location_key')['location_sk'].to_dict()
else:
    location_dict = {}

print("\nBuilding fact_sales...")

fact_sales = orders.copy()

if all(col in fact_sales.columns for col in ['State', 'City', 'Postal_Code']):
    fact_sales['location_key'] = (
        fact_sales['State'].fillna('Unknown').astype(str) + '|' + 
        fact_sales['City'].fillna('Unknown').astype(str) + '|' + 
        fact_sales['Postal_Code'].fillna('0').astype(str)
    )
    fact_sales['location_sk'] = fact_sales['location_key'].map(location_dict)
else:
    fact_sales['location_sk'] = 1

fact_sales['customer_sk'] = fact_sales['Customer_ID'].map(customer_dict)
fact_sales['product_sk'] = fact_sales['Product_ID'].map(product_dict)
fact_sales['date_sk'] = fact_sales['Order_Date'].map(date_dict)

before = len(fact_sales)
fact_sales = fact_sales.dropna(subset=['customer_sk', 'product_sk', 'location_sk', 'date_sk'])
unmapped = before - len(fact_sales)
print(f"Removed {unmapped:,} records with unmapped keys ({unmapped/before*100:.1f}%)")

fact_columns = [
    'Order_ID', 'customer_sk', 'product_sk', 'location_sk', 'date_sk',
    'Sales', 'Quantity', 'Discount', 'Profit', 'Source_System'
]
fact_columns = [col for col in fact_columns if col in fact_sales.columns]
fact_sales = fact_sales[fact_columns].copy()

fact_sales = fact_sales.rename(columns={'Order_ID': 'order_id'})

fact_sales['Sales'] = fact_sales['Sales'].astype(float)
fact_sales['Quantity'] = fact_sales['Quantity'].astype(int)
if 'Discount' in fact_sales.columns:
    fact_sales['Discount'] = fact_sales['Discount'].astype(float)
if 'Profit' in fact_sales.columns:
    fact_sales['Profit'] = fact_sales['Profit'].astype(float)

fact_sales.to_parquet(os.path.join(WAREHOUSE_DIR, 'fact_sales.parquet'), index=False)
print(f"fact_sales created: {len(fact_sales):,} rows")
print("Measures: Sales, Quantity, Discount, Profit")
print("Foreign keys: customer_sk, product_sk, location_sk, date_sk")

if 'Ship_Date' in orders.columns:
    print("\nBuilding fact_shipments...")
    
    fact_shipments = orders.copy()
    
    if all(col in fact_shipments.columns for col in ['State', 'City', 'Postal_Code']):
        fact_shipments['location_key'] = (
            fact_shipments['State'].fillna('Unknown').astype(str) + '|' + 
            fact_shipments['City'].fillna('Unknown').astype(str) + '|' + 
            fact_shipments['Postal_Code'].fillna('0').astype(str)
        )
        fact_shipments['location_sk'] = fact_shipments['location_key'].map(location_dict)
    else:
        fact_shipments['location_sk'] = 1
    
    fact_shipments['product_sk'] = fact_shipments['Product_ID'].map(product_dict)
    fact_shipments['date_sk'] = fact_shipments['Ship_Date'].map(date_dict)
    
    fact_shipments['Delivery_Time'] = (fact_shipments['Ship_Date'] - fact_shipments['Order_Date']).dt.days
    
    fact_shipments = fact_shipments.dropna(subset=['product_sk', 'location_sk', 'date_sk'])
    
    shipment_cols = ['Order_ID', 'product_sk', 'location_sk', 'date_sk', 
                     'Ship_Mode', 'Delivery_Time', 'Returned', 'Source_System']
    shipment_cols = [col for col in shipment_cols if col in fact_shipments.columns]
    fact_shipments = fact_shipments[shipment_cols].copy()
    fact_shipments = fact_shipments.rename(columns={'Order_ID': 'order_id'})
    
    fact_shipments.to_parquet(os.path.join(WAREHOUSE_DIR, 'fact_shipments.parquet'), index=False)
    print(f"fact_shipments created: {len(fact_shipments):,} rows")
else:
    print("\nSkipping fact_shipments: No Ship_Date column found")

print("\n" + "=" * 70)
print("FACT TABLES SUMMARY")
print("=" * 70)

print(f"\nfact_sales: {len(fact_sales):,} records")
if 'Profit' in fact_sales.columns:
    print(f"Total Sales: ${fact_sales['Sales'].sum():,.2f}")
    print(f"Total Profit: ${fact_sales['Profit'].sum():,.2f}")
    margin = (fact_sales['Profit'].sum() / fact_sales['Sales'].sum() * 100) if fact_sales['Sales'].sum() > 0 else 0
    print(f"Profit Margin: {margin:.1f}%")

print(f"\nRecords by Source System:")
source_counts = fact_sales['Source_System'].value_counts()
for source, count in source_counts.items():
    print(f"{source}: {count:,} ({count/len(fact_sales)*100:.1f}%)")

if 'fact_shipments' in locals():
    print(f"\nfact_shipments: {len(fact_shipments):,} records")
    if 'Delivery_Time' in fact_shipments.columns:
        print(f"Avg Delivery Time: {fact_shipments['Delivery_Time'].mean():.1f} days")

print("\n" + "=" * 70)
print("FACT BUILDING COMPLETE")
print("=" * 70)






