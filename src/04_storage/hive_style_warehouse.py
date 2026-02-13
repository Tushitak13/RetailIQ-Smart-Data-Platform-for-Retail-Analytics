import pandas as pd
import os
import shutil
from pathlib import Path
import json
from datetime import datetime

print("=" * 70)
print("CREATING HIVE-STYLE PARTITIONED WAREHOUSE")
print("=" * 70)

print("\nChecking for source data...")

source_files = [
    'data/warehouse/dim_customer.parquet',
    'data/warehouse/dim_product.parquet',
    'data/warehouse/dim_location.parquet',
    'data/warehouse/dim_date.parquet',
    'data/warehouse/fact_sales.parquet',
    'data/warehouse/fact_shipments.parquet'
]

missing_files = []
for file in source_files:
    if os.path.exists(file):
        print(f"   Found: {file}")
    else:
        print(f"   Missing: {file}")
        missing_files.append(file)

if missing_files:
    print("\nERROR: Missing source data files!")
    exit(1)

WAREHOUSE_DIR = 'retail_warehouse/'
METASTORE_DIR = 'retail_metastore/'

if os.path.exists(WAREHOUSE_DIR):
    print(f"\nCleaning existing warehouse...")
    shutil.rmtree(WAREHOUSE_DIR)
os.makedirs(WAREHOUSE_DIR, exist_ok=True)
os.makedirs(METASTORE_DIR, exist_ok=True)

print(f"\nWarehouse location: {WAREHOUSE_DIR}")
print(f"Metastore location: {METASTORE_DIR}")

print("\nLoading existing warehouse data...")

try:
    dim_customer = pd.read_parquet('data/warehouse/dim_customer.parquet')
    print(f"   dim_customer: {len(dim_customer):,} rows")
    
    dim_product = pd.read_parquet('data/warehouse/dim_product.parquet')
    print(f"   dim_product: {len(dim_product):,} rows")
    
    dim_location = pd.read_parquet('data/warehouse/dim_location.parquet')
    print(f"   dim_location: {len(dim_location):,} rows")
    
    dim_date = pd.read_parquet('data/warehouse/dim_date.parquet')
    print(f"   dim_date: {len(dim_date):,} rows")
    
    fact_sales = pd.read_parquet('data/warehouse/fact_sales.parquet')
    print(f"   fact_sales: {len(fact_sales):,} rows")
    
    fact_shipments = pd.read_parquet('data/warehouse/fact_shipments.parquet')
    print(f"   fact_shipments: {len(fact_shipments):,} rows")
    
except Exception as e:
    print(f"ERROR loading data: {e}")
    exit(1)

print("\nStep 1/6: Partitioning dim_customer by Region...")

if 'Region' not in dim_customer.columns:
    print("   Region column not found in dim_customer!")
else:
    regions = dim_customer['Region'].dropna().unique()
    print(f"   Found regions: {list(regions)}")
    
    for region in regions:
        partition_path = os.path.join(WAREHOUSE_DIR, 'dim_customer', f'region={region}')
        os.makedirs(partition_path, exist_ok=True)
        
        region_data = dim_customer[dim_customer['Region'] == region].copy()
        region_data = region_data.drop(columns=['Region'])
        region_data.to_parquet(os.path.join(partition_path, 'data.parquet'), index=False)
        
        print(f"   Created partition: region={region} ({len(region_data):,} customers)")
    
    metadata = {
        'table_name': 'dim_customer',
        'partition_column': 'Region',
        'partitions': list(regions),
        'created_date': datetime.now().isoformat(),
        'record_count': len(dim_customer)
    }
    with open(os.path.join(WAREHOUSE_DIR, 'dim_customer', '_METADATA.json'), 'w') as f:
        json.dump(metadata, f, indent=2)

print("\nStep 2/6: Partitioning dim_product by Category...")

if 'Category' not in dim_product.columns:
    print("   Category column not found in dim_product!")
else:
    categories = dim_product['Category'].dropna().unique()
    print(f"   Found categories: {list(categories)}")
    
    for category in categories:
        partition_path = os.path.join(WAREHOUSE_DIR, 'dim_product', f'category={category}')
        os.makedirs(partition_path, exist_ok=True)
        
        category_data = dim_product[dim_product['Category'] == category].copy()
        category_data = category_data.drop(columns=['Category'])
        category_data.to_parquet(os.path.join(partition_path, 'data.parquet'), index=False)
        
        print(f"   Created partition: category={category} ({len(category_data):,} products)")
    
    metadata = {
        'table_name': 'dim_product',
        'partition_column': 'Category',
        'partitions': list(categories),
        'created_date': datetime.now().isoformat(),
        'record_count': len(dim_product)
    }
    with open(os.path.join(WAREHOUSE_DIR, 'dim_product', '_METADATA.json'), 'w') as f:
        json.dump(metadata, f, indent=2)

print("\nStep 3/6: Partitioning dim_date by Year...")

if 'date' not in dim_date.columns:
    print("   date column not found in dim_date!")
else:
    dim_date_copy = dim_date.copy()
    dim_date_copy['year'] = pd.to_datetime(dim_date_copy['date']).dt.year
    years = dim_date_copy['year'].unique()
    print(f"   Found years: {sorted(years)}")
    
    for year in sorted(years):
        partition_path = os.path.join(WAREHOUSE_DIR, 'dim_date', f'year={year}')
        os.makedirs(partition_path, exist_ok=True)
        
        year_data = dim_date_copy[dim_date_copy['year'] == year].copy()
        year_data = year_data.drop(columns=['year'])
        year_data.to_parquet(os.path.join(partition_path, 'data.parquet'), index=False)
        
        print(f"   Created partition: year={year} ({len(year_data):,} dates)")
    
    metadata = {
        'table_name': 'dim_date',
        'partition_column': 'year',
        'partitions': sorted([int(y) for y in years]),
        'created_date': datetime.now().isoformat(),
        'record_count': len(dim_date)
    }
    with open(os.path.join(WAREHOUSE_DIR, 'dim_date', '_METADATA.json'), 'w') as f:
        json.dump(metadata, f, indent=2)

print("\nStep 4/6: Saving dim_location (non-partitioned)...")

location_path = os.path.join(WAREHOUSE_DIR, 'dim_location')
os.makedirs(location_path, exist_ok=True)
dim_location.to_parquet(os.path.join(location_path, 'data.parquet'), index=False)
print(f"   Saved dim_location: {len(dim_location):,} locations")

metadata = {
    'table_name': 'dim_location',
    'partitioned': False,
    'record_count': len(dim_location),
    'created_date': datetime.now().isoformat()
}
with open(os.path.join(location_path, '_METADATA.json'), 'w') as f:
    json.dump(metadata, f, indent=2)

print("\nStep 5/6: Partitioning fact_sales by Year/Month...")

fact_sales_with_dates = fact_sales.merge(dim_date[['date_sk', 'date']], on='date_sk', how='left')
print(f"   Merged fact_sales with dim_date: {len(fact_sales_with_dates):,} rows")

fact_sales_with_dates['order_year'] = pd.to_datetime(fact_sales_with_dates['date']).dt.year
fact_sales_with_dates['order_month'] = pd.to_datetime(fact_sales_with_dates['date']).dt.month

year_month_combo = fact_sales_with_dates[['order_year', 'order_month']].drop_duplicates().sort_values(['order_year', 'order_month'])
print(f"   Found {len(year_month_combo)} year/month partitions")

for _, row in year_month_combo.iterrows():
    year = int(row['order_year'])
    month = int(row['order_month'])
    
    partition_path = os.path.join(
        WAREHOUSE_DIR, 
        'fact_sales', 
        f'order_year={year}',
        f'order_month={str(month).zfill(2)}'
    )
    os.makedirs(partition_path, exist_ok=True)
    
    month_data = fact_sales_with_dates[
        (fact_sales_with_dates['order_year'] == year) & 
        (fact_sales_with_dates['order_month'] == month)
    ].copy()
    
    month_data = month_data.drop(columns=['order_year', 'order_month', 'date'])
    month_data.to_parquet(os.path.join(partition_path, 'data.parquet'), index=False)
    
    print(f"   Created partition: order_year={year}/order_month={month:02d} ({len(month_data):,} records)")

metadata = {
    'table_name': 'fact_sales',
    'partition_columns': ['order_year', 'order_month'],
    'partitions': year_month_combo.to_dict('records'),
    'created_date': datetime.now().isoformat(),
    'record_count': len(fact_sales)
}
with open(os.path.join(WAREHOUSE_DIR, 'fact_sales', '_METADATA.json'), 'w') as f:
    json.dump(metadata, f, indent=2)

print("\nStep 6/6: Partitioning fact_shipments by Year/Month...")

fact_shipments_with_dates = fact_shipments.merge(dim_date[['date_sk', 'date']], on='date_sk', how='left')
print(f"   Merged fact_shipments with dim_date: {len(fact_shipments_with_dates):,} rows")

fact_shipments_with_dates['ship_year'] = pd.to_datetime(fact_shipments_with_dates['date']).dt.year
fact_shipments_with_dates['ship_month'] = pd.to_datetime(fact_shipments_with_dates['date']).dt.month

year_month_combo_ship = fact_shipments_with_dates[['ship_year', 'ship_month']].drop_duplicates().sort_values(['ship_year', 'ship_month'])
print(f"   Found {len(year_month_combo_ship)} year/month partitions")

for _, row in year_month_combo_ship.iterrows():
    year = int(row['ship_year'])
    month = int(row['ship_month'])
    
    partition_path = os.path.join(
        WAREHOUSE_DIR, 
        'fact_shipments', 
        f'ship_year={year}',
        f'ship_month={str(month).zfill(2)}'
    )
    os.makedirs(partition_path, exist_ok=True)
    
    month_data = fact_shipments_with_dates[
        (fact_shipments_with_dates['ship_year'] == year) & 
        (fact_shipments_with_dates['ship_month'] == month)
    ].copy()
    
    month_data = month_data.drop(columns=['ship_year', 'ship_month', 'date'])
    month_data.to_parquet(os.path.join(partition_path, 'data.parquet'), index=False)
    
    print(f"   Created partition: ship_year={year}/ship_month={month:02d} ({len(month_data):,} records)")

metadata = {
    'table_name': 'fact_shipments',
    'partition_columns': ['ship_year', 'ship_month'],
    'partitions': year_month_combo_ship.to_dict('records'),
    'created_date': datetime.now().isoformat(),
    'record_count': len(fact_shipments)
}
with open(os.path.join(WAREHOUSE_DIR, 'fact_shipments', '_METADATA.json'), 'w') as f:
    json.dump(metadata, f, indent=2)

print("\n" + "=" * 70)
print("HIVE-STYLE PARTITIONED WAREHOUSE CREATED!")
print("=" * 70)

print(f"\nWarehouse location: {WAREHOUSE_DIR}")
print("\nPARTITION SUMMARY:")

total_files = 0
for root, dirs, files in os.walk(WAREHOUSE_DIR):
    total_files += len([f for f in files if f.endswith('.parquet')])

print(f"\nTotal parquet files: {total_files}")
print(f"Total tables: 6")

print("\nFolder structure:")
print(f"retail_warehouse/")
print(f"├── dim_customer/     ({len(regions) if 'regions' in locals() else 0} partitions)")
print(f"├── dim_product/      ({len(categories) if 'categories' in locals() else 0} partitions)")
print(f"├── dim_location/     (non-partitioned)")
print(f"├── dim_date/         ({len(years) if 'years' in locals() else 0} partitions)")
print(f"├── fact_sales/       ({len(year_month_combo)} partitions)")
print(f"└── fact_shipments/   ({len(year_month_combo_ship)} partitions)")

print("\nMetastore files created in each directory (_METADATA.json)")
print("=" * 70)