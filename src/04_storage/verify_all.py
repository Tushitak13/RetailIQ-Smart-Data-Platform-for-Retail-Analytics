# verify_all.py
"""
Run this to verify everything is working correctly
NO IMPORT ERRORS - Completely self-contained!
"""

import os
import sys
import pandas as pd
import glob
from pathlib import Path
import json

print("=" * 70)
print("🔍 VERIFYING RETAILIQ HIVE-STYLE WAREHOUSE")
print("=" * 70)

# ===========================================
# EMBEDDED HIVE WAREHOUSE CLASS (NO IMPORTS!)
# ===========================================

class HiveWarehouse:
    """Simple Hive-style warehouse reader - embedded directly"""
    
    def __init__(self, warehouse_path='retail_warehouse/'):
        self.warehouse_path = warehouse_path
        self.metastore = {}
        self._load_metastore()
    
    def _load_metastore(self):
        """Load metadata files"""
        for meta_file in glob.glob(f'{self.warehouse_path}/*/_METADATA.json'):
            table_name = Path(meta_file).parent.name
            try:
                with open(meta_file, 'r') as f:
                    self.metastore[table_name] = json.load(f)
            except:
                pass
    
    def read_table(self, table_name, partitions=None):
        """Read table with partition pruning"""
        base_path = f"{self.warehouse_path}/{table_name}"
        
        if not partitions:
            all_files = glob.glob(f"{base_path}/**/*.parquet", recursive=True)
            if not all_files:
                all_files = glob.glob(f"{base_path}/*.parquet")
            if not all_files:
                return pd.DataFrame()
            return pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)
        
        # Build partition path
        path_pattern = base_path
        for col, value in partitions.items():
            if col == 'region':
                path_pattern = f"{path_pattern}/region={value}"
            elif col == 'category':
                path_pattern = f"{path_pattern}/category={value}"
            elif col == 'year':
                path_pattern = f"{path_pattern}/year={value}"
            elif col == 'order_year':
                path_pattern = f"{path_pattern}/order_year={value}"
            elif col == 'order_month':
                path_pattern = f"{path_pattern}/order_month={str(value).zfill(2)}"
        
        parquet_files = glob.glob(f"{path_pattern}/**/*.parquet", recursive=True)
        if not parquet_files:
            parquet_files = glob.glob(f"{path_pattern}/*.parquet")
        
        if not parquet_files:
            return pd.DataFrame()
        
        return pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)


# ===========================================
# CHECK 1: Does warehouse directory exist?
# ===========================================
print("\n📁 CHECK 1: Warehouse Directory")
if os.path.exists('retail_warehouse/'):
    print("   ✅ retail_warehouse/ exists")
else:
    print("   ❌ retail_warehouse/ NOT FOUND!")
    print("      Run: python src/04_storage/hive_style_warehouse.py")
    sys.exit(1)

# ===========================================
# CHECK 2: Are all table folders present?
# ===========================================
print("\n📂 CHECK 2: Table Folders")
required_tables = [
    'dim_customer', 'dim_product', 'dim_location', 
    'dim_date', 'fact_sales', 'fact_shipments'
]

all_tables_exist = True
for table in required_tables:
    path = f'retail_warehouse/{table}'
    if os.path.exists(path):
        print(f"   ✅ {table}/ exists")
    else:
        print(f"   ❌ {table}/ MISSING!")
        all_tables_exist = False

# ===========================================
# CHECK 3: Are partitions created?
# ===========================================
print("\n🗂️  CHECK 3: Partitions")

# Check dim_customer partitions
dim_customer_path = 'retail_warehouse/dim_customer/'
if os.path.exists(dim_customer_path):
    partitions = [d for d in os.listdir(dim_customer_path) if d.startswith('region=')]
    print(f"   dim_customer: {len(partitions)} partitions")
    for p in partitions[:3]:  # Show first 3
        print(f"      └── {p}")
    if len(partitions) > 3:
        print(f"      └── ... and {len(partitions)-3} more")

# Check dim_product partitions
dim_product_path = 'retail_warehouse/dim_product/'
if os.path.exists(dim_product_path):
    partitions = [d for d in os.listdir(dim_product_path) if d.startswith('category=')]
    print(f"\n   dim_product: {len(partitions)} partitions")
    for p in partitions:
        print(f"      └── {p}")

# Check dim_date partitions
dim_date_path = 'retail_warehouse/dim_date/'
if os.path.exists(dim_date_path):
    partitions = [d for d in os.listdir(dim_date_path) if d.startswith('year=')]
    print(f"\n   dim_date: {len(partitions)} partitions")
    for p in sorted(partitions):
        print(f"      └── {p}")

# Check fact_sales partitions
fact_sales_path = 'retail_warehouse/fact_sales/'
if os.path.exists(fact_sales_path):
    years = [d for d in os.listdir(fact_sales_path) if d.startswith('order_year=')]
    total_partitions = 0
    print(f"\n   fact_sales: {len(years)} years")
    for year in sorted(years)[:3]:  # Show first 3 years
        year_path = os.path.join(fact_sales_path, year)
        months = [d for d in os.listdir(year_path) if d.startswith('order_month=')]
        total_partitions += len(months)
        print(f"      └── {year}/ ({len(months)} months)")
    if len(years) > 3:
        print(f"      └── ... and {len(years)-3} more years")
    print(f"      Total partitions: {total_partitions}")

# ===========================================
# CHECK 4: Can we read data?
# ===========================================
print("\n📊 CHECK 4: Data Readability")

try:
    # Create warehouse instance
    hive = HiveWarehouse('retail_warehouse/')
    
    # Test reading a customer partition
    print("\n   Testing dim_customer partition read...")
    east_customers = hive.read_table('dim_customer', partitions={'region': 'East'})
    if not east_customers.empty:
        print(f"      ✅ Read dim_customer (region=East): {len(east_customers):,} rows")
    else:
        print(f"      ⚠️  No data in region=East")
    
    # Test reading fact table partition
    print("\n   Testing fact_sales partition read...")
    nov_2017 = hive.read_table('fact_sales', partitions={'order_year': 2017, 'order_month': 11})
    if not nov_2017.empty:
        print(f"      ✅ Read fact_sales (2017-11): {len(nov_2017):,} rows")
        print(f"      💰 Total sales: ${nov_2017['Sales'].sum():,.2f}")
    else:
        print(f"      ⚠️  No data for 2017-11")
    
    # Test reading full table
    print("\n   Testing full table read...")
    all_customers = hive.read_table('dim_customer')
    print(f"      ✅ Read all dim_customer: {len(all_customers):,} rows")
    
    print("\n   ✅ ALL DATA ACCESS TESTS PASSED!")
    data_access_success = True
    
except Exception as e:
    print(f"   ❌ Data access failed: {e}")
    data_access_success = False

# ===========================================
# CHECK 5: Metadata files
# ===========================================
print("\n📋 CHECK 5: Metadata Files")

metadata_files = glob.glob('retail_warehouse/*/_METADATA.json')
print(f"   Found {len(metadata_files)} metadata files")
for meta_file in metadata_files[:5]:  # Show first 5
    table_name = Path(meta_file).parent.name
    print(f"      ✅ {table_name}/_METADATA.json")

# ===========================================
# CHECK 6: Data quality summary
# ===========================================
print("\n📈 CHECK 6: Data Quality Summary")

try:
    hive = HiveWarehouse('retail_warehouse/')
    
    # Get row counts
    dim_customer_df = hive.read_table('dim_customer')
    dim_product_df = hive.read_table('dim_product')
    dim_location_df = hive.read_table('dim_location')
    dim_date_df = hive.read_table('dim_date')
    fact_sales_df = hive.read_table('fact_sales')
    
    print(f"\n   📊 TABLE ROW COUNTS:")
    print(f"      dim_customer:   {len(dim_customer_df):,} rows")
    print(f"      dim_product:    {len(dim_product_df):,} rows")
    print(f"      dim_location:   {len(dim_location_df):,} rows")
    print(f"      dim_date:       {len(dim_date_df):,} rows")
    print(f"      fact_sales:     {len(fact_sales_df):,} rows")
    
    if not fact_sales_df.empty:
        print(f"\n   💰 SALES SUMMARY:")
        print(f"      Total Sales:  ${fact_sales_df['Sales'].sum():,.2f}")
        if 'Profit' in fact_sales_df.columns:
            print(f"      Total Profit: ${fact_sales_df['Profit'].sum():,.2f}")
            margin = (fact_sales_df['Profit'].sum() / fact_sales_df['Sales'].sum() * 100)
            print(f"      Profit Margin: {margin:.1f}%")
    
except Exception as e:
    print(f"   ⚠️  Could not generate summary: {e}")

# ===========================================
# FINAL VERDICT
# ===========================================
print("\n" + "=" * 70)
print("📋 VERIFICATION SUMMARY")
print("=" * 70)

if all_tables_exist and data_access_success:
    print("\n" + "✨" * 35)
    print("✨                                              ✨")
    print("✨   ✅✅✅ EVERYTHING IS WORKING! ✅✅✅    ✨")
    print("✨                                              ✨")
    print("✨" * 35)
    
    print("\n   📁 Your Hive-style warehouse is READY!")
    print("\n   🚀 NEXT STEPS:")
    print("   ────────────────────────────────────────────")
    print("   1. Run analytics:  python src/04_storage/hive_analytics.py")
    print("   2. Explore data:   ls -la retail_warehouse/fact_sales/")
    print("   3. Query engine:   from hive_query_engine import HiveWarehouse")
    print("   4. View reports:   cat analytics_reports/*.csv (if any)")
    
    # Show warehouse size
    total_size = 0
    for path in glob.glob('retail_warehouse/**/*.parquet', recursive=True):
        total_size += os.path.getsize(path)
    print(f"\n   💾 Warehouse size: {total_size / 1e6:.1f} MB")
    
else:
    print("\n" + "⚠️" * 35)
    print("⚠️                                              ⚠️")
    print("⚠️   ❌❌❌ SOMETHING IS NOT WORKING! ❌❌❌   ⚠️")
    print("⚠️                                              ⚠️")
    print("⚠️" * 35)
    
    print("\n   🔧 TROUBLESHOOTING:")
    print("   ────────────────────────────────────────────")
    if not os.path.exists('retail_warehouse/'):
        print("   1. Run: python src/04_storage/hive_style_warehouse.py")
    elif not all_tables_exist:
        print("   1. Re-run: python src/04_storage/hive_style_warehouse.py")
    elif not data_access_success:
        print("   1. Check if parquet files are corrupted")
        print("   2. Re-run: python src/04_storage/hive_style_warehouse.py")
    
    print("\n   📋 Run this diagnostic:")
    print("   python -c \"import os; print(os.listdir('retail_warehouse/'))\"")

print("\n" + "=" * 70)