import pandas as pd
import os
from datetime import datetime

WAREHOUSE_DIR = 'data/warehouse/'
HISTORY_DIR = 'data/history/'
os.makedirs(HISTORY_DIR, exist_ok=True)

print("=" * 70)
print("SLOWLY CHANGING DIMENSION - TYPE 2 (PRODUCT)")
print("=" * 70)

def implement_product_scd2(current_snapshot, previous_history=None, effective_date=None):
    if effective_date is None:
        effective_date = datetime.now()
    
    current_snapshot = current_snapshot.copy()
    
    if previous_history is None or len(previous_history) == 0:
        print("\nFirst time load - initializing SCD Type 2...")
        current_snapshot['Start_Date'] = effective_date
        current_snapshot['End_Date'] = pd.NaT
        current_snapshot['Is_Current'] = 1
        current_snapshot['Version'] = 1
        return current_snapshot
    
    print("\nLoading existing SCD history...")
    active_records = previous_history[previous_history['Is_Current'] == 1].copy()
    
    print("\nChecking for product changes...")
    
    new_records = []
    updated_records = []
    
    for _, product in current_snapshot.iterrows():
        product_id = product['Product_ID']
        active_record = active_records[active_records['Product_ID'] == product_id]
        
        if len(active_record) == 0:
            product['Start_Date'] = effective_date
            product['End_Date'] = pd.NaT
            product['Is_Current'] = 1
            product['Version'] = 1
            new_records.append(product)
        else:
            active_record = active_record.iloc[0]
            
            current_attrs = product[['Product_Name', 'Category', 'Sub_Category']].to_dict()
            active_attrs = active_record[['Product_Name', 'Category', 'Sub_Category']].to_dict()
            
            if current_attrs != active_attrs:
                active_record['End_Date'] = effective_date
                active_record['Is_Current'] = 0
                updated_records.append(active_record)
                
                product['Start_Date'] = effective_date
                product['End_Date'] = pd.NaT
                product['Is_Current'] = 1
                product['Version'] = active_record['Version'] + 1
                new_records.append(product)
            else:
                updated_records.append(active_record)
    
    unchanged_records = previous_history[previous_history['Is_Current'] == 0]
    
    final_history = pd.concat([
        unchanged_records,
        pd.DataFrame(updated_records),
        pd.DataFrame(new_records)
    ], ignore_index=True)
    
    print(f"Processed {len(new_records)} changes")
    
    return final_history.sort_values(['Product_ID', 'Start_Date'])

current_products = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_product.parquet'))
print(f"\nLoaded current products: {len(current_products):,}")

product_history_path = os.path.join(HISTORY_DIR, 'dim_product_scd2.parquet')

previous_product_history = None
if os.path.exists(product_history_path):
    previous_product_history = pd.read_parquet(product_history_path)

dim_product_scd2 = implement_product_scd2(current_products, previous_product_history)
dim_product_scd2.to_parquet(product_history_path, index=False)
print(f"\nSaved SCD history: {product_history_path}")

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
versioned_path = os.path.join(HISTORY_DIR, f'dim_product_scd2_{timestamp}.parquet')
dim_product_scd2.to_parquet(versioned_path, index=False)
print(f"Saved versioned backup: {versioned_path}")

print("\n" + "=" * 70)
print("SCD TYPE 2 SUMMARY")
print("=" * 70)

current_count = len(dim_product_scd2[dim_product_scd2['Is_Current'] == 1])
historical_count = len(dim_product_scd2[dim_product_scd2['Is_Current'] == 0])

print(f"\nProduct Records:")
print(f"- Current active: {current_count:,}")
print(f"- Historical: {historical_count:,}")
print(f"- Total: {len(dim_product_scd2):,}")

print(f"\nVersion Distribution:")
version_dist = dim_product_scd2.groupby('Version').size()
for version, count in version_dist.items():
    print(f"- Version {version}: {count:,} products")

print("\n" + "=" * 70)
print("SCD TYPE 2 IMPLEMENTATION COMPLETE")
print("=" * 70)




