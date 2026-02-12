# src/03_transformation/scd_product.py
import pandas as pd
import os
from datetime import datetime

# Paths
WAREHOUSE_DIR = 'data/warehouse/'
HISTORY_DIR = 'data/history/'
os.makedirs(HISTORY_DIR, exist_ok=True)

def implement_product_scd2(current_snapshot, previous_history=None, effective_date=None):
    """
    Implement SCD Type 2 for product dimension
    """
    if effective_date is None:
        effective_date = datetime.now()
    
    current_snapshot = current_snapshot.copy()
    
    if previous_history is None or len(previous_history) == 0:
        # First load
        current_snapshot['Start_Date'] = effective_date
        current_snapshot['End_Date'] = pd.NaT
        current_snapshot['Is_Current'] = 1
        current_snapshot['Version'] = 1
        return current_snapshot
    
    # Similar logic as customer SCD but for product attributes
    active_records = previous_history[previous_history['Is_Current'] == 1].copy()
    
    new_records = []
    updated_records = []
    
    for _, product in current_snapshot.iterrows():
        product_id = product['Product ID']
        active_record = active_records[active_records['Product ID'] == product_id]
        
        if len(active_record) == 0:
            # New product
            product['Start_Date'] = effective_date
            product['End_Date'] = pd.NaT
            product['Is_Current'] = 1
            product['Version'] = 1
            new_records.append(product)
        else:
            active_record = active_record.iloc[0]
            
            # Check for changes in product attributes
            current_attrs = product[['Product Name', 'Category', 'Sub-Category']].to_dict()
            active_attrs = active_record[['Product Name', 'Category', 'Sub-Category']].to_dict()
            
            if current_attrs != active_attrs:
                # Changes detected
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
    
    return final_history.sort_values(['Product ID', 'Start_Date'])

# Execute product SCD
current_products = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_product.parquet'))
product_history_path = os.path.join(HISTORY_DIR, 'dim_product_scd2.parquet')

previous_product_history = None
if os.path.exists(product_history_path):
    previous_product_history = pd.read_parquet(product_history_path)

dim_product_scd2 = implement_product_scd2(current_products, previous_product_history)
dim_product_scd2.to_parquet(product_history_path, index=False)
print(f"✅ Product SCD Type 2 table created: {len(dim_product_scd2):,} rows")