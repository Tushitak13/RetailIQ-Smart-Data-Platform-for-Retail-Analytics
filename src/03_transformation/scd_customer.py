import pandas as pd
import os
from datetime import datetime

WAREHOUSE_DIR = 'data/warehouse/'
HISTORY_DIR = 'data/history/'
os.makedirs(HISTORY_DIR, exist_ok=True)

print("=" * 70)
print("SLOWLY CHANGING DIMENSION - TYPE 2 (CUSTOMER)")
print("=" * 70)

dim_customer = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_customer.parquet'))
print(f"\nLoaded current customers: {len(dim_customer):,}")

scd_history_path = os.path.join(HISTORY_DIR, 'dim_customer_scd2.parquet')

if not os.path.exists(scd_history_path):
    print("\nFirst time load - initializing SCD Type 2...")
    
    dim_customer_scd2 = dim_customer.copy()
    dim_customer_scd2['Start_Date'] = datetime.now()
    dim_customer_scd2['End_Date'] = pd.NaT
    dim_customer_scd2['Is_Current'] = 1
    dim_customer_scd2['Version'] = 1
    
    cols = ['customer_sk', 'Customer_ID', 'Customer_Name', 'Segment', 'Region', 
            'Source_System', 'Start_Date', 'End_Date', 'Is_Current', 'Version']
    dim_customer_scd2 = dim_customer_scd2[cols]
    
else:
    print("\nLoading existing SCD history...")
    dim_customer_scd2 = pd.read_parquet(scd_history_path)
    
    active_customers = dim_customer_scd2[dim_customer_scd2['Is_Current'] == 1]
    
    print("\nChecking for customer changes...")
    
    new_records = []
    updated_records = []
    
    for _, customer in dim_customer.iterrows():
        customer_id = customer['Customer_ID']
        active = active_customers[active_customers['Customer_ID'] == customer_id]
        
        if len(active) == 0:
            customer['customer_sk'] = dim_customer_scd2['customer_sk'].max() + 1
            customer['Start_Date'] = datetime.now()
            customer['End_Date'] = pd.NaT
            customer['Is_Current'] = 1
            customer['Version'] = 1
            new_records.append(customer)
            
        else:
            active = active.iloc[0]
            if (customer['Customer_Name'] != active['Customer_Name'] or
                customer['Segment'] != active['Segment'] or
                customer['Region'] != active['Region']):
                
                dim_customer_scd2.loc[dim_customer_scd2['customer_sk'] == active['customer_sk'], 
                                    ['End_Date', 'Is_Current']] = [datetime.now(), 0]
                
                customer['customer_sk'] = dim_customer_scd2['customer_sk'].max() + 1
                customer['Start_Date'] = datetime.now()
                customer['End_Date'] = pd.NaT
                customer['Is_Current'] = 1
                customer['Version'] = active['Version'] + 1
                new_records.append(customer)
    
    if new_records:
        new_df = pd.DataFrame(new_records)
        dim_customer_scd2 = pd.concat([dim_customer_scd2, new_df], ignore_index=True)
    
    print(f"Processed {len(new_records)} changes")

dim_customer_scd2.to_parquet(scd_history_path, index=False)
print(f"\nSaved SCD history: {scd_history_path}")

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
versioned_path = os.path.join(HISTORY_DIR, f'dim_customer_scd2_{timestamp}.parquet')
dim_customer_scd2.to_parquet(versioned_path, index=False)
print(f"Saved versioned backup: {versioned_path}")

print("\n" + "=" * 70)
print("SCD TYPE 2 SUMMARY")
print("=" * 70)

current_count = len(dim_customer_scd2[dim_customer_scd2['Is_Current'] == 1])
historical_count = len(dim_customer_scd2[dim_customer_scd2['Is_Current'] == 0])

print(f"\nCustomer Records:")
print(f"- Current active: {current_count:,}")
print(f"- Historical: {historical_count:,}")
print(f"- Total: {len(dim_customer_scd2):,}")

print(f"\nVersion Distribution:")
version_dist = dim_customer_scd2.groupby('Version').size()
for version, count in version_dist.items():
    print(f"- Version {version}: {count:,} customers")

print("\n" + "=" * 70)
print("SCD TYPE 2 IMPLEMENTATION COMPLETE")
print("=" * 70)