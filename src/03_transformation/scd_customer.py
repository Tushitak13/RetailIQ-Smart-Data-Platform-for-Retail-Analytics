# src/03_transformation/scd_customer.py
import pandas as pd
import os
from datetime import datetime

# Paths
WAREHOUSE_DIR = 'data/warehouse/'
HISTORY_DIR = 'data/history/'
os.makedirs(HISTORY_DIR, exist_ok=True)

print("=" * 70)
print("SLOWLY CHANGING DIMENSION - TYPE 2 (CUSTOMER)")
print("=" * 70)

# Load current customer dimension
dim_customer = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_customer.parquet'))
print(f"\n📥 Loaded current customers: {len(dim_customer):,}")

# Check if SCD history already exists
scd_history_path = os.path.join(HISTORY_DIR, 'dim_customer_scd2.parquet')

if not os.path.exists(scd_history_path):
    # First time - initialize SCD
    print("\n🆕 First time load - initializing SCD Type 2...")
    
    dim_customer_scd2 = dim_customer.copy()
    dim_customer_scd2['Start_Date'] = datetime.now()
    dim_customer_scd2['End_Date'] = pd.NaT
    dim_customer_scd2['Is_Current'] = 1
    dim_customer_scd2['Version'] = 1
    
    # Reorder columns
    cols = ['customer_sk', 'Customer_ID', 'Customer_Name', 'Segment', 'Region', 
            'Source_System', 'Start_Date', 'End_Date', 'Is_Current', 'Version']
    dim_customer_scd2 = dim_customer_scd2[cols]
    
else:
    # Load existing history
    print("\n📚 Loading existing SCD history...")
    dim_customer_scd2 = pd.read_parquet(scd_history_path)
    
    # Get currently active records
    active_customers = dim_customer_scd2[dim_customer_scd2['Is_Current'] == 1]
    
    # Check for changes in current dimension vs active records
    print("\n🔄 Checking for customer changes...")
    
    new_records = []
    updated_records = []
    
    for _, customer in dim_customer.iterrows():
        customer_id = customer['Customer_ID']
        active = active_customers[active_customers['Customer_ID'] == customer_id]
        
        if len(active) == 0:
            # New customer
            customer['customer_sk'] = dim_customer_scd2['customer_sk'].max() + 1
            customer['Start_Date'] = datetime.now()
            customer['End_Date'] = pd.NaT
            customer['Is_Current'] = 1
            customer['Version'] = 1
            new_records.append(customer)
            
        else:
            active = active.iloc[0]
            # Check if attributes changed
            if (customer['Customer_Name'] != active['Customer_Name'] or
                customer['Segment'] != active['Segment'] or
                customer['Region'] != active['Region']):
                
                # Expire old record
                dim_customer_scd2.loc[dim_customer_scd2['customer_sk'] == active['customer_sk'], 
                                    ['End_Date', 'Is_Current']] = [datetime.now(), 0]
                
                # Create new version
                customer['customer_sk'] = dim_customer_scd2['customer_sk'].max() + 1
                customer['Start_Date'] = datetime.now()
                customer['End_Date'] = pd.NaT
                customer['Is_Current'] = 1
                customer['Version'] = active['Version'] + 1
                new_records.append(customer)
    
    # Add new records to history
    if new_records:
        new_df = pd.DataFrame(new_records)
        dim_customer_scd2 = pd.concat([dim_customer_scd2, new_df], ignore_index=True)
    
    print(f"   ✅ Processed {len(new_records)} changes")

# Save SCD table
dim_customer_scd2.to_parquet(scd_history_path, index=False)
print(f"\n💾 Saved SCD history: {scd_history_path}")

# Save with timestamp for versioning
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
versioned_path = os.path.join(HISTORY_DIR, f'dim_customer_scd2_{timestamp}.parquet')
dim_customer_scd2.to_parquet(versioned_path, index=False)
print(f"💾 Saved versioned backup: {versioned_path}")

# Summary
print("\n" + "=" * 70)
print("📋 SCD TYPE 2 SUMMARY")
print("=" * 70)

current_count = len(dim_customer_scd2[dim_customer_scd2['Is_Current'] == 1])
historical_count = len(dim_customer_scd2[dim_customer_scd2['Is_Current'] == 0])

print(f"\n🏷️  Customer Records:")
print(f"   - Current active: {current_count:,}")
print(f"   - Historical: {historical_count:,}")
print(f"   - Total: {len(dim_customer_scd2):,}")

print(f"\n📊 Version Distribution:")
version_dist = dim_customer_scd2.groupby('Version').size()
for version, count in version_dist.items():
    print(f"   - Version {version}: {count:,} customers")

print("\n" + "=" * 70)
print("✅ SCD TYPE 2 IMPLEMENTATION COMPLETE")
print("=" * 70)