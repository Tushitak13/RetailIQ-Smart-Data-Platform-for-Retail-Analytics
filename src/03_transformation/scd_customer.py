import pandas as pd
import os
from datetime import datetime

WAREHOUSE_DIR = 'data/warehouse/'
os.makedirs(WAREHOUSE_DIR, exist_ok=True)

dim_customer = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_customer.parquet'))

# Sort by Customer ID (to track history)
dim_customer = dim_customer.sort_values(['Customer ID'])

# Initialize SCD Type 2 columns
dim_customer['Start_Date'] = datetime(2014,1,1)
dim_customer['End_Date'] = pd.NaT
dim_customer['Is_Current'] = 1

# If you have historical changes, logic would go here
# For demo, we assume current snapshot is the only version

dim_customer_scd2 = dim_customer[['Customer ID', 'Customer Name', 'Segment', 'Region', 'Start_Date', 'End_Date', 'Is_Current']]

# Save
dim_customer_scd2.to_parquet(os.path.join(WAREHOUSE_DIR, 'dim_customer_scd2.parquet'), index=False)
print(f"Customer SCD Type 2 table created: {dim_customer_scd2.shape[0]} rows")
