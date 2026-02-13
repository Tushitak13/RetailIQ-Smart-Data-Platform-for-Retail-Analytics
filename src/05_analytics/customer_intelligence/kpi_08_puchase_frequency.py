import pandas as pd
import numpy as np
from datetime import datetime
import os

os.makedirs('data/outputs/kpi_exports', exist_ok=True)

start_time = datetime.now()
print("=" * 70)
print("KPI 8: PURCHASE FREQUENCY")
print("=" * 70)
print(f"\nStart Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

print("\nLOADING DATA...")
fact_sales = pd.read_parquet('data/warehouse/fact_sales.parquet')
dim_customer = pd.read_parquet('data/warehouse/dim_customer.parquet')

print(f"fact_sales loaded - Rows: {len(fact_sales):,}")
print(f"dim_customer loaded - Rows: {len(dim_customer):,}")

print("\nCALCULATING PURCHASE FREQUENCY...")

customer_col = 'customer_sk' if 'customer_sk' in fact_sales.columns else 'customer_id'

purchase_freq = fact_sales.groupby(customer_col).agg({
    'order_id': 'nunique',
    'Sales': 'sum'
}).reset_index()

purchase_freq.columns = [customer_col, 'purchase_frequency', 'total_sales']

purchase_freq['average_order_value'] = purchase_freq['total_sales'] / purchase_freq['purchase_frequency']

purchase_freq = purchase_freq.merge(dim_customer, on=customer_col, how='left')

print(f"\nPurchase Frequency Calculated!")
print(f"Total customers: {len(purchase_freq):,}")
print(f"Average purchases per customer: {purchase_freq['purchase_frequency'].mean():.2f}")
print(f"Median purchases: {purchase_freq['purchase_frequency'].median():.0f}")
print(f"Maximum purchases: {purchase_freq['purchase_frequency'].max():.0f}")

purchase_freq['frequency_segment'] = pd.cut(purchase_freq['purchase_frequency'],
                                            bins=[0, 2, 5, float('inf')],
                                            labels=['Low Frequency', 'Medium Frequency', 'High Frequency'])

print(f"\nFrequency Segment Distribution:")
segment_counts = purchase_freq['frequency_segment'].value_counts()
for segment, count in segment_counts.items():
    percentage = (count / len(purchase_freq)) * 100
    print(f"{segment}: {count:,} customers ({percentage:.1f}%)")

purchase_freq = purchase_freq.sort_values('purchase_frequency', ascending=False)

purchase_freq.to_csv('data/outputs/kpi_exports/purchase_frequency.csv', index=False)

print(f"\nTop 10 Most Frequent Customers:")
top_10 = purchase_freq[['Customer_Name', 'purchase_frequency', 'total_sales', 'average_order_value']].head(10)
print(top_10.to_string(index=False))

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

print(f"\nSaved to: data/outputs/kpi_exports/purchase_frequency.csv")
print(f"\nEnd Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Duration: {duration:.2f} seconds")
print("=" * 70)