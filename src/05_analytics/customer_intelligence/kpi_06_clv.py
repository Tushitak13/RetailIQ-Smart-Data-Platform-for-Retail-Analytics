import pandas as pd
import numpy as np
from datetime import datetime
import os

os.makedirs('data/outputs/kpi_exports', exist_ok=True)

start_time = datetime.now()
print("=" * 70)
print("KPI 6: CUSTOMER LIFETIME VALUE (CLV)")
print("=" * 70)
print(f"\nStart Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

fact_sales = pd.read_parquet('data/warehouse/fact_sales.parquet')
dim_customer = pd.read_parquet('data/warehouse/dim_customer.parquet')

print(f"\nLoaded {len(fact_sales):,} sales records")
print(f"Loaded {len(dim_customer):,} customers")

clv_df = fact_sales.groupby('customer_sk').agg({
    'Sales': 'sum',
    'order_id': 'nunique',
    'Quantity': 'sum'
}).reset_index()

clv_df.columns = ['customer_sk', 'customer_lifetime_value', 'total_orders', 'total_quantity']

clv_df['average_order_value'] = clv_df['customer_lifetime_value'] / clv_df['total_orders']

clv_df = clv_df.merge(dim_customer, on='customer_sk', how='left')

clv_df['clv_segment'] = pd.qcut(clv_df['customer_lifetime_value'], 
                                  q=[0, 0.2, 0.8, 1.0], 
                                  labels=['Low Value', 'Medium Value', 'High Value'])

clv_df = clv_df.sort_values('customer_lifetime_value', ascending=False)

clv_df.to_csv('data/outputs/kpi_exports/customer_lifetime_value.csv', index=False)

print(f"\nCLV Summary:")
print(f"Total Customers: {len(clv_df):,}")
print(f"Average CLV: ${clv_df['customer_lifetime_value'].mean():,.2f}")
print(f"Median CLV: ${clv_df['customer_lifetime_value'].median():,.2f}")
print(f"Highest CLV: ${clv_df['customer_lifetime_value'].max():,.2f}")
print(f"Lowest CLV: ${clv_df['customer_lifetime_value'].min():,.2f}")

print(f"\nSegment Distribution:")
segment_counts = clv_df['clv_segment'].value_counts()
for segment, count in segment_counts.items():
    percentage = (count / len(clv_df)) * 100
    avg_value = clv_df[clv_df['clv_segment'] == segment]['customer_lifetime_value'].mean()
    print(f"{segment}: {count:,} customers ({percentage:.1f}%) - Avg: ${avg_value:,.2f}")

print(f"\nTop 10 Customers by CLV:")
top_10 = clv_df[['Customer_Name', 'customer_lifetime_value', 'total_orders', 'Segment']].head(10)
print(top_10.to_string(index=False))

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

print(f"\nSaved to: data/outputs/kpi_exports/customer_lifetime_value.csv")
print(f"\nEnd Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Duration: {duration:.2f} seconds")
print("=" * 70)