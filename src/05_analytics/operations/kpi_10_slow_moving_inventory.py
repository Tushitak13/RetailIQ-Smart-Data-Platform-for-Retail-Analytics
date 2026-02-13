import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

os.makedirs('data/outputs/kpi_exports', exist_ok=True)

start_time = datetime.now()
print("=" * 70)
print("KPI 10: SLOW-MOVING INVENTORY")
print("=" * 70)
print(f"\nStart Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

print("\nLOADING DATA...")
fact_sales = pd.read_parquet('data/warehouse/fact_sales.parquet')
dim_product = pd.read_parquet('data/warehouse/dim_product.parquet')
dim_date = pd.read_parquet('data/warehouse/dim_date.parquet')

print(f"fact_sales loaded - Rows: {len(fact_sales):,}")
print(f"dim_product loaded - Rows: {len(dim_product):,}")

fact_sales = fact_sales.merge(dim_date, on='date_sk')
fact_sales['order_date'] = pd.to_datetime(fact_sales['date'])

SLOW_MOVING_DAYS = 90
latest_date = fact_sales['order_date'].max()
cutoff_date = latest_date - timedelta(days=SLOW_MOVING_DAYS)

print(f"\nAnalysis Parameters:")
print(f"Latest date in data: {latest_date.strftime('%Y-%m-%d')}")
print(f"Slow-moving threshold: {SLOW_MOVING_DAYS} days")
print(f"Cutoff date: {cutoff_date.strftime('%Y-%m-%d')}")

product_col = 'product_sk' if 'product_sk' in fact_sales.columns else 'product_id'

last_movement = fact_sales.groupby(product_col)['order_date'].max().reset_index()
last_movement.columns = [product_col, 'last_movement_date']

last_movement['days_since_movement'] = (latest_date - last_movement['last_movement_date']).dt.days
last_movement['is_slow_moving'] = last_movement['days_since_movement'] > SLOW_MOVING_DAYS

slow_inventory = last_movement.merge(dim_product, on=product_col, how='left')

total_products = len(slow_inventory)
slow_moving_count = slow_inventory['is_slow_moving'].sum()
slow_moving_pct = (slow_moving_count / total_products) * 100

print(f"\nSLOW-MOVING INVENTORY ANALYSIS:")
print(f"Total products: {total_products:,}")
print(f"Slow-moving products: {slow_moving_count:,}")
print(f"Percentage slow moving: {slow_moving_pct:.2f}%")

if slow_moving_count > 0:
    slow_only = slow_inventory[slow_inventory['is_slow_moving'] == True].copy()
    slow_only = slow_only.sort_values('days_since_movement', ascending=False)
    
    print(f"\nTop 10 Slowest Moving Products:")
    top_10 = slow_only[['Product_Name', 'Category', 'days_since_movement', 'last_movement_date']].head(10)
    print(top_10.to_string(index=False))
    
    print(f"\nSlow-Moving Inventory by Category:")
    category_breakdown = slow_only.groupby('Category').size().reset_index(name='count')
    category_breakdown = category_breakdown.sort_values('count', ascending=False)
    print(category_breakdown.to_string(index=False))

slow_inventory.to_csv('data/outputs/kpi_exports/slow_moving_inventory.csv', index=False)

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

print(f"\nSaved to: data/outputs/kpi_exports/slow_moving_inventory.csv")
print(f"\nEnd Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Duration: {duration:.2f} seconds")
print("=" * 70)