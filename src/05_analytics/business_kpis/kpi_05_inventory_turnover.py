# src/05_analytics/business_kpis/kpi_05_inventory_turnover.py
"""
KPI 05: INVENTORY TURNOVER RATIO
Time: 45 minutes (MOST COMPLEX)
Purpose: Calculate how efficiently inventory is being sold and replaced
Formula: Inventory Turnover = COGS / Average Inventory
Output: Gauge chart or KPI card for dashboard
Dashboard Page: Page 2 - Inventory
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

# Paths
WAREHOUSE_DIR = 'data/warehouse/'
OUTPUT_DIR = 'data/outputs/kpi_exports/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 70)
print("KPI 05: INVENTORY TURNOVER RATIO")
print("=" * 70)

# Load required tables
print("\n📥 Loading data...")
fact_sales = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'fact_sales.parquet'))
dim_product = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_product.parquet'))
dim_date = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_date.parquet'))

print(f"   ✅ Loaded {len(fact_sales):,} sales records")
print(f"   ✅ Loaded {len(dim_product):,} products")

# Step 1: Calculate COGS (Cost of Goods Sold)
print("\n💰 Step 1: Calculating COGS...")
COGS_PERCENTAGE = 0.60  # 60% assumption for retail

sales_with_products = fact_sales.merge(dim_product, on='product_sk', how='left')

product_cogs = sales_with_products.groupby('product_sk').agg({
    'Sales': 'sum',
    'Quantity': 'sum',
    'Product_Name': 'first',
    'Category': 'first',
    'Sub_Category': 'first'
}).reset_index()

product_cogs['COGS'] = product_cogs['Sales'] * COGS_PERCENTAGE

print(f"   ✅ Calculated COGS for {len(product_cogs)} products")
print(f"   💵 Total COGS: ${product_cogs['COGS'].sum():,.2f}")

# Step 2: Calculate Average Inventory
print("\n📦 Step 2: Calculating Average Inventory...")

# Find date column
sales_with_date = fact_sales.merge(dim_date, on='date_sk', how='left')
date_column = None
for col in sales_with_date.columns:
    if 'date' in col.lower() and 'sk' not in col.lower():
        if pd.api.types.is_datetime64_any_dtype(sales_with_date[col]):
            date_column = col
            break

if date_column:
    min_date = sales_with_date[date_column].min()
    max_date = sales_with_date[date_column].max()
    days_in_period = (max_date - min_date).days
    print(f"   📅 Analysis period: {min_date} to {max_date} ({days_in_period} days)")
else:
    print(f"   ⚠️  Could not find date column, using simplified calculation")

INVENTORY_MULTIPLIER = 0.25  # Average inventory = 25% of total sold

product_cogs['Avg_Inventory_Units'] = product_cogs['Quantity'] * INVENTORY_MULTIPLIER
product_cogs['Avg_Inventory_Value'] = product_cogs['COGS'] * INVENTORY_MULTIPLIER

print(f"   ✅ Calculated average inventory levels")

# Step 3: Calculate Inventory Turnover Ratio
print("\n📊 Step 3: Calculating Inventory Turnover Ratio...")
product_cogs['Inventory_Turnover_Ratio'] = np.where(
    product_cogs['Avg_Inventory_Value'] > 0,
    product_cogs['COGS'] / product_cogs['Avg_Inventory_Value'],
    0
)

product_cogs['Days_Inventory_Outstanding'] = np.where(
    product_cogs['Inventory_Turnover_Ratio'] > 0,
    365 / product_cogs['Inventory_Turnover_Ratio'],
    365
)

print(f"   ✅ Calculated turnover ratios")

# Overall Company Metrics
print("\n" + "=" * 70)
print("📊 INVENTORY TURNOVER SUMMARY")
print("=" * 70)

overall_cogs = product_cogs['COGS'].sum()
overall_avg_inventory = product_cogs['Avg_Inventory_Value'].sum()
overall_turnover = overall_cogs / overall_avg_inventory if overall_avg_inventory > 0 else 0
overall_dio = 365 / overall_turnover if overall_turnover > 0 else 365

print(f"\n🏢 COMPANY-WIDE METRICS:")
print(f"   Total COGS: ${overall_cogs:,.2f}")
print(f"   Average Inventory Value: ${overall_avg_inventory:,.2f}")
print(f"   📈 Overall Turnover Ratio: {overall_turnover:.2f}x")
print(f"   ⏱️  Days Inventory Outstanding: {overall_dio:.1f} days")

print(f"\n💡 INTERPRETATION:")
if overall_turnover > 8:
    print(f"   ✅ EXCELLENT - Inventory turning {overall_turnover:.1f}x per year")
    print(f"   💪 Stock sells quickly, minimal holding costs")
elif overall_turnover > 5:
    print(f"   ✅ GOOD - Healthy turnover of {overall_turnover:.1f}x per year")
elif overall_turnover > 3:
    print(f"   ⚠️  AVERAGE - Turnover of {overall_turnover:.1f}x could be improved")
else:
    print(f"   ⚠️  CONCERN - Low turnover {overall_turnover:.1f}x may indicate overstocking")

# Category Analysis
print(f"\n📂 TURNOVER BY CATEGORY:")
category_turnover = product_cogs.groupby('Category').agg({
    'COGS': 'sum',
    'Avg_Inventory_Value': 'sum',
    'Sales': 'sum'
}).reset_index()

category_turnover['Turnover_Ratio'] = category_turnover['COGS'] / category_turnover['Avg_Inventory_Value']
category_turnover['DIO_Days'] = 365 / category_turnover['Turnover_Ratio']
category_turnover = category_turnover.sort_values('Turnover_Ratio', ascending=False)

for idx, row in category_turnover.iterrows():
    print(f"   {row['Category']}:")
    print(f"      Turnover: {row['Turnover_Ratio']:.2f}x | DIO: {row['DIO_Days']:.0f} days | Sales: ${row['Sales']:,.0f}")

# Fast Movers
print(f"\n🚀 TOP 10 FAST-MOVING PRODUCTS (High Turnover):")
fast_movers = product_cogs.nlargest(10, 'Inventory_Turnover_Ratio')[
    ['Product_Name', 'Category', 'Inventory_Turnover_Ratio', 'Days_Inventory_Outstanding', 'Sales']
]
for idx, row in fast_movers.iterrows():
    name = row['Product_Name'][:40] if pd.notna(row['Product_Name']) else 'Unknown'
    print(f"   {name} ({row['Category']})")
    print(f"      Turnover: {row['Inventory_Turnover_Ratio']:.1f}x | DIO: {row['Days_Inventory_Outstanding']:.0f} days | Sales: ${row['Sales']:,.0f}")

# Slow Movers
print(f"\n🐌 TOP 10 SLOW-MOVING PRODUCTS (Low Turnover - Risk):")
slow_movers = product_cogs.nsmallest(10, 'Inventory_Turnover_Ratio')[
    ['Product_Name', 'Category', 'Inventory_Turnover_Ratio', 'Days_Inventory_Outstanding', 'Sales']
]
for idx, row in slow_movers.iterrows():
    name = row['Product_Name'][:40] if pd.notna(row['Product_Name']) else 'Unknown'
    print(f"   {name} ({row['Category']})")
    print(f"      Turnover: {row['Inventory_Turnover_Ratio']:.1f}x | DIO: {row['Days_Inventory_Outstanding']:.0f} days | Sales: ${row['Sales']:,.0f}")

# Save outputs
print(f"\n💾 Saving KPI exports...")

summary_data = {
    'metric': ['Overall Turnover Ratio', 'Days Inventory Outstanding', 'Total COGS', 'Avg Inventory Value'],
    'value': [overall_turnover, overall_dio, overall_cogs, overall_avg_inventory]
}
pd.DataFrame(summary_data).to_csv(os.path.join(OUTPUT_DIR, 'inventory_turnover_summary.csv'), index=False)

category_turnover[['Category', 'Turnover_Ratio', 'DIO_Days', 'Sales']].to_csv(
    os.path.join(OUTPUT_DIR, 'inventory_turnover_by_category.csv'), index=False
)

product_cogs[['product_sk', 'Product_Name', 'Category', 'Sub_Category',
              'Inventory_Turnover_Ratio', 'Days_Inventory_Outstanding', 
              'Sales', 'COGS', 'Quantity']].to_csv(
    os.path.join(OUTPUT_DIR, 'inventory_turnover_by_product.csv'), index=False
)

fast_movers.to_csv(os.path.join(OUTPUT_DIR, 'fast_moving_products.csv'), index=False)
slow_movers.to_csv(os.path.join(OUTPUT_DIR, 'slow_moving_products.csv'), index=False)

print(f"   ✅ Saved: inventory_turnover_summary.csv")
print(f"   ✅ Saved: inventory_turnover_by_category.csv")
print(f"   ✅ Saved: inventory_turnover_by_product.csv")
print(f"   ✅ Saved: fast_moving_products.csv")
print(f"   ✅ Saved: slow_moving_products.csv")

print("\n" + "=" * 70)
print("✅ KPI 05 CALCULATION COMPLETE")
print("=" * 70)
print("\n📊 Ready for dashboard: Gauge Chart or KPI Card")
print("📄 Dashboard: Page 2 - Inventory")
print(f"\n🎯 KEY INSIGHT: Company turnover ratio is {overall_turnover:.2f}x")
print(f"   (Industry average for retail: 5-10x)")