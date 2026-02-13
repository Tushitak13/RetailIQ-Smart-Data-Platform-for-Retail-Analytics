# src/05_analytics/business_kpis/kpi_01_daily_revenue.py
"""
KPI 01: DAILY REVENUE
Time: 20 minutes
Purpose: Calculate total sales revenue for each day
Output: Line chart data for executive dashboard
"""

import pandas as pd
import os
from datetime import datetime

# Paths
WAREHOUSE_DIR = 'data/warehouse/'
OUTPUT_DIR = 'data/outputs/kpi_exports/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 70)
print("KPI 01: DAILY REVENUE CALCULATION")
print("=" * 70)

# Load required tables
print("\n📥 Loading data...")
fact_sales = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'fact_sales.parquet'))
dim_date = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_date.parquet'))

print(f"   ✅ Loaded {len(fact_sales):,} sales records")
print(f"   ✅ Loaded {len(dim_date):,} date records")

# Debug: Print column names
print("\n🔍 Checking column names...")
print(f"   dim_date columns: {dim_date.columns.tolist()}")

# Find the date column (it might be 'Date', 'date', 'order_date', etc.)
date_column = None
for col in dim_date.columns:
    if 'date' in col.lower() and 'sk' not in col.lower():
        date_column = col
        break

if date_column is None:
    # If no date column found, look for columns with dates
    for col in dim_date.columns:
        if pd.api.types.is_datetime64_any_dtype(dim_date[col]):
            date_column = col
            break

if date_column is None:
    print("\n❌ ERROR: Could not find a date column in dim_date")
    print(f"   Available columns: {dim_date.columns.tolist()}")
    print("\n   Please check your dim_date table structure")
    exit(1)

print(f"   ✅ Using date column: '{date_column}'")

# Merge sales with date dimension
print("\n🔗 Joining sales with date dimension...")
sales_with_date = fact_sales.merge(dim_date, on='date_sk', how='left')

# Calculate daily revenue
print("\n💰 Calculating daily revenue...")
daily_revenue = sales_with_date.groupby(date_column).agg({
    'Sales': 'sum',
    'Quantity': 'sum',
    'Profit': 'sum'
}).reset_index()

# Rename the date column to 'Date' for consistency
daily_revenue = daily_revenue.rename(columns={date_column: 'Date'})

# Ensure Date is datetime type
if not pd.api.types.is_datetime64_any_dtype(daily_revenue['Date']):
    daily_revenue['Date'] = pd.to_datetime(daily_revenue['Date'])

# Sort by date
daily_revenue = daily_revenue.sort_values('Date')

# Add additional metrics
daily_revenue['Profit_Margin'] = (daily_revenue['Profit'] / daily_revenue['Sales'] * 100).round(2)
daily_revenue['Revenue'] = daily_revenue['Sales']  # Rename for clarity

# Format currency columns
daily_revenue['Revenue_Formatted'] = daily_revenue['Revenue'].apply(lambda x: f"${x:,.2f}")
daily_revenue['Profit_Formatted'] = daily_revenue['Profit'].apply(lambda x: f"${x:,.2f}")

print(f"   ✅ Calculated revenue for {len(daily_revenue)} days")

# Summary statistics
print("\n" + "=" * 70)
print("📊 DAILY REVENUE SUMMARY")
print("=" * 70)

print(f"\n📅 Date Range:")
print(f"   From: {daily_revenue['Date'].min()}")
print(f"   To: {daily_revenue['Date'].max()}")

print(f"\n💰 Revenue Statistics:")
print(f"   Total Revenue: ${daily_revenue['Revenue'].sum():,.2f}")
print(f"   Average Daily Revenue: ${daily_revenue['Revenue'].mean():,.2f}")
print(f"   Median Daily Revenue: ${daily_revenue['Revenue'].median():,.2f}")
print(f"   Highest Day: ${daily_revenue['Revenue'].max():,.2f}")
print(f"   Lowest Day: ${daily_revenue['Revenue'].min():,.2f}")

print(f"\n📦 Volume Statistics:")
print(f"   Total Units Sold: {daily_revenue['Quantity'].sum():,.0f}")
print(f"   Average Daily Units: {daily_revenue['Quantity'].mean():,.0f}")

print(f"\n💵 Profitability:")
print(f"   Total Profit: ${daily_revenue['Profit'].sum():,.2f}")
print(f"   Average Profit Margin: {daily_revenue['Profit_Margin'].mean():.2f}%")

# Top 5 best days
print(f"\n🏆 Top 5 Revenue Days:")
top_5 = daily_revenue.nlargest(5, 'Revenue')[['Date', 'Revenue_Formatted', 'Quantity', 'Profit_Margin']]
for idx, row in top_5.iterrows():
    print(f"   {row['Date'].date()}: {row['Revenue_Formatted']} ({row['Quantity']:.0f} units, {row['Profit_Margin']:.1f}% margin)")

# Save to CSV for dashboard
output_path = os.path.join(OUTPUT_DIR, 'daily_revenue.csv')
daily_revenue[['Date', 'Revenue', 'Quantity', 'Profit', 'Profit_Margin']].to_csv(output_path, index=False)

print(f"\n💾 Saved KPI export: {output_path}")

# Also save a simplified version for quick dashboard loading
simple_output = daily_revenue[['Date', 'Revenue']].copy()
simple_output['Date'] = simple_output['Date'].astype(str)
simple_output.to_csv(os.path.join(OUTPUT_DIR, 'daily_revenue_simple.csv'), index=False)

print(f"💾 Saved simplified version: {OUTPUT_DIR}daily_revenue_simple.csv")

print("\n" + "=" * 70)
print("✅ KPI 01 CALCULATION COMPLETE")
print("=" * 70)
print("\n📊 Ready for dashboard visualization (Line Chart 📈)")
print("📄 Dashboard: Page 1 - Executive")