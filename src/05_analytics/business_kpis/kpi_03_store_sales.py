# src/05_analytics/business_kpis/kpi_03_store_sales.py
"""
KPI 03: STORE-WISE SALES (CITY-WISE)
Time: 20 minutes
Purpose: Calculate total sales for each store/city location
Output: Horizontal bar chart for executive dashboard
Dashboard Page: Page 1 - Executive
"""

import pandas as pd
import os
from datetime import datetime

# Paths
WAREHOUSE_DIR = 'data/warehouse/'
OUTPUT_DIR = 'data/outputs/kpi_exports/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 70)
print("KPI 03: STORE-WISE SALES (CITY-WISE)")
print("=" * 70)

# Load required tables
print("\n📥 Loading data...")
fact_sales = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'fact_sales.parquet'))
dim_location = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_location.parquet'))

print(f"   ✅ Loaded {len(fact_sales):,} sales records")
print(f"   ✅ Loaded {len(dim_location):,} location records")

# Debug: Print location columns
print("\n🔍 Checking location columns...")
print(f"   dim_location columns: {dim_location.columns.tolist()}")

# Find the location/city column
location_cols = []
for col in dim_location.columns:
    if col.lower() in ['city', 'location', 'store', 'store_name', 'city_name']:
        location_cols.append(col)

if not location_cols:
    # Try to find any column that might contain location info
    for col in dim_location.columns:
        if 'city' in col.lower() or 'location' in col.lower() or 'store' in col.lower():
            location_cols.append(col)

if not location_cols:
    print("\n❌ ERROR: Could not find a location/city column")
    print(f"   Available columns: {dim_location.columns.tolist()}")
    print("\n   Defaulting to first non-key column...")
    # Use first column that's not a surrogate key
    for col in dim_location.columns:
        if 'sk' not in col.lower() and 'id' not in col.lower():
            location_cols = [col]
            break

location_column = location_cols[0]
print(f"   ✅ Using location column: '{location_column}'")

# Merge sales with location dimension
print("\n🔗 Joining sales with location dimension...")
sales_with_location = fact_sales.merge(dim_location, on='location_sk', how='left')

print(f"   ✅ Joined {len(sales_with_location):,} records")

# Calculate store/city-wise sales
print("\n💰 Calculating sales by location...")
store_sales = sales_with_location.groupby(location_column).agg({
    'Sales': 'sum',
    'Quantity': 'sum',
    'Profit': 'sum',
    'location_sk': 'count'  # Number of orders
}).reset_index()

# Rename columns for clarity
store_sales = store_sales.rename(columns={
    'location_sk': 'Order_Count',
    location_column: 'Location'
})

# Calculate additional metrics
store_sales['Profit_Margin'] = (store_sales['Profit'] / store_sales['Sales'] * 100).round(2)
store_sales['Avg_Order_Value'] = (store_sales['Sales'] / store_sales['Order_Count']).round(2)
store_sales['Revenue'] = store_sales['Sales']

# Sort by revenue (descending)
store_sales = store_sales.sort_values('Revenue', ascending=False)

# Add rank
store_sales['Rank'] = range(1, len(store_sales) + 1)

# Format currency columns
store_sales['Revenue_Formatted'] = store_sales['Revenue'].apply(lambda x: f"${x:,.2f}")
store_sales['Profit_Formatted'] = store_sales['Profit'].apply(lambda x: f"${x:,.2f}")
store_sales['AOV_Formatted'] = store_sales['Avg_Order_Value'].apply(lambda x: f"${x:,.2f}")

print(f"   ✅ Calculated sales for {len(store_sales)} locations")

# Summary statistics
print("\n" + "=" * 70)
print("📊 STORE/CITY-WISE SALES SUMMARY")
print("=" * 70)

print(f"\n🏪 Location Statistics:")
print(f"   Total Locations: {len(store_sales)}")
print(f"   Total Revenue: ${store_sales['Revenue'].sum():,.2f}")
print(f"   Total Orders: {store_sales['Order_Count'].sum():,}")

print(f"\n💰 Revenue Distribution:")
print(f"   Highest Location: ${store_sales['Revenue'].max():,.2f}")
print(f"   Lowest Location: ${store_sales['Revenue'].min():,.2f}")
print(f"   Average per Location: ${store_sales['Revenue'].mean():,.2f}")
print(f"   Median per Location: ${store_sales['Revenue'].median():,.2f}")

print(f"\n📦 Volume Statistics:")
print(f"   Total Units Sold: {store_sales['Quantity'].sum():,.0f}")
print(f"   Average Units per Location: {store_sales['Quantity'].mean():,.0f}")

print(f"\n💵 Profitability:")
print(f"   Total Profit: ${store_sales['Profit'].sum():,.2f}")
print(f"   Average Profit Margin: {store_sales['Profit_Margin'].mean():.2f}%")
print(f"   Average Order Value: ${store_sales['Avg_Order_Value'].mean():.2f}")

# Top 10 locations
print(f"\n🏆 Top 10 Locations by Revenue:")
top_10 = store_sales.head(10)[['Rank', 'Location', 'Revenue_Formatted', 'Order_Count', 'Profit_Margin', 'AOV_Formatted']]
for idx, row in top_10.iterrows():
    print(f"   #{row['Rank']}. {row['Location']}: {row['Revenue_Formatted']} ({row['Order_Count']:,} orders, {row['Profit_Margin']:.1f}% margin, AOV: {row['AOV_Formatted']})")

# Bottom 5 locations (need attention)
print(f"\n⚠️  Bottom 5 Locations (Need Attention):")
bottom_5 = store_sales.tail(5)[['Rank', 'Location', 'Revenue_Formatted', 'Order_Count', 'Profit_Margin']]
for idx, row in bottom_5.iterrows():
    print(f"   #{row['Rank']}. {row['Location']}: {row['Revenue_Formatted']} ({row['Order_Count']:,} orders, {row['Profit_Margin']:.1f}% margin)")

# Revenue concentration analysis
top_20_percent_locations = int(len(store_sales) * 0.2)
top_20_percent_revenue = store_sales.head(top_20_percent_locations)['Revenue'].sum()
revenue_concentration = (top_20_percent_revenue / store_sales['Revenue'].sum() * 100)

print(f"\n📊 Revenue Concentration:")
print(f"   Top 20% of locations generate {revenue_concentration:.1f}% of total revenue")
if revenue_concentration > 60:
    print(f"   ⚠️  High concentration - business depends heavily on few locations")
else:
    print(f"   ✅ Good distribution across locations")

# Save to CSV for dashboard
output_path = os.path.join(OUTPUT_DIR, 'store_sales.csv')
store_sales[['Rank', 'Location', 'Revenue', 'Quantity', 'Profit', 'Profit_Margin', 
             'Order_Count', 'Avg_Order_Value']].to_csv(output_path, index=False)

print(f"\n💾 Saved KPI export: {output_path}")

# Also save top 20 for quick dashboard loading
top_20_output = store_sales.head(20)[['Location', 'Revenue', 'Profit_Margin']].copy()
top_20_output.to_csv(os.path.join(OUTPUT_DIR, 'store_sales_top20.csv'), index=False)

print(f"💾 Saved top 20 locations: {OUTPUT_DIR}store_sales_top20.csv")

# Save geographic distribution if state/region available
if len(location_cols) > 1 or any('state' in col.lower() or 'region' in col.lower() for col in dim_location.columns):
    print(f"💾 Additional geographic columns detected - consider regional analysis")

print("\n" + "=" * 70)
print("✅ KPI 03 CALCULATION COMPLETE")
print("=" * 70)
print("\n📊 Ready for dashboard visualization (Horizontal Bar Chart 📊)")
print("📄 Dashboard: Page 1 - Executive")

