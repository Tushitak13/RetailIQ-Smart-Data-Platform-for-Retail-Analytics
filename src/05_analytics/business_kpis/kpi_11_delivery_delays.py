# src/05_analytics/business_kpis/kpi_11_delivery_delays.py
"""
KPI 11: DELIVERY DELAYS
Time: 25 minutes
Purpose: Analyze shipment delivery times and identify delays
Output: Bar chart showing delay distribution
Dashboard Page: Page 3 - Operations
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
print("KPI 11: DELIVERY DELAYS ANALYSIS")
print("=" * 70)

# Load required tables
print("\n📥 Loading data...")
fact_shipments = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'fact_shipments.parquet'))
dim_date = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_date.parquet'))

print(f"   ✅ Loaded {len(fact_shipments):,} shipment records")

# Check if we have delivery_days column
if 'delivery_days' not in fact_shipments.columns:
    print("\n⚠️  Warning: 'delivery_days' column not found")
    print("   Attempting to calculate from order and ship dates...")
    
    # Try to merge with dates and calculate
    shipments_with_date = fact_shipments.merge(dim_date, on='date_sk', how='left')
    
    # This is a simplified approach - you may need to adjust based on your schema
    if 'Ship_Date' in fact_shipments.columns and 'Order_Date' in fact_shipments.columns:
        fact_shipments['delivery_days'] = (fact_shipments['Ship_Date'] - fact_shipments['Order_Date']).dt.days
    else:
        print("   ❌ Cannot calculate delivery days from available data")
        print("   Using 'delivery_days' from fact_shipments as-is")

# Analyze delivery performance
print("\n📊 Analyzing delivery performance...")

# Create delivery status categories
def categorize_delivery(days):
    if pd.isna(days):
        return 'Unknown'
    elif days <= 1:
        return 'Same/Next Day'
    elif days <= 3:
        return 'On Time (≤3 days)'
    elif days <= 5:
        return 'Acceptable (4-5 days)'
    elif days <= 7:
        return 'Delayed (6-7 days)'
    else:
        return 'Severely Delayed (>7 days)'

fact_shipments['Delivery_Status'] = fact_shipments['delivery_days'].apply(categorize_delivery)

# Calculate metrics
delivery_summary = fact_shipments.groupby('Delivery_Status').agg({
    'shipment_sk': 'count',
    'delivery_days': 'mean'
}).reset_index()

delivery_summary = delivery_summary.rename(columns={
    'shipment_sk': 'Shipment_Count',
    'delivery_days': 'Avg_Days'
})

# Calculate percentages
total_shipments = delivery_summary['Shipment_Count'].sum()
delivery_summary['Percentage'] = (delivery_summary['Shipment_Count'] / total_shipments * 100).round(2)

# Sort by a custom order
status_order = ['Same/Next Day', 'On Time (≤3 days)', 'Acceptable (4-5 days)', 
                'Delayed (6-7 days)', 'Severely Delayed (>7 days)', 'Unknown']
delivery_summary['Status_Order'] = delivery_summary['Delivery_Status'].apply(
    lambda x: status_order.index(x) if x in status_order else 999
)
delivery_summary = delivery_summary.sort_values('Status_Order')

print(f"   ✅ Categorized {len(fact_shipments):,} shipments")

# Overall statistics
print("\n" + "=" * 70)
print("📊 DELIVERY PERFORMANCE SUMMARY")
print("=" * 70)

avg_delivery = fact_shipments['delivery_days'].mean()
median_delivery = fact_shipments['delivery_days'].median()
min_delivery = fact_shipments['delivery_days'].min()
max_delivery = fact_shipments['delivery_days'].max()

print(f"\n⏱️  DELIVERY TIME STATISTICS:")
print(f"   Average Delivery Time: {avg_delivery:.1f} days")
print(f"   Median Delivery Time: {median_delivery:.1f} days")
print(f"   Fastest Delivery: {min_delivery:.0f} days")
print(f"   Slowest Delivery: {max_delivery:.0f} days")

# Performance metrics
on_time_count = fact_shipments[fact_shipments['delivery_days'] <= 3].shape[0]
delayed_count = fact_shipments[fact_shipments['delivery_days'] > 5].shape[0]
on_time_rate = (on_time_count / len(fact_shipments) * 100)
delayed_rate = (delayed_count / len(fact_shipments) * 100)

print(f"\n📈 PERFORMANCE METRICS:")
print(f"   On-Time Deliveries (≤3 days): {on_time_count:,} ({on_time_rate:.1f}%)")
print(f"   Delayed Deliveries (>5 days): {delayed_count:,} ({delayed_rate:.1f}%)")

if on_time_rate >= 90:
    print(f"   ✅ EXCELLENT - {on_time_rate:.1f}% on-time rate")
elif on_time_rate >= 75:
    print(f"   ✅ GOOD - {on_time_rate:.1f}% on-time rate")
elif on_time_rate >= 60:
    print(f"   ⚠️  AVERAGE - {on_time_rate:.1f}% on-time rate, needs improvement")
else:
    print(f"   ❌ POOR - {on_time_rate:.1f}% on-time rate, urgent action needed")

# Distribution breakdown
print(f"\n📊 DELIVERY STATUS DISTRIBUTION:")
for idx, row in delivery_summary.iterrows():
    print(f"   {row['Delivery_Status']}: {row['Shipment_Count']:,} shipments ({row['Percentage']:.1f}%)")
    if not pd.isna(row['Avg_Days']):
        print(f"      Average: {row['Avg_Days']:.1f} days")

# Identify problem shipments (severely delayed)
severely_delayed = fact_shipments[fact_shipments['delivery_days'] > 7]
if len(severely_delayed) > 0:
    print(f"\n⚠️  SEVERELY DELAYED SHIPMENTS:")
    print(f"   Count: {len(severely_delayed):,}")
    print(f"   Average delay: {severely_delayed['delivery_days'].mean():.1f} days")
    print(f"   Max delay: {severely_delayed['delivery_days'].max():.0f} days")

# Monthly trend if date available
if 'date_sk' in fact_shipments.columns:
    print(f"\n📅 Analyzing delivery trends over time...")
    shipments_with_date = fact_shipments.merge(dim_date, on='date_sk', how='left')
    
    # Find date column
    date_column = None
    for col in shipments_with_date.columns:
        if 'date' in col.lower() and 'sk' not in col.lower():
            if pd.api.types.is_datetime64_any_dtype(shipments_with_date[col]):
                date_column = col
                break
    
    if date_column:
        shipments_with_date[date_column] = pd.to_datetime(shipments_with_date[date_column])
        shipments_with_date['Year_Month'] = shipments_with_date[date_column].dt.to_period('M')
        
        monthly_delivery = shipments_with_date.groupby('Year_Month').agg({
            'delivery_days': 'mean',
            'shipment_sk': 'count'
        }).reset_index()
        
        monthly_delivery = monthly_delivery.rename(columns={
            'delivery_days': 'Avg_Delivery_Days',
            'shipment_sk': 'Shipment_Count'
        })
        
        print(f"\n📈 RECENT MONTHS PERFORMANCE:")
        for idx, row in monthly_delivery.tail(6).iterrows():
            print(f"   {row['Year_Month']}: {row['Avg_Delivery_Days']:.1f} days ({row['Shipment_Count']:,} shipments)")
        
        # Save monthly trend
        monthly_delivery['Year_Month'] = monthly_delivery['Year_Month'].astype(str)
        monthly_delivery.to_csv(os.path.join(OUTPUT_DIR, 'delivery_delays_monthly.csv'), index=False)
        print(f"\n💾 Saved monthly trends")

# Save outputs
print(f"\n💾 Saving KPI exports...")

# Main summary
delivery_summary[['Delivery_Status', 'Shipment_Count', 'Percentage', 'Avg_Days']].to_csv(
    os.path.join(OUTPUT_DIR, 'delivery_delays_summary.csv'), index=False
)

# Overall metrics
metrics_data = {
    'metric': ['Average Delivery Days', 'Median Delivery Days', 'On-Time Rate %', 'Delayed Rate %'],
    'value': [avg_delivery, median_delivery, on_time_rate, delayed_rate]
}
pd.DataFrame(metrics_data).to_csv(os.path.join(OUTPUT_DIR, 'delivery_delays_metrics.csv'), index=False)

# Detailed shipment data (for drill-down)
fact_shipments[['shipment_sk', 'delivery_days', 'Delivery_Status']].to_csv(
    os.path.join(OUTPUT_DIR, 'delivery_delays_detail.csv'), index=False
)

print(f"   ✅ Saved: delivery_delays_summary.csv")
print(f"   ✅ Saved: delivery_delays_metrics.csv")
print(f"   ✅ Saved: delivery_delays_detail.csv")

print("\n" + "=" * 70)
print("✅ KPI 11 CALCULATION COMPLETE")
print("=" * 70)
print("\n📊 Ready for dashboard: Bar Chart showing delay distribution")
print("📄 Dashboard: Page 3 - Operations")
print(f"\n🎯 KEY INSIGHT: {on_time_rate:.1f}% on-time delivery rate")
