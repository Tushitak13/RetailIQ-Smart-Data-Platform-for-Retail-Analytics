import pandas as pd
import numpy as np
import os
from datetime import datetime

WAREHOUSE_DIR = 'data/warehouse/'
OUTPUT_DIR = 'data/outputs/kpi_exports/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 70)
print("KPI 11: DELIVERY DELAYS ANALYSIS")
print("=" * 70)

print("\nLoading data...")
fact_shipments = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'fact_shipments.parquet'))
dim_date = pd.read_parquet(os.path.join(WAREHOUSE_DIR, 'dim_date.parquet'))

print(f"Loaded {len(fact_shipments):,} shipment records")

print("\nAnalyzing delivery performance...")

def categorize_delivery(days):
    if pd.isna(days):
        return 'Unknown'
    elif days <= 1:
        return 'Same/Next Day'
    elif days <= 3:
        return 'On Time (<=3 days)'
    elif days <= 5:
        return 'Acceptable (4-5 days)'
    elif days <= 7:
        return 'Delayed (6-7 days)'
    else:
        return 'Severely Delayed (>7 days)'

fact_shipments['Delivery_Status'] = fact_shipments['Delivery_Time'].apply(categorize_delivery)

delivery_summary = fact_shipments.groupby('Delivery_Status').agg({
    'order_id': 'count',
    'Delivery_Time': 'mean'
}).reset_index()

delivery_summary = delivery_summary.rename(columns={
    'order_id': 'Shipment_Count',
    'Delivery_Time': 'Avg_Days'
})

total_shipments = delivery_summary['Shipment_Count'].sum()
delivery_summary['Percentage'] = (delivery_summary['Shipment_Count'] / total_shipments * 100).round(2)

status_order = ['Same/Next Day', 'On Time (<=3 days)', 'Acceptable (4-5 days)', 
                'Delayed (6-7 days)', 'Severely Delayed (>7 days)', 'Unknown']
delivery_summary['Status_Order'] = delivery_summary['Delivery_Status'].apply(
    lambda x: status_order.index(x) if x in status_order else 999
)
delivery_summary = delivery_summary.sort_values('Status_Order')

print(f"Categorized {len(fact_shipments):,} shipments")

print("\n" + "=" * 70)
print("DELIVERY PERFORMANCE SUMMARY")
print("=" * 70)

avg_delivery = fact_shipments['Delivery_Time'].mean()
median_delivery = fact_shipments['Delivery_Time'].median()
min_delivery = fact_shipments['Delivery_Time'].min()
max_delivery = fact_shipments['Delivery_Time'].max()

print(f"\nDELIVERY TIME STATISTICS:")
print(f"Average Delivery Time: {avg_delivery:.1f} days")
print(f"Median Delivery Time: {median_delivery:.1f} days")
print(f"Fastest Delivery: {min_delivery:.0f} days")
print(f"Slowest Delivery: {max_delivery:.0f} days")

on_time_count = fact_shipments[fact_shipments['Delivery_Time'] <= 3].shape[0]
delayed_count = fact_shipments[fact_shipments['Delivery_Time'] > 5].shape[0]
on_time_rate = (on_time_count / len(fact_shipments) * 100)
delayed_rate = (delayed_count / len(fact_shipments) * 100)

print(f"\nPERFORMANCE METRICS:")
print(f"On-Time Deliveries (<=3 days): {on_time_count:,} ({on_time_rate:.1f}%)")
print(f"Delayed Deliveries (>5 days): {delayed_count:,} ({delayed_rate:.1f}%)")

if on_time_rate >= 90:
    print(f"Rating: EXCELLENT - {on_time_rate:.1f}% on-time rate")
elif on_time_rate >= 75:
    print(f"Rating: GOOD - {on_time_rate:.1f}% on-time rate")
elif on_time_rate >= 60:
    print(f"Rating: AVERAGE - {on_time_rate:.1f}% on-time rate, needs improvement")
else:
    print(f"Rating: POOR - {on_time_rate:.1f}% on-time rate, urgent action needed")

print(f"\nDELIVERY STATUS DISTRIBUTION:")
for idx, row in delivery_summary.iterrows():
    print(f"{row['Delivery_Status']}: {row['Shipment_Count']:,} shipments ({row['Percentage']:.1f}%)")
    if not pd.isna(row['Avg_Days']):
        print(f"   Average: {row['Avg_Days']:.1f} days")

severely_delayed = fact_shipments[fact_shipments['Delivery_Time'] > 7]
if len(severely_delayed) > 0:
    print(f"\nSEVERELY DELAYED SHIPMENTS:")
    print(f"Count: {len(severely_delayed):,}")
    print(f"Average delay: {severely_delayed['Delivery_Time'].mean():.1f} days")
    print(f"Max delay: {severely_delayed['Delivery_Time'].max():.0f} days")

print(f"\nAnalyzing delivery trends...")
shipments_with_date = fact_shipments.merge(dim_date, on='date_sk', how='left')

date_column = 'date' if 'date' in shipments_with_date.columns else None

if date_column:
    shipments_with_date[date_column] = pd.to_datetime(shipments_with_date[date_column])
    shipments_with_date['Year_Month'] = shipments_with_date[date_column].dt.to_period('M')
    
    monthly_delivery = shipments_with_date.groupby('Year_Month').agg({
        'Delivery_Time': 'mean',
        'order_id': 'count'
    }).reset_index()
    
    monthly_delivery = monthly_delivery.rename(columns={
        'Delivery_Time': 'Avg_Delivery_Days',
        'order_id': 'Shipment_Count'
    })
    
    print(f"\nRECENT MONTHS PERFORMANCE:")
    for idx, row in monthly_delivery.tail(6).iterrows():
        print(f"{row['Year_Month']}: {row['Avg_Delivery_Days']:.1f} days ({row['Shipment_Count']:,} shipments)")
    
    monthly_delivery['Year_Month'] = monthly_delivery['Year_Month'].astype(str)
    monthly_delivery.to_csv(os.path.join(OUTPUT_DIR, 'delivery_delays_monthly.csv'), index=False)

print(f"\nSaving KPI exports...")

delivery_summary[['Delivery_Status', 'Shipment_Count', 'Percentage', 'Avg_Days']].to_csv(
    os.path.join(OUTPUT_DIR, 'delivery_delays_summary.csv'), index=False
)

metrics_data = {
    'metric': ['Average Delivery Days', 'Median Delivery Days', 'On-Time Rate %', 'Delayed Rate %'],
    'value': [avg_delivery, median_delivery, on_time_rate, delayed_rate]
}
pd.DataFrame(metrics_data).to_csv(os.path.join(OUTPUT_DIR, 'delivery_delays_metrics.csv'), index=False)

fact_shipments[['order_id', 'Delivery_Time', 'Delivery_Status']].to_csv(
    os.path.join(OUTPUT_DIR, 'delivery_delays_detail.csv'), index=False
)

print(f"Saved: delivery_delays_summary.csv")
print(f"Saved: delivery_delays_metrics.csv")
print(f"Saved: delivery_delays_detail.csv")
if date_column:
    print(f"Saved: delivery_delays_monthly.csv")

print("\n" + "=" * 70)
print("KPI 11 CALCULATION COMPLETE")
print("=" * 70)
print("\nReady for dashboard: Bar Chart showing delay distribution")
print("Dashboard: Page 3 - Operations")
print(f"\nKEY INSIGHT: {on_time_rate:.1f}% on-time delivery rate")