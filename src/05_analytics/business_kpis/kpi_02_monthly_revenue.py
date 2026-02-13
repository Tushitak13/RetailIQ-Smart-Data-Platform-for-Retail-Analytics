import pandas as pd
import os

os.makedirs('data/outputs/kpi_exports', exist_ok=True)

print("=" * 70)
print("KPI 2: MONTHLY REVENUE")
print("=" * 70)

fact_sales = pd.read_parquet('data/warehouse/fact_sales.parquet')
dim_date = pd.read_parquet('data/warehouse/dim_date.parquet')

print(f"\nLoaded {len(fact_sales):,} sales records")

monthly_revenue = fact_sales.merge(dim_date, on='date_sk')

monthly_revenue = monthly_revenue.groupby(['year', 'month'])['Sales'].sum().reset_index()

monthly_revenue['Year_Month'] = monthly_revenue['year'].astype(str) + '-' + monthly_revenue['month'].astype(str).str.zfill(2)

monthly_revenue = monthly_revenue.sort_values(['year', 'month'])

monthly_revenue.to_csv('data/outputs/kpi_exports/monthly_revenue.csv', index=False)

print(f"\nMonthly Revenue Summary:")
print(f"Total Months: {len(monthly_revenue)}")
print(f"Average Monthly Revenue: ${monthly_revenue['Sales'].mean():,.2f}")
print(f"Highest Monthly Revenue: ${monthly_revenue['Sales'].max():,.2f}")
print(f"Lowest Monthly Revenue: ${monthly_revenue['Sales'].min():,.2f}")

print(f"\nTop 5 Months by Revenue:")
print(monthly_revenue.nlargest(5, 'Sales')[['Year_Month', 'Sales']])

print(f"\nSaved to: data/outputs/kpi_exports/monthly_revenue.csv")
print("=" * 70)