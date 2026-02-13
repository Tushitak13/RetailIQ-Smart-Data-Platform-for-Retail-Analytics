import pandas as pd
import os

os.makedirs('data/outputs/kpi_exports', exist_ok=True)

print("=" * 70)
print("KPI 4: CITY-WISE SALES")
print("=" * 70)

fact_sales = pd.read_parquet('data/warehouse/fact_sales.parquet')
dim_location = pd.read_parquet('data/warehouse/dim_location.parquet')

print(f"\nLoaded {len(fact_sales):,} sales records")
print(f"Location columns available: {list(dim_location.columns)}")

city_sales = fact_sales.merge(dim_location, on='location_sk')

available_columns = []
if 'City' in city_sales.columns:
    available_columns.append('City')
elif 'city' in city_sales.columns:
    available_columns.append('city')

if 'State' in city_sales.columns:
    available_columns.append('State')
elif 'state' in city_sales.columns:
    available_columns.append('state')

if not available_columns:
    print("\nERROR: No City or State columns found in location dimension")
    print("Available columns:", list(dim_location.columns))
    exit(1)

print(f"\nGrouping by: {available_columns}")

city_sales = city_sales.groupby(available_columns)['Sales'].sum().reset_index()

city_sales = city_sales.sort_values('Sales', ascending=False)

city_sales.to_csv('data/outputs/kpi_exports/city_wise_sales.csv', index=False)

print(f"\nCity-Wise Sales Summary:")
print(f"Total Locations: {len(city_sales)}")
print(f"Average Location Revenue: ${city_sales['Sales'].mean():,.2f}")
print(f"Total Revenue: ${city_sales['Sales'].sum():,.2f}")

print(f"\nTop 15 Locations by Revenue:")
print(city_sales.head(15).to_string(index=False))

print(f"\nSaved to: data/outputs/kpi_exports/city_wise_sales.csv")
print("=" * 70)