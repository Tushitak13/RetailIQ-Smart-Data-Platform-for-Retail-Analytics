import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BASE_DIR / 'data'
WAREHOUSE_DIR = DATA_DIR / 'warehouse'
OUTPUT_DIR = DATA_DIR / 'outputs' / 'kpi_exports'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_data():
    print("Loading fact_sales data...")
    fact_sales = pd.read_parquet(WAREHOUSE_DIR / 'fact_sales.parquet')
    
    if 'order_date' not in fact_sales.columns:
        dim_date = pd.read_parquet(WAREHOUSE_DIR / 'dim_date.parquet')
        fact_sales = fact_sales.merge(dim_date, on='date_sk')
        fact_sales['order_date'] = fact_sales['date']
    
    fact_sales['order_date'] = pd.to_datetime(fact_sales['order_date'])
    
    print(f"Loaded {len(fact_sales):,} sales records")
    return fact_sales

def identify_first_purchases(fact_sales):
    print("\nIdentifying first purchase dates...")
    
    customer_col = 'customer_sk' if 'customer_sk' in fact_sales.columns else 'customer_id'
    
    first_purchase = fact_sales.groupby(customer_col)['order_date'] \
        .min() \
        .reset_index() \
        .rename(columns={'order_date': 'first_purchase_date'})
    
    print(f"Identified first purchases for {len(first_purchase):,} customers")
    return first_purchase, customer_col

def classify_orders(fact_sales, first_purchase, customer_col):
    print("\nClassifying orders as New or Returning...")
    
    fact_sales_classified = fact_sales.merge(
        first_purchase, 
        on=customer_col,
        how='left'
    )
    
    fact_sales_classified['customer_type'] = np.where(
        fact_sales_classified['order_date'] == fact_sales_classified['first_purchase_date'],
        'New',
        'Returning'
    )
    
    print(f"Classified {len(fact_sales_classified):,} orders")
    return fact_sales_classified

def aggregate_by_date(fact_sales_classified, customer_col):
    print("\nAggregating daily customer counts...")
    
    fact_sales_classified['date'] = fact_sales_classified['order_date'].dt.date
    
    new_vs_returning = fact_sales_classified.groupby(
        ['date', 'customer_type']
    )[customer_col].nunique().reset_index(name='customer_count')
    
    print(f"Created {len(new_vs_returning):,} date-type records")
    return new_vs_returning

def pivot_data(new_vs_returning):
    print("\nPivoting to wide format...")
    
    new_vs_returning_pivot = new_vs_returning.pivot_table(
        index='date',
        columns='customer_type',
        values='customer_count',
        fill_value=0
    ).reset_index()
    
    new_vs_returning_pivot.columns.name = None
    
    print(f"Pivoted to {len(new_vs_returning_pivot):,} daily records")
    return new_vs_returning_pivot

def save_output(new_vs_returning_pivot):
    output_path = OUTPUT_DIR / 'new_vs_returning.csv'
    new_vs_returning_pivot.to_csv(output_path, index=False)
    print(f"\nSaved to: {output_path}")
    print(f"Total rows: {len(new_vs_returning_pivot):,}")
    
    if 'New' in new_vs_returning_pivot.columns:
        print(f"\nTotal New Customers: {new_vs_returning_pivot['New'].sum():,}")
    if 'Returning' in new_vs_returning_pivot.columns:
        print(f"Total Returning Customers: {new_vs_returning_pivot['Returning'].sum():,}")

def main():
    print("=" * 60)
    print("KPI 7: NEW VS RETURNING CUSTOMERS")
    print("=" * 60)
    
    fact_sales = load_data()
    
    first_purchase, customer_col = identify_first_purchases(fact_sales)
    
    fact_sales_classified = classify_orders(fact_sales, first_purchase, customer_col)
    
    new_vs_returning = aggregate_by_date(fact_sales_classified, customer_col)
    
    new_vs_returning_pivot = pivot_data(new_vs_returning)
    
    save_output(new_vs_returning_pivot)
    
    print("\n" + "=" * 60)
    print("KPI 7 COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()