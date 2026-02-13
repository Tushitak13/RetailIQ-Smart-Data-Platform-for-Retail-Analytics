import pandas as pd
import numpy as np
from datetime import datetime
from itertools import combinations
from collections import Counter
import os

os.makedirs('data/outputs/kpi_exports', exist_ok=True)

start_time = datetime.now()
print("=" * 70)
print("KPI 12: BASKET ANALYSIS")
print("=" * 70)
print(f"\nStart Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

print("\nLOADING DATA...")
fact_sales = pd.read_parquet('data/warehouse/fact_sales.parquet')
dim_product = pd.read_parquet('data/warehouse/dim_product.parquet')

print(f"fact_sales loaded - Rows: {len(fact_sales):,}")
print(f"dim_product loaded - Rows: {len(dim_product):,}")

product_col = 'product_sk' if 'product_sk' in fact_sales.columns else 'product_id'

sales_with_products = fact_sales.merge(dim_product[[product_col, 'Product_Name']], 
                                       on=product_col, how='left')

print("\nPERFORMING BASKET ANALYSIS...")

baskets = sales_with_products.groupby('order_id')['Product_Name'].apply(list).reset_index()
baskets.columns = ['order_id', 'products']

baskets['basket_size'] = baskets['products'].apply(len)
avg_basket_size = baskets['basket_size'].mean()
total_orders = len(baskets)

print(f"\nBasket metrics calculated:")
print(f"Total orders analyzed: {total_orders:,}")
print(f"Average basket size: {avg_basket_size:.2f} items")
print(f"Min basket size: {baskets['basket_size'].min()}")
print(f"Max basket size: {baskets['basket_size'].max()}")

print("\nFINDING PRODUCT ASSOCIATIONS...")
product_pairs = []
for products in baskets['products']:
    if len(products) >= 2:
        pairs = list(combinations(sorted(products), 2))
        product_pairs.extend(pairs)

pair_counts = Counter(product_pairs)
top_pairs = pair_counts.most_common(20)

associations_df = pd.DataFrame(top_pairs, columns=['product_pair', 'frequency'])
associations_df[['product_1', 'product_2']] = pd.DataFrame(
    associations_df['product_pair'].tolist(), index=associations_df.index
)
associations_df['support'] = (associations_df['frequency'] / total_orders) * 100
associations_df = associations_df[['product_1', 'product_2', 'frequency', 'support']]

print(f"\nTop product associations identified:")
print(f"Unique product pairs: {len(pair_counts):,}")

print(f"\nTOP 10 PRODUCT COMBINATIONS:")
for idx, row in associations_df.head(10).iterrows():
    print(f"{idx+1}. {row['product_1']} + {row['product_2']}")
    print(f"   Bought together {row['frequency']} times ({row['support']:.2f}% support)")

print("\nEXPORTING RESULTS...")
baskets.to_csv('data/outputs/kpi_exports/basket_analysis.csv', index=False)
associations_df.to_csv('data/outputs/kpi_exports/product_associations.csv', index=False)

summary = pd.DataFrame({
    'metric': ['Total Orders', 'Average Basket Size', 'Min Basket Size', 
               'Max Basket Size', 'Unique Products Sold', 'Total Product Pairs Found'],
    'value': [total_orders, avg_basket_size, baskets['basket_size'].min(),
              baskets['basket_size'].max(), sales_with_products[product_col].nunique(),
              len(pair_counts)]
})
summary.to_csv('data/outputs/kpi_exports/basket_analysis_summary.csv', index=False)

print(f"\nSaved: basket_analysis.csv")
print(f"Saved: product_associations.csv")
print(f"Saved: basket_analysis_summary.csv")

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

print(f"\nEnd Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Duration: {duration:.2f} seconds")
print("=" * 70)