import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BASE_DIR / 'data'
WAREHOUSE_DIR = DATA_DIR / 'warehouse'
OUTPUT_DIR = DATA_DIR / 'outputs' / 'kpi_exports'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_data():
    print("Loading data...")
    
    fact_sales = pd.read_parquet(WAREHOUSE_DIR / 'fact_sales.parquet')
    dim_product = pd.read_parquet(WAREHOUSE_DIR / 'dim_product.parquet')
    dim_date = pd.read_parquet(WAREHOUSE_DIR / 'dim_date.parquet')
    
    fact_sales = fact_sales.merge(dim_date[['date_sk', 'date']], on='date_sk')
    fact_sales['order_date'] = pd.to_datetime(fact_sales['date'])
    
    print(f"Loaded {len(fact_sales):,} sales records")
    print(f"Loaded {len(dim_product):,} products")
    
    return fact_sales, dim_product

def calculate_sales_velocity(fact_sales):
    print("\nCalculating sales velocity...")
    
    product_velocity = fact_sales.groupby('product_sk').agg({
        'Quantity': 'sum',
        'order_date': ['min', 'max']
    }).reset_index()
    
    product_velocity.columns = ['product_sk', 'total_quantity', 'first_sale', 'last_sale']
    
    print(f"Analyzed {len(product_velocity):,} unique products")
    
    return product_velocity

def calculate_days_on_market(product_velocity):
    print("\nCalculating days on market...")
    
    product_velocity['days_on_market'] = (
        product_velocity['last_sale'] - product_velocity['first_sale']
    ).dt.days
    
    product_velocity['days_on_market'] = product_velocity['days_on_market'].replace(0, 1)
    
    print(f"Average days on market: {product_velocity['days_on_market'].mean():.1f}")
    
    return product_velocity

def calculate_daily_sales_rate(product_velocity):
    print("\nCalculating daily sales rates...")
    
    product_velocity['daily_sales_rate'] = (
        product_velocity['total_quantity'] / product_velocity['days_on_market']
    )
    
    product_velocity['daily_sales_rate'] = product_velocity['daily_sales_rate'].round(2)
    
    print(f"Average daily sales rate: {product_velocity['daily_sales_rate'].mean():.2f} units/day")
    
    return product_velocity

def simulate_current_stock(product_velocity):
    print("\nSimulating current stock levels...")
    
    product_velocity['current_stock'] = (
        product_velocity['total_quantity'] * 0.15
    ).round(0)
    
    print(f"Average stock level: {product_velocity['current_stock'].mean():.0f} units")
    print("NOTE: Stock levels are simulated (15% of total sales)")
    
    return product_velocity

def calculate_stockout_timeline(product_velocity):
    print("\nCalculating stockout timelines...")
    
    product_velocity['days_until_stockout'] = (
        product_velocity['current_stock'] / product_velocity['daily_sales_rate']
    )
    
    product_velocity['days_until_stockout'] = product_velocity['days_until_stockout'].round(1)
    
    product_velocity['days_until_stockout'] = product_velocity['days_until_stockout'].replace(
        [np.inf, -np.inf], 999
    )
    
    at_risk_count = (product_velocity['days_until_stockout'] < 14).sum()
    print(f"Products at risk (<14 days): {at_risk_count}")
    
    return product_velocity

def assign_risk_levels(product_velocity):
    print("\nAssigning risk levels...")
    
    product_velocity['risk_level'] = 'Low'
    
    product_velocity.loc[
        product_velocity['days_until_stockout'] < 14, 
        'risk_level'
    ] = 'Medium'
    
    product_velocity.loc[
        product_velocity['days_until_stockout'] < 7, 
        'risk_level'
    ] = 'High'
    
    risk_counts = product_velocity['risk_level'].value_counts()
    print("\nRisk Level Distribution:")
    for level in ['High', 'Medium', 'Low']:
        count = risk_counts.get(level, 0)
        print(f"   {level:8}: {count:4} products")
    
    return product_velocity

def add_product_details(product_velocity, dim_product):
    print("\nAdding product details...")
    
    out_of_stock_risks = product_velocity.merge(
        dim_product[['product_sk', 'Product_Name', 'Category', 'Sub_Category']], 
        on='product_sk',
        how='left'
    )
    
    column_order = [
        'product_sk',
        'Product_Name',
        'Category',
        'Sub_Category',
        'risk_level',
        'days_until_stockout',
        'current_stock',
        'daily_sales_rate',
        'total_quantity',
        'days_on_market',
        'first_sale',
        'last_sale'
    ]
    
    out_of_stock_risks = out_of_stock_risks[column_order]
    
    risk_order = {'High': 0, 'Medium': 1, 'Low': 2}
    out_of_stock_risks['risk_sort'] = out_of_stock_risks['risk_level'].map(risk_order)
    out_of_stock_risks = out_of_stock_risks.sort_values(
        ['risk_sort', 'days_until_stockout'],
        ascending=[True, True]
    )
    out_of_stock_risks = out_of_stock_risks.drop('risk_sort', axis=1)
    
    print(f"Final dataset: {len(out_of_stock_risks):,} products with full details")
    
    return out_of_stock_risks

def save_output(out_of_stock_risks):
    output_path = OUTPUT_DIR / 'out_of_stock_risks.csv'
    out_of_stock_risks.to_csv(output_path, index=False)
    
    print(f"\nSaved to: {output_path}")
    print(f"Total products: {len(out_of_stock_risks):,}")
    
    high_risk = out_of_stock_risks[out_of_stock_risks['risk_level'] == 'High']
    if len(high_risk) > 0:
        print("\nTOP 5 HIGH-RISK PRODUCTS (Urgent Action Required):")
        print("-" * 80)
        top_5 = high_risk[['Product_Name', 'days_until_stockout', 'current_stock', 'daily_sales_rate']].head()
        for idx, row in top_5.iterrows():
            print(f"   {row['Product_Name'][:40]:40} | "
                  f"{row['days_until_stockout']:5.1f} days | "
                  f"Stock: {row['current_stock']:6.0f} | "
                  f"Rate: {row['daily_sales_rate']:5.1f}/day")
        print("-" * 80)
    else:
        print("\nNo high-risk products found")
    
    medium_risk = out_of_stock_risks[out_of_stock_risks['risk_level'] == 'Medium']
    if len(medium_risk) > 0:
        print(f"\n{len(medium_risk)} products at MEDIUM risk (Monitor closely)")
    
    return output_path

def print_summary_statistics(out_of_stock_risks):
    print("\n" + "="*70)
    print("SUMMARY STATISTICS")
    print("="*70)
    
    print(f"\nInventory Health Metrics:")
    print(f"Average days until stockout: {out_of_stock_risks['days_until_stockout'].mean():.1f} days")
    print(f"Median days until stockout:  {out_of_stock_risks['days_until_stockout'].median():.1f} days")
    print(f"Total inventory value:       {out_of_stock_risks['current_stock'].sum():,.0f} units")
    
    print(f"\nRisk Distribution by Category:")
    risk_by_cat = out_of_stock_risks.groupby(['Category', 'risk_level']).size().unstack(fill_value=0)
    if 'High' in risk_by_cat.columns:
        risk_by_cat = risk_by_cat.sort_values('High', ascending=False)
        print(risk_by_cat.head(10).to_string())
    
    if 'High' in risk_by_cat.columns:
        print(f"\nCategories with most HIGH-risk products:")
        top_risk_cats = risk_by_cat.nlargest(5, 'High')['High']
        for cat, count in top_risk_cats.items():
            if count > 0:
                print(f"   {cat}: {count} products")

def main():
    print("="*70)
    print("KPI 9: OUT-OF-STOCK RISKS ANALYSIS")
    print("="*70)
    
    try:
        fact_sales, dim_product = load_data()
        
        product_velocity = calculate_sales_velocity(fact_sales)
        
        product_velocity = calculate_days_on_market(product_velocity)
        
        product_velocity = calculate_daily_sales_rate(product_velocity)
        
        product_velocity = simulate_current_stock(product_velocity)
        
        product_velocity = calculate_stockout_timeline(product_velocity)
        
        product_velocity = assign_risk_levels(product_velocity)
        
        out_of_stock_risks = add_product_details(product_velocity, dim_product)
        
        output_path = save_output(out_of_stock_risks)
        
        print_summary_statistics(out_of_stock_risks)
        
        print("\n" + "="*70)
        print("KPI 9 COMPLETE")
        print("="*70)
        
    except Exception as e:
        print(f"\nERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()