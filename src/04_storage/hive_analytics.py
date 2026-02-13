import pandas as pd
import os
import json
import glob
from pathlib import Path
from datetime import datetime


# =========================================================
# Hive Warehouse Reader
# =========================================================
class HiveWarehouse:

    def __init__(self, warehouse_path='retail_warehouse/'):
        self.warehouse_path = warehouse_path
        self.metastore = {}
        self._load_metastore()

    # ---------------- Load Metadata ----------------
    def _load_metastore(self):
        print("Loading Hive Metastore...")

        for meta_file in glob.glob(f'{self.warehouse_path}/*/_METADATA.json'):
            table_name = Path(meta_file).parent.name
            with open(meta_file, 'r') as f:
                self.metastore[table_name] = json.load(f)
            print(f"   Loaded table: {table_name}")

    # ---------------- Read Table ----------------
    def read_table(self, table_name, partitions=None):

        if table_name not in self.metastore:
            raise ValueError(f"Table {table_name} not found")

        base_path = f"{self.warehouse_path}/{table_name}"

        # -------- FULL TABLE READ --------
        if not partitions:

            parquet_files = glob.glob(f"{base_path}/**/*.parquet", recursive=True)

            if not parquet_files:
                return pd.DataFrame()

            dfs = []
            for file_path in parquet_files:
                df = pd.read_parquet(file_path)

                # Extract ALL partition columns
                for part in Path(file_path).parts:
                    if '=' in part:
                        col, val = part.split('=', 1)
                        df[col] = val

                dfs.append(df)

            return pd.concat(dfs, ignore_index=True)

        # -------- PARTITION PRUNING --------
        print(f"   Partition pruning: {partitions}")

        path_pattern = base_path
        for col, val in partitions.items():
            path_pattern += f"/{col}={val}"

        parquet_files = glob.glob(f"{path_pattern}/**/*.parquet", recursive=True)

        if not parquet_files:
            return pd.DataFrame()

        dfs = []
        for file_path in parquet_files:
            df = pd.read_parquet(file_path)

            # ⭐ FIX → Always rebuild partition columns
            for part in Path(file_path).parts:
                if '=' in part:
                    col, val = part.split('=', 1)
                    df[col] = val

            dfs.append(df)

        return pd.concat(dfs, ignore_index=True)

    # ---------------- Show Tables ----------------
    def show_tables(self):
        print("\nTABLES IN WAREHOUSE:")
        print("-" * 50)
        for table_name, metadata in self.metastore.items():
            print(f"   {table_name}: {metadata.get('record_count','?')}")


# =========================================================
# Retail Analytics Engine
# =========================================================
class RetailAnalytics:

    def __init__(self, warehouse_path='retail_warehouse/'):
        print("=" * 70)
        print("INITIALIZING RETAIL ANALYTICS ENGINE")
        print("=" * 70)
        self.hive = HiveWarehouse(warehouse_path)

    # ---------- Normalize Columns ----------
    def _normalize_columns(self, df):
        df.columns = df.columns.str.lower().str.strip()
        return df

    # =====================================================
    # Year Over Year Growth
    # =====================================================
    def year_over_year_growth(self):

        print("\nAnalyzing year-over-year growth...")

        years = [2014, 2015, 2016, 2017]
        growth_data = []

        for year in years:

            sales = self.hive.read_table('fact_sales', {'order_year': year})

            if sales.empty:
                continue

            sales = self._normalize_columns(sales)

            growth_data.append({
                'Year': year,
                'Sales': sales['sales'].sum(),
                'Profit': sales.get('profit', pd.Series([0])).sum(),
                'Orders': sales['order_id'].nunique()
            })

        df = pd.DataFrame(growth_data)

        if len(df) > 1:
            df['Sales_Growth'] = df['Sales'].pct_change() * 100
            df['Profit_Growth'] = df['Profit'].pct_change() * 100

        return df.round(2)

    # =====================================================
    # Category Performance
    # =====================================================
    def category_performance(self):

        print("\nAnalyzing product category performance...")

        sales = self._normalize_columns(self.hive.read_table('fact_sales'))
        products = self._normalize_columns(self.hive.read_table('dim_product'))

        if sales.empty or products.empty:
            return pd.DataFrame()

        df = sales.merge(products, on='product_sk')

        if 'category' not in df.columns:
            print("Category column missing")
            return pd.DataFrame()

        stats = df.groupby('category').agg({
            'sales': ['sum', 'mean', 'count'],
            'profit': ['sum', 'mean'],
            'discount': 'mean',
            'product_sk': 'nunique'
        }).round(2)

        stats.columns = [
            'Total_Sales', 'Avg_Sale', 'Transactions',
            'Total_Profit', 'Avg_Profit', 'Avg_Discount', 'Products'
        ]

        return stats.sort_values('Total_Sales', ascending=False)

    # =====================================================
    # Monthly Sales By Category
    # =====================================================
    def monthly_sales_by_category(self, year=2017):

        print(f"\nAnalyzing monthly sales by category for {year}...")

        sales = self.hive.read_table('fact_sales', {'order_year': year})
        products = self.hive.read_table('dim_product')

        if sales.empty or products.empty:
            return pd.DataFrame()

        sales = self._normalize_columns(sales)
        products = self._normalize_columns(products)

        df = sales.merge(products, on='product_sk')

        # ⭐ FIX: ensure partition column exists
        if 'order_month' not in df.columns:
            print("order_month missing — check partitions")
            return pd.DataFrame()

        df['order_month'] = df['order_month'].astype(int)

        monthly = df.groupby(['order_month', 'category']).agg({
            'sales': 'sum',
            'profit': 'sum',
            'order_id': 'count'
        }).reset_index()

        monthly.columns = ['Month', 'Category', 'Sales', 'Profit', 'Orders']

        return monthly.sort_values(['Month', 'Sales'], ascending=[True, False])

    # =====================================================
    # Regional Performance
    # =====================================================
    def regional_performance(self, year=2017):

        print(f"\nAnalyzing regional performance for {year}...")

        sales = self._normalize_columns(
            self.hive.read_table('fact_sales', {'order_year': year})
        )

        regions = ['East', 'West', 'South', 'Central']
        data = []

        for region in regions:

            customers = self.hive.read_table('dim_customer', {'region': region})

            if customers.empty:
                continue

            customers = self._normalize_columns(customers)

            merged = sales.merge(customers[['customer_sk']], on='customer_sk')

            data.append({
                'Region': region,
                'Sales': merged['sales'].sum(),
                'Profit': merged.get('profit', pd.Series([0])).sum(),
                'Transactions': len(merged),
                'Customers': customers['customer_sk'].nunique()
            })

        return pd.DataFrame(data).sort_values('Sales', ascending=False)

    # =====================================================
    # Customer Lifetime Value
    # =====================================================
    def customer_lifetime_value(self, top_n=10):

        print(f"\nAnalyzing top {top_n} customers...")

        sales = self._normalize_columns(self.hive.read_table('fact_sales'))
        customers = self._normalize_columns(self.hive.read_table('dim_customer'))

        clv = sales.groupby('customer_sk').agg({
            'sales': 'sum',
            'profit': 'sum',
            'order_id': 'count'
        }).reset_index()

        clv = clv.merge(customers[['customer_sk', 'customer_name', 'region']], on='customer_sk')

        clv.columns = [
            'Customer_SK', 'Total_Sales', 'Total_Profit',
            'Order_Count', 'Customer_Name', 'Region'
        ]

        return clv.sort_values('Total_Sales', ascending=False).head(top_n)

    # =====================================================
    # Full Report
    # =====================================================
    def generate_full_report(self):

        print("\nGenerating full analytics report...")

        return {
            'yoy_growth': self.year_over_year_growth(),
            'category_performance': self.category_performance(),
            'regional_2017': self.regional_performance(2017),
            'top_customers': self.customer_lifetime_value(10),
            'monthly_trends_2017': self.monthly_sales_by_category(2017)
        }


# =========================================================
# CLI DRIVER
# =========================================================
if __name__ == "__main__":

    if not os.path.exists('retail_warehouse/'):
        print("Warehouse not found. Run hive_style_warehouse.py first.")
        exit(1)

    analytics = RetailAnalytics('retail_warehouse/')
    analytics.hive.show_tables()

    while True:

        print("\n1. Year-over-Year Growth")
        print("2. Category Performance")
        print("3. Regional Performance")
        print("4. Top Customers")
        print("5. Monthly Sales")
        print("6. Full Report")
        print("7. Exit")

        choice = input("\nEnter choice: ")

        if choice == '1':
            print(analytics.year_over_year_growth())

        elif choice == '2':
            print(analytics.category_performance())

        elif choice == '3':
            print(analytics.regional_performance())

        elif choice == '4':
            print(analytics.customer_lifetime_value())

        elif choice == '5':
            print(analytics.monthly_sales_by_category())

        elif choice == '6':
            report = analytics.generate_full_report()

            os.makedirs('analytics_reports', exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            for name, df in report.items():
                if not df.empty:
                    df.to_csv(f"analytics_reports/{name}_{timestamp}.csv")

            print("Reports saved.")

        elif choice == '7':
            print("Exiting")
            break
