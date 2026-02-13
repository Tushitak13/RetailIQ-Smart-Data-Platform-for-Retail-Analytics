"""
PURPOSE: Query Hive-style partitioned warehouse using pandas
"""

import pandas as pd
import os
import glob
from pathlib import Path
import json


class HiveWarehouse:

    def __init__(self, warehouse_path='retail_warehouse/'):
        self.warehouse_path = warehouse_path
        self.metastore = {}
        self._load_metastore()

    # =========================================================
    # LOAD METADATA (Hive Metastore Equivalent)
    # =========================================================
    def _load_metastore(self):
        print("📋 Loading Hive Metastore...")

        for meta_file in glob.glob(f'{self.warehouse_path}/*/_METADATA.json'):
            table_name = Path(meta_file).parent.name

            with open(meta_file, 'r') as f:
                self.metastore[table_name] = json.load(f)

            print(f"   ✅ Loaded table: {table_name}")

    # =========================================================
    # SHOW TABLES
    # =========================================================
    def show_tables(self):
        print("\n📊 TABLES IN WAREHOUSE:")
        print("-" * 50)

        for table_name, metadata in self.metastore.items():
            partitions = metadata.get('partitions', [])

            if partitions:
                print(
                    f"   {table_name}: {metadata['record_count']:,} rows, "
                    f"{len(partitions)} partitions"
                )
            else:
                print(
                    f"   {table_name}: {metadata['record_count']:,} rows (non-partitioned)"
                )

    # =========================================================
    # SHOW PARTITIONS (FULLY FIXED - Supports Multi-Level)
    # =========================================================
    def show_partitions(self, table_name):

        if table_name not in self.metastore:
            print(f"❌ Table {table_name} not found")
            return

        table_path = f"{self.warehouse_path}/{table_name}"

        print(f"\n📂 PARTITIONS FOR {table_name}:")
        print("-" * 50)

        partition_dirs = []

        # Recursively scan directories
        for root, dirs, files in os.walk(table_path):
            if any(f.endswith(".parquet") for f in files):
                partition_dirs.append(root)

        if not partition_dirs:
            print("   (Not partitioned)")
            return

        # Display partitions
        for pdir in sorted(partition_dirs):

            parquet_files = glob.glob(f"{pdir}/*.parquet")

            record_count = sum(
                pd.read_parquet(f).shape[0]
                for f in parquet_files
            )

            relative_path = os.path.relpath(pdir, table_path)

            print(f"   {relative_path}: {record_count:,} records")

    # =========================================================
    # READ TABLE WITH PARTITION PRUNING
    # =========================================================
    def read_table(self, table_name, partitions=None):

        if table_name not in self.metastore:
            raise ValueError(f"Table {table_name} not found")

        base_path = f"{self.warehouse_path}/{table_name}"

        # ---------- READ ALL ----------
        if not partitions:

            all_files = glob.glob(f"{base_path}/**/*.parquet", recursive=True)

            if not all_files:
                all_files = glob.glob(f"{base_path}/*.parquet")

            if not all_files:
                return pd.DataFrame()

            df = pd.concat(
                [pd.read_parquet(f) for f in all_files],
                ignore_index=True
            )

            return df

        # ---------- PARTITION PRUNING ----------
        print(f"🔍 Partition pruning enabled: {partitions}")

        path_pattern = base_path

        for col, value in partitions.items():
            value = str(value)

            if col.endswith("_month"):
                value = value.zfill(2)

            path_pattern = f"{path_pattern}/{col}={value}"

        parquet_files = glob.glob(
            f"{path_pattern}/**/*.parquet",
            recursive=True
        )

        if not parquet_files:
            parquet_files = glob.glob(f"{path_pattern}/*.parquet")

        if not parquet_files:
            print(f"⚠️ No data found at: {path_pattern}")
            return pd.DataFrame()

        df = pd.concat(
            [pd.read_parquet(f) for f in parquet_files],
            ignore_index=True
        )

        # Add partition columns back
        for col, value in partitions.items():
            df[col] = value

        return df

    # =========================================================
    # SIMPLE SQL-LIKE QUERY INTERFACE
    # =========================================================
    def query(self, sql_like_filter):

        parts = sql_like_filter.split()

        if 'FROM' not in parts:
            raise ValueError("Query must contain FROM")

        table_name = parts[parts.index('FROM') + 1]

        filters = {}

        if 'WHERE' in parts:

            where_clause = ' '.join(
                parts[parts.index('WHERE') + 1:]
            )

            conditions = where_clause.split('AND')

            for cond in conditions:

                if '=' in cond:

                    col, val = cond.split('=')

                    col = col.strip()
                    val = val.strip().strip("'").strip('"')

                    try:
                        val = int(val)
                    except:
                        try:
                            val = float(val)
                        except:
                            pass

                    filters[col] = val

        return self.read_table(
            table_name,
            partitions=filters if filters else None
        )


# =========================================================
# DEMO EXECUTION
# =========================================================
if __name__ == "__main__":

    print("=" * 70)
    print("🚀 HIVE QUERY ENGINE FOR PANDAS")
    print("=" * 70)

    hive = HiveWarehouse('retail_warehouse/')

    hive.show_tables()

    hive.show_partitions('dim_customer')
    hive.show_partitions('fact_sales')

    # ---------- QUERY 1 ----------
    print("\n" + "=" * 70)
    print("📊 QUERY 1: Partition Pruning")
    print("=" * 70)

    east_customers = hive.read_table(
        'dim_customer',
        partitions={'region': 'East'}
    )

    print(f"Customers: {len(east_customers):,}")

    # ---------- QUERY 2 ----------
    print("\n" + "=" * 70)
    print("📊 QUERY 2: Fact Partition Pruning")
    print("=" * 70)

    nov_sales = hive.read_table(
        'fact_sales',
        partitions={
            'order_year': 2017,
            'order_month': 11
        }
    )

    if not nov_sales.empty:
        print(f"Transactions: {len(nov_sales):,}")
        print(f"Sales: ${nov_sales['Sales'].sum():,.2f}")
        print(f"Profit: ${nov_sales['Profit'].sum():,.2f}")

    # ---------- QUERY 3 ----------
    print("\n" + "=" * 70)
    print("📊 QUERY 3: Join Example")
    print("=" * 70)

    east_customers = hive.read_table(
        'dim_customer',
        partitions={'region': 'East'}
    )

    sales_2017 = hive.read_table(
        'fact_sales',
        partitions={'order_year': 2017}
    )

    joined = sales_2017.merge(
        east_customers[['customer_sk', 'Customer_Name']],
        on='customer_sk'
    )

    print(f"Transactions: {len(joined):,}")
    print(f"Sales: ${joined['Sales'].sum():,.2f}")

    # ---------- QUERY 4 ----------
    print("\n" + "=" * 70)
    print("📊 QUERY 4: SQL-like Query")
    print("=" * 70)

    result = hive.query(
        "SELECT * FROM fact_sales WHERE order_year=2017 AND order_month=11"
    )

    print(f"Records: {len(result):,}")
