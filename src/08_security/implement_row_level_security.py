import pandas as pd

print("=" * 70)
print("ROW-LEVEL SECURITY IMPLEMENTATION")
print("=" * 70)

class SecureDataAccess:
    
    def __init__(self, user_role, user_region=None):
        self.user_role = user_role
        self.user_region = user_region
        print(f"\nUser Role: {user_role}")
        if user_region:
            print(f"User Region: {user_region}")
    
    def get_sales_data(self):
        fact_sales = pd.read_parquet('data/warehouse/fact_sales.parquet')
        dim_customer = pd.read_parquet('data/warehouse/dim_customer.parquet')
        
        sales_with_region = fact_sales.merge(
            dim_customer[['customer_sk', 'Region']], 
            on='customer_sk'
        )
        
        if self.user_role == 'Admin':
            print("\nAdmin access: Returning ALL data")
            return sales_with_region
        
        elif self.user_role == 'Store_Manager':
            if not self.user_region:
                print("\nERROR: Store Manager must have a region assigned")
                return pd.DataFrame()
            
            print(f"\nStore Manager access: Filtering to {self.user_region} only")
            filtered = sales_with_region[sales_with_region['Region'] == self.user_region]
            print(f"Returning {len(filtered):,} records (out of {len(sales_with_region):,} total)")
            return filtered
        
        elif self.user_role == 'Analyst':
            print("\nAnalyst access: Returning all data with masked customer info")
            return sales_with_region
        
        else:
            print("\nUnknown role: No access")
            return pd.DataFrame()

print("\nDEMO: Store Manager (West Region)")
west_manager = SecureDataAccess(user_role='Store_Manager', user_region='West')
west_data = west_manager.get_sales_data()
print(f"\nWest Manager sees: {len(west_data):,} records")
print(f"Total Sales: ${west_data['Sales'].sum():,.2f}")

print("\n" + "=" * 70)

print("\nDEMO: Store Manager (East Region)")
east_manager = SecureDataAccess(user_role='Store_Manager', user_region='East')
east_data = east_manager.get_sales_data()
print(f"\nEast Manager sees: {len(east_data):,} records")
print(f"Total Sales: ${east_data['Sales'].sum():,.2f}")

print("\n" + "=" * 70)

print("\nDEMO: Admin (Full Access)")
admin = SecureDataAccess(user_role='Admin')
admin_data = admin.get_sales_data()
print(f"\nAdmin sees: {len(admin_data):,} records")
print(f"Total Sales: ${admin_data['Sales'].sum():,.2f}")

print("\n" + "=" * 70)
print("ROW-LEVEL SECURITY DEMONSTRATION COMPLETE")
print("=" * 70)