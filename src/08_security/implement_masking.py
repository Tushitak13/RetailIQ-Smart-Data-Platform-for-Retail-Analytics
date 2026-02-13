import pandas as pd
import os

print("=" * 70)
print("DATA MASKING IMPLEMENTATION")
print("=" * 70)

dim_customer = pd.read_parquet('data/warehouse/dim_customer.parquet')

print(f"\nOriginal data: {len(dim_customer):,} customers")
print("\nBEFORE MASKING:")
print(dim_customer[['Customer_Name', 'Customer_ID']].head(3))

def mask_name(name):
    if pd.isna(name) or len(str(name)) == 0:
        return "***"
    return str(name)[0] + "***"

def mask_email(email):
    if pd.isna(email) or '@' not in str(email):
        return "***@***.com"
    parts = str(email).split('@')
    return parts[0][0] + "***@" + parts[1]

dim_customer_masked = dim_customer.copy()
dim_customer_masked['Customer_Name'] = dim_customer_masked['Customer_Name'].apply(mask_name)

if 'Email' in dim_customer_masked.columns:
    dim_customer_masked['Email'] = dim_customer_masked['Email'].apply(mask_email)

print("\nAFTER MASKING:")
print(dim_customer_masked[['Customer_Name', 'Customer_ID']].head(3))

os.makedirs('data/warehouse/secured', exist_ok=True)
dim_customer_masked.to_parquet('data/warehouse/secured/dim_customer_masked.parquet', index=False)

print(f"\nSaved masked data to: data/warehouse/secured/dim_customer_masked.parquet")
print("\nSecurity Implementation Complete!")
print("=" * 70)
print("\nAnalysts will use dim_customer_masked.parquet")
print("Admins will use dim_customer.parquet (unmasked)")