# Security & Access Control Design

**Team:** RetailIQ Data Platform Team
**Date:** February 14, 2025
**Deliverable:** Problem Statement Section C (Security)

---

## 1. Data Classification Matrix

| Dataset | Sensitive Fields | Classification | Protection |
|---------|-----------------|----------------|------------|
| dim_customer | Customer_Name, email, phone | HIGH | Masking, Restricted Access |
| fact_sales | Sales, Profit | MEDIUM | Role-Based Access |
| dim_product | Product_Name, Category | LOW | Public |
| dim_location | State, City, Postal_Code | LOW | Public |
| fact_shipments | Delivery_Time, Ship_Mode | LOW | Public |

**Rationale:** Customer PII (Personally Identifiable Information) is classified as HIGH sensitivity and requires the strictest protection. Financial metrics like sales and profit are MEDIUM sensitivity and require role-based access control.

---

## 2. Role-Based Access Control (RBAC)

| Role | Access Level | PII Access? | Store Filter? | Description |
|------|--------------|-------------|---------------|-------------|
| Admin | Full | Yes (Unmasked) | No (all data) | Platform administrators with full system access |
| Analyst | Read-only | Masked Only | No (all data) | Data analysts performing cross-store analysis |
| Store Manager | Read-only | No | Yes (own store) | Individual store performance review only |
| Executive | Read-only (aggregated) | No | No (all data) | Leadership viewing KPIs and summaries |
| Auditor | Read-only | Yes (Unmasked) | No (all data) | Compliance reviews with audit log access |

### Implementation Example

```sql
-- Create roles
CREATE ROLE analyst;
CREATE ROLE store_manager;
CREATE ROLE executive;
CREATE ROLE auditor;

-- Grant permissions to Analyst (with masking)
GRANT SELECT ON fact_sales TO analyst;
GRANT SELECT ON fact_shipments TO analyst;
GRANT SELECT ON dim_product TO analyst;
GRANT SELECT ON dim_location TO analyst;

-- Column-level access for PII (masked)
GRANT SELECT (customer_sk, Customer_Name, Segment, Region) ON dim_customer TO analyst;

-- Store Manager - Row-Level Security
CREATE SECURITY POLICY StoreAccessFilter
ADD FILTER PREDICATE dbo.fn_store_filter(location_sk) ON fact_sales
WITH (STATE = ON);

-- Executive - Aggregated views only
GRANT SELECT ON vw_executive_dashboard TO executive;
```

---

## 3. Data Protection Implementation

### Encryption

**Data at Rest:**
- Algorithm: AES-256 encryption
- Implementation: All Parquet files stored on encrypted disk/cloud storage
- Key Management: In production, use cloud provider KMS (AWS KMS, Azure Key Vault, GCP Cloud KMS)

**Data in Transit:**
- Protocol: TLS 1.3 for all API and data transfers
- Implementation: All connections between services use HTTPS/SSL

### Dynamic Data Masking

Applied to protect PII while maintaining analytical utility:

| Column | Masking Rule | Example (Original → Masked) |
|--------|--------------|----------------------------|
| Customer_Name | Show first 1 char, rest X | "John Smith" → "J***" |
| Email | Show first 1 char + domain | "john@email.com" → "j***@email.com" |
| Phone | Show last 4 digits | "9876-5432" → "XXXX-5432" |

```sql
-- Dynamic Data Masking for PII columns
ALTER TABLE dim_customer
ALTER COLUMN Customer_Name ADD MASKED WITH (FUNCTION = 'partial(1, "XXX", 0)');

ALTER TABLE dim_customer
ALTER COLUMN email ADD MASKED WITH (FUNCTION = 'email()');

ALTER TABLE dim_customer
ALTER COLUMN phone ADD MASKED WITH (FUNCTION = 'default()');
```

**Result:** Analysts can still perform aggregations and grouping on customer segments without seeing actual PII.

### Row-Level Security (RLS)

**Use Case:** Store Managers should only see their own store's data.

```sql
-- Create security predicate function
CREATE FUNCTION dbo.fn_store_filter(@location_sk INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS fn_result
WHERE @location_sk = (
    SELECT location_sk FROM dbo.dim_location
    WHERE store_id = USER_NAME()
);

-- Apply security policy
CREATE SECURITY POLICY StoreAccessFilter
ADD FILTER PREDICATE dbo.fn_store_filter(location_sk) ON fact_sales,
ADD FILTER PREDICATE dbo.fn_store_filter(location_sk) ON fact_shipments
WITH (STATE = ON);
```

**Benefit:** The database automatically filters rows based on the logged-in user. No application logic required.

### Audit Logging

All access to sensitive data is tracked and logged:

| Event Type | Examples | Retention Period |
|------------|----------|------------------|
| Authentication | Login success/failure, password changes | 90 days |
| Data Access | SELECT queries on dim_customer, fact_sales | 1 year |
| Data Modification | INSERT/UPDATE/DELETE operations | 3 years |
| Permission Changes | GRANT/REVOKE statements | 3 years |

```sql
-- Enable audit logging
CREATE SERVER AUDIT RetailIQ_Audit
TO FILE (FILEPATH = '/var/log/audit/');

CREATE DATABASE AUDIT SPECIFICATION RetailIQ_DB_Audit
FOR SERVER AUDIT RetailIQ_Audit
ADD (SELECT ON dim_customer BY analyst),
ADD (SELECT ON dim_customer BY auditor),
ADD (UPDATE, DELETE ON fact_sales BY admin)
WITH (STATE = ON);
```

---

## 4. Production Cloud Architecture

If deployed to production, we would use cloud-native security services:

| Component | AWS | Azure | GCP |
|-----------|-----|-------|-----|
| Identity & Access | AWS IAM + MFA | Azure AD + MFA | Cloud IAM + MFA |
| Storage Encryption | S3 with SSE-KMS | ADLS Gen2 with CMK | GCS with CSEK |
| Data Warehouse | Redshift (TDE, RLS) | Synapse Analytics | BigQuery (column-level security) |
| Key Management | AWS KMS | Azure Key Vault | Cloud KMS |
| Audit Logging | CloudTrail | Azure Monitor | Cloud Audit Logs |
| Network Security | VPC, Security Groups | VNet, NSG | VPC, Firewall Rules |

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                 PRODUCTION SECURITY ARCHITECTURE             │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  IDENTITY & ACCESS MANAGEMENT                                │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ • Azure AD / AWS IAM / Google Cloud IAM            │    │
│  │ • Multi-Factor Authentication (MFA) required       │    │
│  │ • SSO integration with corporate identity          │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                               │
│  STORAGE SECURITY                                            │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ • Azure ADLS / AWS S3 / Google Cloud Storage       │    │
│  │ • Server-side encryption (AES-256)                 │    │
│  │ • Customer-managed keys (CMK) via KMS              │    │
│  │ • Private endpoints / VPC isolation                │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                               │
│  DATABASE SECURITY                                           │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ • Azure Synapse / AWS Redshift / BigQuery          │    │
│  │ • Transparent Data Encryption (TDE)                │    │
│  │ • Row-Level Security + Column-Level Security       │    │
│  │ • SQL Audit Logs → Log Analytics / CloudWatch      │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                               │
│  COMPLIANCE FRAMEWORKS                                       │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ • GDPR ready (right to delete, data portability)   │    │
│  │ • CCPA compliant (opt-out, PII protection)         │    │
│  │ • PCI DSS compliant for payment data               │    │
│  │ • SOC2 Type II attestation                         │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. Compliance Considerations

### GDPR (General Data Protection Regulation)
- Right to access: Customers can request all their data
- Right to deletion: Customers can request data removal
- Data portability: Export customer data in machine-readable format
- Implementation: Added `is_deleted` flag in dim_customer for soft deletes

### CCPA (California Consumer Privacy Act)
- Opt-out mechanism: Customers can opt out of data sharing
- Data disclosure: Clear documentation of what data is collected
- Implementation: Privacy preference tracking in customer dimension

### PCI DSS (Payment Card Industry Data Security Standard)
- No credit card data stored in our current implementation
- If payment data added: Use tokenization, never store CVV
- All payment processing would be handled by certified payment gateway

### Data Retention Policy
- Customer data: Retained for 7 years (legal requirement)
- Transaction data: Retained for 5 years
- Audit logs: Retained for 3 years
- Aggregate reports: Retained indefinitely

---

## 6. Implementation Status

### Current Implementation (Hackathon)
- Data stored in encrypted Parquet files
- Access control documented but not enforced (proof-of-concept)
- Column-level permissions defined in documentation
- Audit logging designed but not implemented

### Production Requirements
- Deploy to cloud platform (Azure/AWS/GCP)
- Implement Azure AD / AWS IAM authentication
- Enable Transparent Data Encryption on data warehouse
- Configure Row-Level Security policies
- Set up audit logging and monitoring
- Implement automated compliance reporting

---

## 7. Security Testing & Validation

### Planned Security Tests
1. **Access Control Test**: Verify store managers can only see their store's data
2. **Masking Test**: Confirm analysts see masked PII
3. **Encryption Test**: Validate data-at-rest encryption is enabled
4. **Audit Test**: Verify all sensitive data access is logged
5. **Penetration Test**: Third-party security assessment before production

### Security Metrics
- Failed login attempts: < 0.1% of total logins
- Unauthorized access attempts: 0 (should trigger alerts)
- Data breach incidents: 0
- Audit log completeness: 100%

---

**Prepared by:** Data Engineering Team
**Review Date:** February 14, 2025
**Next Review:** Quarterly security audit required

---

## Summary

This security design ensures:
- Customer PII is protected through masking and access controls
- Store managers can only access their own store's data
- All data is encrypted at rest and in transit
- Complete audit trail for compliance
- Cloud-ready architecture for production deployment

Our approach follows industry best practices and is ready for enterprise deployment with cloud-native security services.