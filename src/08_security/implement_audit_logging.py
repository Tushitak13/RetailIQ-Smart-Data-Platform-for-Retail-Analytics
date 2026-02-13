import pandas as pd
import os
from datetime import datetime
import json

class AuditLogger:
    
    def __init__(self, log_file='data/security/audit_log.json'):
        self.log_file = log_file
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        if not os.path.exists(log_file):
            with open(log_file, 'w') as f:
                json.dump([], f)
    
    def log_access(self, user_id, table_name, action, rows_accessed=None):
        timestamp = datetime.now().isoformat()
        
        log_entry = {
            'timestamp': timestamp,
            'user_id': user_id,
            'table': table_name,
            'action': action,
            'rows_accessed': rows_accessed
        }
        
        with open(self.log_file, 'r') as f:
            logs = json.load(f)
        
        logs.append(log_entry)
        
        with open(self.log_file, 'w') as f:
            json.dump(logs, f, indent=2)
        
        print(f"[AUDIT] {timestamp} | User: {user_id} | Table: {table_name} | Action: {action}")
    
    def get_logs(self, user_id=None):
        with open(self.log_file, 'r') as f:
            logs = json.load(f)
        
        if user_id:
            logs = [log for log in logs if log['user_id'] == user_id]
        
        return pd.DataFrame(logs)

print("=" * 70)
print("AUDIT LOGGING IMPLEMENTATION")
print("=" * 70)

logger = AuditLogger()

print("\nSimulating data access...")

logger.log_access(
    user_id='analyst_001',
    table_name='dim_customer',
    action='SELECT',
    rows_accessed=738
)

logger.log_access(
    user_id='store_manager_west',
    table_name='fact_sales',
    action='SELECT',
    rows_accessed=1200
)

logger.log_access(
    user_id='admin_001',
    table_name='dim_customer',
    action='UPDATE',
    rows_accessed=1
)

logger.log_access(
    user_id='analyst_002',
    table_name='fact_sales',
    action='SELECT',
    rows_accessed=4042
)

print("\n" + "=" * 70)
print("AUDIT LOG SUMMARY")
print("=" * 70)

logs_df = logger.get_logs()
print(f"\nTotal audit entries: {len(logs_df)}")
print("\nRecent activity:")
print(logs_df.to_string(index=False))

print(f"\nAudit log saved to: {logger.log_file}")
print("=" * 70)