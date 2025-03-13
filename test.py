# Temporary debug script
from BigQueryIntergration import BigQueryClient
from google.cloud import bigquery
bq_client = BigQueryClient()
worker_name = 'Ryan Sosa'.strip()
        
query = """
    SELECT worker_id, name, working_hours, timezone
    FROM `calendar_system.workers`
    WHERE LOWER(name) = LOWER(@worker_name)
    LIMIT 1
"""
print("**** query : ", query)
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("worker_name", "STRING", worker_name)
    ]
)

try:
    # Execute query and fetch results
    query_job = bq_client.query(query,job_config=job_config)
    result = query_job[0] if len(query_job)>0 else None
    
    print(f"Worker lookup: {worker_name} → Found: {bool(result)}")
    print( dict(result) if result else None ) # Convert Row to dict
    
except Exception as e:
    print(f"Worker lookup failed: {str(e)}")
    print( None)