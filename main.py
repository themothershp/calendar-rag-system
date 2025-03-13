# main.py
from BigQueryIntergration import BigQueryClient
from sampleDataGeneration import DataGenerator

def initialize_system():
    # Initialize BigQuery
    bq = BigQueryClient()
    bq.initialize_database()
    
    # Generate sample data
    dg = DataGenerator()
    users = dg.generate_users(50)
    workers = dg.generate_workers(10)
    appointments = dg.generate_appointments(users, workers, 200)
    
    # Insert data
    bq.insert_data('users', [u.model_dump() for u in users])
    bq.insert_data('workers', [w.model_dump() for w in workers])
    bq.insert_data('appointments', appointments)

if __name__ == "__main__":
    initialize_system()
    print("System initialized successfully!")