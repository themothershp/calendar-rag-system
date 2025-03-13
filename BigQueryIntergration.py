from google.cloud import bigquery
from google.oauth2 import service_account
from CoreDatamodels import User,Worker,Appointment
from typing import List, Dict, Any
import os
import json

class BigQueryClient:
    def __init__(self, credentials_path='service-account.json'):
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.client = bigquery.Client(
            credentials=self.credentials,
            project=self.credentials.project_id
        )
    
    def initialize_database(self):
    # Create dataset if it doesn't exist
        dataset_id = "calendar_system"
        dataset_ref = self.client.dataset(dataset_id)
        
        try:
            self.client.get_dataset(dataset_ref)
            print(f"Dataset {dataset_id} already exists.")
        except Exception as e:
            print(f"Dataset {dataset_id} not found. Creating it...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            print(f"Dataset {dataset_id} created.")

        # Define table schemas
        tables = {
            'users': [
                bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("phone", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("timezone", "STRING", mode="REQUIRED"),
            ],
            'workers': [
                bigquery.SchemaField("worker_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("role", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("working_hours", "RECORD", mode="REQUIRED", fields=[
                    bigquery.SchemaField("start", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("end", "STRING", mode="REQUIRED")
                ]),
                bigquery.SchemaField("timezone", "STRING", mode="REQUIRED"),
            ],
            'appointments': [
                bigquery.SchemaField("appointment_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("worker_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("start_time", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("end_time", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "DATETIME", mode="REQUIRED"),
            ]
        }

        # Create tables if they don't exist
        for table_name, schema in tables.items():
            table_ref = dataset_ref.table(table_name)
            try:
                self.client.get_table(table_ref)
                print(f"Table {table_name} already exists.")
            except Exception as e:
                print(f"Table {table_name} not found. Creating it...")
                table = bigquery.Table(table_ref, schema=schema)
                self.client.create_table(table)
                print(f"Table {table_name} created.")
                
    def insert_data(self, table_name: str, data: List[Dict[str, Any]]):

        print("Sample data being inserted:", json.dumps(data[0], indent=2))

        table_ref = self.client.dataset('calendar_system').table(table_name)
        table = self.client.get_table(table_ref)
        
        errors = self.client.insert_rows_json(table, data)
        if errors:
            raise RuntimeError(f"BigQuery insertion errors: {errors}")

    def query(self, query: str, job_config=None) -> list:  # Add job_config parameter
        query_job = self.client.query(query, job_config=job_config)  # Pass job_config
        return query_job

    def list_datasets_and_tables(self):
        print("Listing datasets and tables:")
        for dataset in self.client.list_datasets():
            print(f"Dataset: {dataset.dataset_id}")
            for table in self.client.list_tables(dataset.reference):
                print(f"  Table: {table.table_id}")

    def insert_rows_json(self, table_id: str, rows: list) -> list:
        """Insert JSON rows into a BigQuery table"""
        table_ref = self.client.get_table(table_id)
        errors = self.client.insert_rows_json(table_ref, rows)
        if errors:
            raise RuntimeError(f"Insert errors: {errors}")
        return errors