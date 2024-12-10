'''

from google.cloud import bigquery
import os

# Define your project and dataset details
project_id = "savannahinformaticsassessment"  # Replace with your GCP project ID
dataset_id = "filtered_users.csv"    # Replace with your BigQuery dataset name
table_id = "users_table"            # Desired table name in BigQuery
csv_file_path = "filtered_users.csv"  # Path to the CSV file

# Initialize BigQuery client
client = bigquery.Client(project=project_id)

# Construct the full table ID
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Define the schema for the table
schema = [
    bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("gender", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("street", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("postal_code", "STRING", mode="REQUIRED"),
]

# Load the CSV file into BigQuery
job_config = bigquery.LoadJobConfig(
    schema=schema,
    skip_leading_rows=1,  # Skip the header row in the CSV file
    source_format=bigquery.SourceFormat.CSV,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite the table if it exists
)

# Load data to BigQuery
with open(csv_file_path, "rb") as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_ref,
        job_config=job_config,
    )

# Wait for the job to complete
load_job.result()

# Verify the table creation
table = client.get_table(table_ref)
print(f"Loaded {table.num_rows} rows into {table.project}:{table.dataset_id}.{table.table_id}.")

'''


'''
from google.cloud import bigquery

# Define your project and dataset details
project_id = "savannahinformaticsassessment"  # Replace with your GCP project ID
dataset_id = "filtered_users.csv"  # Replace with your BigQuery dataset name
table_id = "users_table"  # Desired table name in BigQuery
csv_file_path = "/home/malcolmbuluku/data_pipeline/scripts/filtered_users.csv"  # Path to the CSV file

# Initialize BigQuery client
client = bigquery.Client(project=project_id)

# Ensure the dataset exists
dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
try:
    client.get_dataset(dataset_ref)
except Exception:
    print(f"Dataset {dataset_id} does not exist. Creating it...")
    client.create_dataset(dataset_ref)
    print(f"Dataset {dataset_id} created.")

# Construct the full table ID
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Define the schema for the table
schema = [
    bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("gender", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("street", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("postal_code", "STRING", mode="REQUIRED"),
]

# Configure the job to load the CSV into BigQuery
job_config = bigquery.LoadJobConfig(
    schema=schema,
    skip_leading_rows=1,  # Skip the header row in the CSV file
    source_format=bigquery.SourceFormat.CSV,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite the table if it exists
)

# Load the CSV file into BigQuery
print(f"Loading {csv_file_path} into BigQuery table {table_ref}...")
with open(csv_file_path, "rb") as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_ref,
        job_config=job_config,
    )

# Wait for the load job to complete
load_job.result()
print("Data loaded successfully.")

# Verify the table creation
table = client.get_table(table_ref)
print(f"Loaded {table.num_rows} rows into {table.project}:{table.dataset_id}.{table.table_id}.")

'''

'''
from google.cloud import bigquery
import os

# Define your project and dataset details
project_id = "data_pipeline"  # Replace with your GCP project ID
dataset_id = "filtered_users.csv"    # Replace with your BigQuery dataset name
table_id = "users_table"            # Desired table name in BigQuery
csv_file_path = "filtered_users.csv"  # Path to the CSV file

# Initialize BigQuery client
client = bigquery.Client(project=project_id)

# Construct the full table ID
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Define the schema for the table
schema = [
    bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("gender", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("street", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("postal_code", "STRING", mode="REQUIRED"),
]

# Load the CSV file into BigQuery
job_config = bigquery.LoadJobConfig(
    schema=schema,
    skip_leading_rows=1,  # Skip the header row in the CSV file
    source_format=bigquery.SourceFormat.CSV,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite the table if it exists
)

# Load data to BigQuery
with open(csv_file_path, "rb") as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_ref,
        job_config=job_config,
    )

# Wait for the job to complete
load_job.result()

# Verify the table creation
table = client.get_table(table_ref)
print(f"Loaded {table.num_rows} rows into {table.project}:{table.dataset_id}.{table.table_id}.")

'''
'''
from google.cloud import bigquery

# Define your project, dataset, and table details
project_id = "savannahinformaticsassessment"  # Replace with your GCP project ID
dataset_id = "user_data"  # Replace with your BigQuery dataset name
table_id = "users_table"  # Table name to create
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Initialize the BigQuery client
client = bigquery.Client(project=project_id)

# Define the schema for the table
schema = [
    bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("gender", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("street", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("postal_code", "STRING", mode="REQUIRED"),
]

# Define the dataset reference
dataset_ref = f"{project_id}.{dataset_id}"

# Create the dataset if it does not exist
try:
    client.get_dataset(dataset_ref)
    print(f"Dataset {dataset_id} already exists.")
except Exception:
    print(f"Dataset {dataset_id} does not exist. Creating it...")
    dataset = bigquery.Dataset(dataset_ref)
    client.create_dataset(dataset)
    print(f"Dataset {dataset_id} created.")

# Create the table
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table)

print(f"Table {table.table_id} created in dataset {dataset_id}.")

'''

import json
from google.cloud import storage, bigquery

# Initialize GCS and BigQuery clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Define your GCS bucket and file details
bucket_name = "savannah_informatics_assesment"  # Replace with your GCS bucket name
file_name = "raw/users_raw.json"  # Replace with the path to the JSON file in your bucket

# Define the BigQuery project, dataset, and table
project_id = "savannahinformaticsassessment"  # Replace with your GCP project ID
dataset_id = "users_table"    # Replace with your BigQuery dataset name
table_id = "users_table"            # Desired table name in BigQuery

# Fetch the JSON file from the GCS bucket
bucket = storage_client.get_bucket(bucket_name)
blob = bucket.blob(file_name)
json_data = blob.download_as_text()

# Parse the JSON data
data = json.loads(json_data)

# Flatten the users
users = data['users']
flat_data = []

for user in users:
    flat_data.append({
        'user_id': user['id'],
        'first_name': user['firstName'],
        'last_name': user['lastName'],
        'gender': user['gender'],
        'age': user['age'],
        'street': user['address']['address'],
        'city': user['address']['city'],
        'postal_code': user['address']['postalCode'],
    })

# Define the schema for the table in BigQuery
schema = [
    bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("gender", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("street", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("postal_code", "STRING", mode="REQUIRED"),
]

# Load the data into BigQuery
table_ref = f"{project_id}.{dataset_id}.{table_id}"
job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite the table if it exists
)

# Convert flat_data to a DataFrame
import pandas as pd
df = pd.DataFrame(flat_data)

# Load DataFrame directly into BigQuery
load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)

# Wait for the job to complete
load_job.result()

# Verify the table creation
table = bq_client.get_table(table_ref)
print(f"Loaded {table.num_rows} rows into {table.project}:{table.dataset_id}.{table.table_id}.")

