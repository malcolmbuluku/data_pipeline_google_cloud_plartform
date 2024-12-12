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
dataset_id = "savannahinformaticsassessment.savannah_informatics_assessment_data"  # Replace with your BigQuery dataset name
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
dataset_id = "savannahinformaticsassessment.savannah_informatics_assessment_data"    # Replace with your BigQuery dataset name
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

'''

'''
from google.cloud import bigquery  
from google.oauth2 import service_account  
  
credentialsPath = r'/home/malcolmbuluku/data_pipeline/credentials /credentials.json'  
credentials = service_account.Credentials.from_service_account_file(credentialsPath)  
client = bigquery.Client(credentials=credentials)  
table_id = "savannahinformaticsassessment.savannah_informatics_assessment_data"  
file_path = r"/home/malcolmbuluku/data_pipeline/scripts/filtered_users.csv"  
  
job_config = bigquery.LoadJobConfig(  
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  
    source_format=bigquery.SourceFormat.CSV,  
    field_delimiter=",",  
    skip_leading_rows=1,  
    # schema=schema_arr  
)  
  
with open(file_path, "rb") as source_file:  
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)  
  
job.result()  # Waits for the job to complete.  
  
table = client.get_table(table_id)  # Make an API request.  
print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))

'''



'''
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError, NotFound

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_csv_to_bigquery(credentials_path, table_id, file_path):
    """
    Load a CSV file into a BigQuery table.

    Args:
        credentials_path (str): Path to the GCP service account JSON key file.
        table_id (str): BigQuery table identifier in the format `project_id.dataset_name.table_name`.
        file_path (str): Path to the CSV file.

    Returns:
        None
    """
    try:
        # Authenticate using the service account
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials)

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=",",
            skip_leading_rows=1,
        )

        # Load the CSV file into BigQuery
        logging.info(f"Starting the load job for table: {table_id}")
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        # Wait for the job to complete
        job.result()
        logging.info("Data loaded successfully!")

        # Retrieve table details and print summary
        table = client.get_table(table_id)
        logging.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path}")
    except NotFound as e:
        logging.error(f"Table not found: {table_id}. Ensure the table exists or create it.")
    except GoogleAPIError as e:
        logging.error(f"Google API Error: {e.message}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
    finally:
        logging.info("Script execution completed.")

# Usage
if __name__ == "__main__":
    # Replace these paths with actual values
    credentials_path = r"/home/malcolmbuluku/data_pipeline/credentials /credentials.json"
    table_id = "savannahinformaticsassessment.savannah_informatics_assessment_data"
    file_path = r"/home/malcolmbuluku/data_pipeline/scripts/filtered_users.csv"

    load_csv_to_bigquery(credentials_path, table_id, file_path)

'''


'''

import csv
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError, NotFound

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def infer_schema_from_csv(file_path):
    """
    Infer the schema for BigQuery based on the CSV file header.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        list: A list of bigquery.SchemaField objects.
    """
    schema = []
    try:
        with open(file_path, mode="r", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            for column_name in reader.fieldnames:
                # Assuming all columns are strings by default (can be adjusted as needed)
                schema.append(bigquery.SchemaField(column_name, "STRING"))
            logging.info(f"Inferred schema: {[field.name for field in schema]}")
    except Exception as e:
        logging.error(f"Error inferring schema from CSV: {e}")
        raise
    return schema

def create_table_if_not_exists(client, table_id, schema):
    """
    Create a BigQuery table if it doesn't exist.

    Args:
        client (bigquery.Client): BigQuery client.
        table_id (str): BigQuery table identifier in the format `project_id.dataset_name.table_name`.
        schema (list): List of bigquery.SchemaField objects defining the table schema.

    Returns:
        None
    """
    try:
        client.get_table(table_id)  # Check if the table exists
        logging.info(f"Table {table_id} already exists.")
    except NotFound:
        # Table does not exist; create it
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        logging.info(f"Table {table_id} created successfully.")

def load_csv_to_bigquery(credentials_path, table_id, file_path):
    """
    Load a CSV file into a BigQuery table.

    Args:
        credentials_path (str): Path to the GCP service account JSON key file.
        table_id (str): BigQuery table identifier in the format `project_id.dataset_name.table_name`.
        file_path (str): Path to the CSV file.

    Returns:
        None
    """
    try:
        # Authenticate using the service account
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials)

        # Infer schema from CSV
        schema = infer_schema_from_csv(file_path)

        # Ensure the table exists
        logging.info(f"Checking if table {table_id} exists...")
        create_table_if_not_exists(client, table_id, schema)

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=",",
            skip_leading_rows=1,
        )

        # Load the CSV file into BigQuery
        logging.info(f"Starting the load job for table: {table_id}")
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        # Wait for the job to complete
        job.result()
        logging.info("Data loaded successfully!")

        # Retrieve table details and print summary
        table = client.get_table(table_id)
        logging.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
    except GoogleAPIError as e:
        logging.error(f"Google API Error: {e.message}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
    finally:
        logging.info("Script execution completed.")

# Usage
if __name__ == "__main__":
    # Replace these paths with actual values
    credentials_path = r"/home/malcolmbuluku/data_pipeline/credentials /credentials.json"
    table_id = "savannahinformaticsassessment.savannah_informatics_assessment_data.carts_table"
    file_path = r"/home/malcolmbuluku/data_pipeline/scripts/flattened_carts.csv"

    load_csv_to_bigquery(credentials_path, table_id, file_path)

'''


import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError, NotFound

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_table_if_not_exists(client, table_id):
    """
    Create a BigQuery table if it doesn't exist.

    Args:
        client (bigquery.Client): BigQuery client.
        table_id (str): BigQuery table identifier in the format `project_id.dataset_name.table_name`.

    Returns:
        None
    """
    try:
        client.get_table(table_id)  # Check if the table exists
        logging.info(f"Table {table_id} already exists.")
    except NotFound:
        # Table does not exist; create it
        table = bigquery.Table(table_id)
        client.create_table(table)
        logging.info(f"Table {table_id} created successfully.")

def load_csv_to_bigquery(credentials_path, table_id, file_path):
    """
    Load a CSV file into a BigQuery table with schema on write.

    Args:
        credentials_path (str): Path to the GCP service account JSON key file.
        table_id (str): BigQuery table identifier in the format `project_id.dataset_name.table_name`.
        file_path (str): Path to the CSV file.

    Returns:
        None
    """
    try:
        # Authenticate using the service account
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials)

        # Ensure the table exists
        logging.info(f"Checking if table {table_id} exists...")
        create_table_if_not_exists(client, table_id)

        # Configure the load job for schema on write
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrites the table
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=",",
            skip_leading_rows=1,
            autodetect=True,  # Enable schema detection
        )

        # Load the CSV file into BigQuery
        logging.info(f"Starting the load job for table: {table_id}")
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        # Wait for the job to complete
        job.result()
        logging.info("Data loaded successfully!")

        # Retrieve table details and print summary
        table = client.get_table(table_id)
        logging.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
    except GoogleAPIError as e:
        logging.error(f"Google API Error: {e.message}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
    finally:
        logging.info("Script execution completed.")

# Usage
if __name__ == "__main__":
    # Replace these paths with actual values
    credentials_path = r"/home/malcolmbuluku/data_pipeline/credentials /credentials.json"
    table_id = "savannahinformaticsassessment.savannah_informatics_assessment_data.products_table"
    file_path = r"/home/malcolmbuluku/data_pipeline/scripts/filtered_products.csv"

    load_csv_to_bigquery(credentials_path, table_id, file_path)



'''
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError, NotFound

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_table_if_not_exists(client, table_id):
    """
    Create a BigQuery table if it doesn't exist. 
    This version assumes schema is not predefined and allows schema on read logic.
    
    Args:
        client (bigquery.Client): BigQuery client.
        table_id (str): BigQuery table identifier in the format `project_id.dataset_name.table_name`.

    Returns:
        None
    """
    try:
        client.get_table(table_id)  # Check if the table exists
        logging.info(f"Table {table_id} already exists.")
    except NotFound:
        # Table does not exist; create it
        table = bigquery.Table(table_id)
        client.create_table(table)
        logging.info(f"Table {table_id} created successfully.")

def load_csv_to_bigquery(credentials_path, table_id, file_path):
    """
    Load a CSV file into a BigQuery table with schema on read. The schema will be inferred when querying.

    Args:
        credentials_path (str): Path to the GCP service account JSON key file.
        table_id (str): BigQuery table identifier in the format `project_id.dataset_name.table_name`.
        file_path (str): Path to the CSV file.

    Returns:
        None
    """
    try:
        # Authenticate using the service account
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials)

        # Ensure the table exists
        logging.info(f"Checking if table {table_id} exists...")
        create_table_if_not_exists(client, table_id)

        # Configure the load job for schema on write (no schema is provided)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append data to the table
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=",",
            skip_leading_rows=1,
            autodetect=False,  # Disable schema autodetect to let BigQuery handle schema on read
        )

        # Load the CSV file into BigQuery
        logging.info(f"Starting the load job for table: {table_id}")
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        # Wait for the job to complete
        job.result()
        logging.info("Data loaded successfully!")

        # Retrieve table details and print summary
        table = client.get_table(table_id)
        logging.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
    except GoogleAPIError as e:
        logging.error(f"Google API Error: {e.message}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
    finally:
        logging.info("Script execution completed.")

# Usage
if __name__ == "__main__":
    # Replace these paths with actual values
    credentials_path = r"/home/malcolmbuluku/data_pipeline/credentials /credentials.json"
    table_id = "savannahinformaticsassessment.savannah_informatics_assessment_data.carts_table"
    file_path = r"/home/malcolmbuluku/data_pipeline/scripts/flattened_carts.csv"

    load_csv_to_bigquery(credentials_path, table_id, file_path)

'''