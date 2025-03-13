from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError, NotFound
import requests
import json
import logging
from requests.exceptions import RequestException, HTTPError, Timeout

# Define the API endpoints
API_ENDPOINTS = {
    "users": "https://dummyjson.com/users",
    "products": "https://dummyjson.com/products",
    "carts": "https://dummyjson.com/carts"
}

# Define the GCS bucket name
GCS_BUCKET = "savannah_informatics_assesment"

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_and_save_to_gcs(api_name, gcs_bucket):
    """
    Fetch data from an API endpoint and save it as a separate object in a GCS bucket.

    Args:
        api_name (str): The key of the API endpoint to fetch data from.
        gcs_bucket (str): Name of the GCS bucket to upload the file to.

    Returns:
        None
    """
    try:
        # Fetch data from the API
        logging.info(f"Fetching data from API endpoint: {API_ENDPOINTS[api_name]}")
        response = requests.get(API_ENDPOINTS[api_name], timeout=10)
        response.raise_for_status()  # Will raise an HTTPError for bad responses (4xx, 5xx)
        data = response.json()
    except Timeout:
        logging.error(f"Request to {API_ENDPOINTS[api_name]} timed out.")
        return
    except HTTPError as http_err:
        logging.error(f"HTTP error occurred while accessing {API_ENDPOINTS[api_name]}: {http_err}")
        return
    except RequestException as req_err:
        logging.error(f"Request error occurred: {req_err}")
        return
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON response from {API_ENDPOINTS[api_name]}.")
        return
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return

    # Convert data to a JSON string
    data_json = json.dumps(data, indent=4)

    # Initialize GCS client
    try:
        client = storage.Client()
        bucket = client.bucket(gcs_bucket)

        # Upload data to GCS
        blob_name = f"raw/{api_name}_raw.json"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data_json, content_type="application/json")
        logging.info(f"Data uploaded to GCS bucket '{gcs_bucket}' at '{blob_name}'")
    except Exception as e:
        logging.error(f"Error uploading data to GCS: {e}")

def perform_transformation_task(source_blob, target_blob):
    """
    Transform data from a source blob and save the transformed data to a target blob in GCS.

    Args:
        source_blob (str): Path to the source blob in GCS.
        target_blob (str): Path to the target blob in GCS.

    Returns:
        None
    """
    try:
        logging.info(f"Transforming data from source blob '{source_blob}'")
        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)

        # Download source data
        source_data = bucket.blob(source_blob).download_as_text()
        source_json = json.loads(source_data)

        # Perform transformation (example: filter or modify the data)
        transformed_data = {
            "transformed_items": [item for item in source_json.get("products", []) if item.get("price", 0) > 100]
        }

        # Convert transformed data to JSON string
        transformed_json = json.dumps(transformed_data, indent=4)

        # Save transformed data to target blob
        target_blob_instance = bucket.blob(target_blob)
        target_blob_instance.upload_from_string(transformed_json, content_type="application/json")
        logging.info(f"Transformed data saved to GCS bucket '{GCS_BUCKET}' at '{target_blob}'")
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")

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

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'extract_json_to_gcs',
    default_args=default_args,
    description='A DAG to extract JSON data from APIs and save to GCS',
    schedule_interval=None,  # Trigger manually or set a schedule
    catchup=False,
)

# Create tasks for each API endpoint
def create_task(api_name):
    return PythonOperator(
        task_id=f'fetch_and_save_{api_name}',
        python_callable=fetch_and_save_to_gcs,
        op_kwargs={'api_name': api_name, 'gcs_bucket': GCS_BUCKET},
        dag=dag,
    )

# Create the transformation task
def create_transformation_task():
    return PythonOperator(
        task_id='transform_data',
        python_callable=perform_transformation_task,
        op_kwargs={'source_blob': 'raw/products_raw.json', 'target_blob': 'transformed/products_transformed.json'},
        dag=dag,
    )

# Create the loading task
def create_loading_task():
    return PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_csv_to_bigquery,
        op_kwargs={
            'credentials_path': '/home/malcolmbuluku/data_pipeline/credentials/credentials.json',
            'table_id': 'savannahinformaticsassessment.savannah_informatics_assessment_data.products_table',
            'file_path': '/home/malcolmbuluku/data_pipeline/scripts/filtered_products.csv',
        },
        dag=dag,
    )

# Add tasks to the DAG
user_task = create_task("users")
product_task = create_task("products")
cart_task = create_task("carts")
transformation_task = create_transformation_task()
loading_task = create_loading_task()

# Set task dependencies
user_task >> product_task >> cart_task >> transformation_task >> loading_task

# Additional script functions
def additional_task_function():
    """
    This is a function for additional functionality.
    """
    logging.info("Running an additional task function.")

def upload_logs_to_gcs(logs_path, gcs_bucket):
    """
    Upload logs from a file to a GCS bucket.

    Args:
        logs_path (str): Path to the log file.
        gcs_bucket (str): Name of the GCS bucket to upload the log file to.

    Returns:
        None
    """
    try:
        client = storage.Client()
        bucket = client.bucket(gcs_bucket)

        blob_name = "logs/execution_logs.txt"
        blob = bucket.blob(blob_name)
        with open(logs_path, "r") as log_file:
            blob.upload_from_file(log_file, content_type="text/plain")
        logging.info(f"Logs uploaded to GCS bucket '{gcs_bucket}' at '{blob_name}'")
    except Exception as e:
        logging.error(f"Error uploading logs to GCS: {e}")
