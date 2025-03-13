'''
Loading Data to BigQuery
The final step in the ETL pipeline is to load the transformed data into BigQuery.
The load_csv_to_bigquery function loads a CSV file into a BigQuery table.
It first infers the schema from the CSV file header and creates the table if it doesn't exist.
The function then loads the CSV file into the table using the BigQuery client library.
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








