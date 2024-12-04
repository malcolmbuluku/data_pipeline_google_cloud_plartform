'''
import requests
import json
from google.cloud import storage

# Define the API endpoints
API_ENDPOINTS = {
    "users": "https://dummyjson.com/users",
    "products": "https://dummyjson.com/products",
    "carts": "https://dummyjson.com/carts"
}

def fetch_and_save_to_gcs(api_name, gcs_bucket):
    """
    Fetch data from an API endpoint and save it as a separate object in a GCS bucket.
    
    Args:
        api_name (str): The key of the API endpoint to fetch data from.
        gcs_bucket (str): Name of the GCS bucket to upload the file to.
    
    Returns:
        dict: The fetched data.
    """
    # Fetch data from the API
    response = requests.get(API_ENDPOINTS[api_name])
    response.raise_for_status()
    data = response.json()

    # Convert data to a JSON string
    data_json = json.dumps(data, indent=4)

    # Initialize GCS client
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)

    # Upload data to GCS
    blob_name = f"raw/{api_name}_raw.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data_json, content_type="application/json")
    print(f"Data uploaded to GCS bucket '{gcs_bucket}' at '{blob_name}'")

    return data

if __name__ == "__main__":
    GCS_BUCKET = "savannah_informatics_assesment" 

    # Loop through all API endpoints and save data separately
    for api_name in API_ENDPOINTS.keys():
        fetch_and_save_to_gcs(api_name, GCS_BUCKET)

'''

import requests
import json
import logging
from google.cloud import storage
from requests.exceptions import RequestException, HTTPError, Timeout

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the API endpoints
API_ENDPOINTS = {
    "users": "https://dummyjson.com/users",
    "products": "https://dummyjson.com/products",
    "carts": "https://dummyjson.com/carts"
}

def fetch_and_save_to_gcs(api_name, gcs_bucket):
    """
    Fetch data from an API endpoint and save it as a separate object in a GCS bucket.
    
    Args:
        api_name (str): The key of the API endpoint to fetch data from.
        gcs_bucket (str): Name of the GCS bucket to upload the file to.
    
    Returns:
        dict: The fetched data, or None in case of failure.
    """
    try:
        # Fetch data from the API
        logging.info(f"Fetching data from API endpoint: {API_ENDPOINTS[api_name]}")
        response = requests.get(API_ENDPOINTS[api_name], timeout=10)
        response.raise_for_status()  # Will raise an HTTPError for bad responses (4xx, 5xx)
        data = response.json()
    except Timeout:
        logging.error(f"Request to {API_ENDPOINTS[api_name]} timed out.")
        return None
    except HTTPError as http_err:
        logging.error(f"HTTP error occurred while accessing {API_ENDPOINTS[api_name]}: {http_err}")
        return None
    except RequestException as req_err:
        logging.error(f"Request error occurred: {req_err}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON response from {API_ENDPOINTS[api_name]}.")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None

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
        return None

    return data

if __name__ == "__main__":
    GCS_BUCKET = "savannah_informatics_assesment" 

    # Loop through all API endpoints and save data separately
    for api_name in API_ENDPOINTS.keys():
        result = fetch_and_save_to_gcs(api_name, GCS_BUCKET)
        if result is None:
            logging.error(f"Failed to process data for {api_name}.")
        else:
            logging.info(f"Successfully processed data for {api_name}.")
