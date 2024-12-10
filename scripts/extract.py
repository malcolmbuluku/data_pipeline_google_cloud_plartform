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
    GCS_BUCKET = "" 

    # Loop through all API endpoints and save data separately
    for api_name in API_ENDPOINTS.keys():
        fetch_and_save_to_gcs(api_name, GCS_BUCKET)

'''


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

'''

'''
import requests
import pandas as pd

# Fetch the data
response = requests.get("https://dummyjson.com/carts")
data = response.json()

# Flatten the carts and products
carts = data['carts']
flat_data = []

for cart in carts:
    for product in cart['products']:
        flat_data.append({
            'cart_id': cart['id'],
            'user_id': cart['userId'],
            'total_cart_value': cart['total'],
            'discounted_total': cart['discountedTotal'],
            'total_products_in_cart': cart['totalProducts'],
            'total_quantity': cart['totalQuantity'],
            'product_id': product['id'],
            'product_title': product['title'],
            'product_price': product['price'],
            'product_quantity': product['quantity'],
            'product_total': product['total'],
            'discount_percentage': product['discountPercentage'],
            'discounted_price': product['discountedPrice']
        })

# Convert to a DataFrame for further processing or saving
df = pd.DataFrame(flat_data)

# Save to a CSV or view the DataFrame
df.to_csv('flattened_carts.csv', index=False)
print(df.head())

'''

'''
from google.cloud import storage
import pandas as pd
import json

# Initialize the Google Cloud Storage client
client = storage.Client()

# Define your bucket and file name
bucket_name = "savannah_informatics_assesment"
file_name = "raw/carts_raw.json"

# Get the bucket and blob
bucket = client.bucket(bucket_name)
blob = bucket.blob(file_name)

# Download the JSON file as a string
data_string = blob.download_as_text()

# Parse the JSON data
data = json.loads(data_string)

# Flatten the carts and products
carts = data['carts']
flat_data = []

for cart in carts:
    for product in cart['products']:
        flat_data.append({
            'cart_id': cart['id'],
            'user_id': cart['userId'],
            'total_cart_value': cart['total'],
            'discounted_total': cart['discountedTotal'],
            'total_products_in_cart': cart['totalProducts'],
            'total_quantity': cart['totalQuantity'],
            'product_id': product['id'],
            'product_title': product['title'],
            'product_price': product['price'],
            'product_quantity': product['quantity'],
            'product_total': product['total'],
            'discount_percentage': product['discountPercentage'],
#            'discounted_price': product['discountedPrice']
        })

# Convert to a DataFrame for further processing or saving
df = pd.DataFrame(flat_data)

# Save to a CSV or view the DataFrame
df.to_csv('flattened_carts.csv', index=False)
print(df.head())

'''

'''
from google.cloud import storage
import pandas as pd
import json

# Initialize the Google Cloud Storage client
client = storage.Client()

# Define your bucket and file name
bucket_name = "savannah_informatics_assesment"
file_name = "raw/carts_raw.json"

# Get the bucket and blob
bucket = client.bucket(bucket_name)
blob = bucket.blob(file_name)

# Download the JSON file as a string
data_string = blob.download_as_text()

# Parse the JSON data
data = json.loads(data_string)

# Flatten the carts and products
carts = data['carts']
flat_data = []

for cart in carts:
    for product in cart['products']:
        flat_data.append({
            'cart_id': cart['id'],
            'user_id': cart['userId'],
            'product_id': product['id'],
            'quantity': product['quantity'],
            'price': product['price'],
            'total_cart_value': cart['total']
        })

# Convert to a DataFrame for further processing or saving
df = pd.DataFrame(flat_data)

# Save to a CSV or view the DataFrame
df.to_csv('flattened_carts.csv', index=False)
print(df.head())

'''

'''
from google.cloud import storage
import pandas as pd
import json

# Initialize the Google Cloud Storage client
client = storage.Client()

# Define your bucket and file name
bucket_name = "savannah_informatics_assesment"
file_name = "raw/products_raw.json"

# Get the bucket and blob
bucket = client.bucket(bucket_name)
blob = bucket.blob(file_name)

# Download the JSON file as a string
data_string = blob.download_as_text()

# Parse the JSON data
data = json.loads(data_string)

# Extract and filter products
products = data['products']
filtered_products = [
    {
        'product_id': product['id'],
        'name': product['title'],
        'category': product['category'],
        'brand': product['brand'],
        'price': product['price']
    }
    for product in products if product['price'] > 50
]

# Convert to a DataFrame for further processing or saving
df = pd.DataFrame(filtered_products)

# Save to a CSV or view the DataFrame
df.to_csv('filtered_products.csv', index=False)
print(df.head())

'''



import pandas as pd
from google.cloud import storage
import json

# Initialize GCS client
client = storage.Client()

# Define the bucket and file details
bucket_name = "savannah_informatics_assesment"  # Replace with your GCS bucket name
file_name = "raw/users_raw.json"  # Replace with the path to the JSON file in your bucket

# Fetch the JSON file from the GCS bucket
bucket = client.get_bucket(bucket_name)
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

# Convert to a DataFrame for further processing or saving
df = pd.DataFrame(flat_data)

# Save to a CSV or view the DataFrame
output_file_name = "filtered_users.csv"
df.to_csv(output_file_name, index=False)

print(f"Data saved to {output_file_name}")
print(df.head())

