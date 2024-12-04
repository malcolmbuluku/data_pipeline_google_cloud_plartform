'''
import json
from google.cloud import storage
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_json_from_gcs(bucket_name, blob_name):
    """
    Download a JSON file from a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the bucket.

    Returns:
        list: JSON data as a list of objects.
    """
    try:
        logging.info(f"Downloading JSON from GCS bucket '{bucket_name}', blob '{blob_name}'")
        client = storage.Client()  # Use default credentials in Cloud Shell
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        json_data = json.loads(blob.download_as_text())
        logging.info("JSON data successfully downloaded.")
        return json_data
    except Exception as e:
        logging.error(f"Failed to download JSON from GCS: {e}")
        raise

def flatten_json(json_data):
    """
    Flatten JSON data into a tabular format.

    Args:
        json_data (list): List of JSON objects.

    Returns:
        pd.DataFrame: Flattened data.
    """
    try:
        logging.info("Flattening JSON data.")
        # Normalize JSON and flatten nested fields
        flat_data = pd.json_normalize(
            json_data,
            sep='_'
        )
        # Select required fields if they exist
        required_fields = ['id', 'firstName', 'lastName', 'gender', 'age', 
                           'address.street', 'address.city', 'address.postalCode']
        missing_fields = [field for field in required_fields if field not in flat_data.columns]
        if missing_fields:
            logging.warning(f"Missing fields in JSON data: {missing_fields}")
        
        # Keep only the fields that exist in the data
        flat_data = flat_data[[field for field in required_fields if field in flat_data.columns]]

        # Rename address fields for clarity
        flat_data.rename(
            columns={
                'address.street': 'street',
                'address.city': 'city',
                'address.postalCode': 'postal_code',
                'firstName': 'first_name',
                'lastName': 'last_name',
            }, inplace=True
        )
        logging.info("JSON data successfully flattened.")
        return flat_data
    except Exception as e:
        logging.error(f"Failed to flatten JSON data: {e}")
        raise

def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_file (str): Path to save the CSV file.
    """
    try:
        logging.info(f"Saving data to CSV file '{output_file}'")
        df.to_csv(output_file, index=False)
        logging.info(f"Flattened data successfully saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save data to CSV: {e}")
        raise

if __name__ == "__main__":
    # GCS bucket and file details
    bucket_name = "savannah_informatics_assesment"  # Replace with your GCS bucket name
    blob_name = "raw/users_raw.json"         # Replace with your JSON file name
    output_file = "users.csv"  # Output file name in Cloud Shell

    try:
        # Download JSON data from GCS
        json_data = download_json_from_gcs(bucket_name, blob_name)
        
        # Flatten the JSON data
        flattened_data = flatten_json(json_data)
        
        # Save the flattened data to CSV
        save_to_csv(flattened_data, output_file)
    except Exception as e:
        logging.error(f"An error occurred in the main process: {e}")

'''



'''
import json
from google.cloud import storage
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_json_from_gcs(bucket_name, blob_name):
    """
    Download a JSON file from a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the bucket.

    Returns:
        dict: JSON data or None if download fails.
    """
    try:
        logging.info(f"Downloading JSON from GCS bucket '{bucket_name}', blob '{blob_name}'")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Check if the blob exists
        if not blob.exists():
            logging.error(f"The object '{blob_name}' does not exist in the bucket '{bucket_name}'.")
            return None

        # Download and parse JSON
        json_data = json.loads(blob.download_as_text())
        logging.info("JSON data successfully downloaded.")
        return json_data
    except Exception as e:
        logging.error(f"Failed to download JSON from GCS: {e}")
        return None

def process_products(json_data):
    """
    Process the products JSON data to extract required fields and filter rows.

    Args:
        json_data (list): List of JSON objects.

    Returns:
        pd.DataFrame: Processed DataFrame or None if processing fails.
    """
    if not json_data:
        logging.error("No data to process.")
        return None

    try:
        # Normalize JSON data into a DataFrame
        logging.info("Normalizing JSON data into a DataFrame.")
        products_df = pd.json_normalize(json_data)

        # Ensure the required fields are in the DataFrame
        required_fields = ['product_id', 'name', 'category', 'brand', 'price']
        missing_fields = [field for field in required_fields if field not in products_df.columns]

        if missing_fields:
            logging.error(f"Missing required fields: {', '.join(missing_fields)}")
            return None

        # Select required fields and filter out products with price <= 50
        products_df = products_df[required_fields]
        filtered_df = products_df[products_df['price'] > 50]

        logging.info(f"Filtered products count: {len(filtered_df)}")
        return filtered_df
    except Exception as e:
        logging.error(f"Error processing products data: {e}")
        return None

def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_file (str): Path to save the CSV file.
    """
    try:
        logging.info(f"Saving filtered data to CSV file '{output_file}'.")
        df.to_csv(output_file, index=False)
        logging.info(f"Filtered products saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save DataFrame to CSV: {e}")

if __name__ == "__main__":
    # GCS bucket and file details
    bucket_name = "savannah_informatics_assesment"
    blob_name = "raw/products_raw.json"  # Ensure this is the correct path
    output_file = "products.csv"  # Output file name

    # Download JSON data from GCS
    json_data = download_json_from_gcs(bucket_name, blob_name)

    if json_data is not None:
        # Process the products data
        filtered_data = process_products(json_data)

        if filtered_data is not None:
            # Save the filtered data to CSV
            save_to_csv(filtered_data, output_file)
        else:
            logging.error("Product processing failed. No data to save.")
    else:
        logging.error("Failed to download or process the product data.")

'''



'''
import json
from google.cloud import storage
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_json_from_gcs(bucket_name, blob_name):
    """
    Download a JSON file from a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the bucket.

    Returns:
        list: JSON data as a list of dictionaries or None if download fails.
    """
    try:
        logging.info(f"Downloading JSON from GCS bucket '{bucket_name}', blob '{blob_name}'")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Check if the blob exists
        if not blob.exists():
            logging.error(f"The object '{blob_name}' does not exist in the bucket '{bucket_name}'.")
            return None

        # Download and parse JSON
        json_data = json.loads(blob.download_as_text())
        if not isinstance(json_data, list):
            logging.error("The JSON data is not in the expected list format.")
            return None

        logging.info("JSON data successfully downloaded.")
        return json_data
    except Exception as e:
        logging.error(f"Failed to download JSON from GCS: {e}")
        return None

def process_products(json_data):
    """
    Process the products JSON data to extract required fields, normalize, and filter rows.

    Args:
        json_data (list): List of JSON objects.

    Returns:
        pd.DataFrame: Cleaned and filtered DataFrame or None if processing fails.
    """
    if not json_data:
        logging.error("No data to process.")
        return None

    try:
        # Normalize JSON data into a DataFrame
        logging.info("Normalizing JSON data into a DataFrame.")
        products_df = pd.json_normalize(json_data)

        # Ensure the required fields are present
        required_fields = ['product_id', 'name', 'category', 'brand', 'price']
        missing_fields = [field for field in required_fields if field not in products_df.columns]

        if missing_fields:
            logging.error(f"Missing required fields: {', '.join(missing_fields)}")
            return None

        # Select required fields
        products_df = products_df[required_fields]

        # Handle missing or invalid price values
        products_df['price'] = pd.to_numeric(products_df['price'], errors='coerce')
        products_df = products_df.dropna(subset=['price'])

        # Filter out products with price <= 50
        filtered_df = products_df[products_df['price'] > 50]

        logging.info(f"Filtered products count: {len(filtered_df)}")
        return filtered_df
    except Exception as e:
        logging.error(f"Error processing products data: {e}")
        return None

def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_file (str): Path to save the CSV file.
    """
    try:
        logging.info(f"Saving filtered data to CSV file '{output_file}'.")
        df.to_csv(output_file, index=False)
        logging.info(f"Filtered products saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save DataFrame to CSV: {e}")

if __name__ == "__main__":
    # GCS bucket and file details
    bucket_name = "savannah_informatics_assesment"
    blob_name = "raw/products_raw.json"  # Ensure this is the correct path
    output_file = "products.csv"  # Output file name

    # Download JSON data from GCS
    json_data = download_json_from_gcs(bucket_name, blob_name)

    if json_data is not None:
        # Process the products data
        filtered_data = process_products(json_data)

        if filtered_data is not None:
            # Save the filtered data to CSV
            save_to_csv(filtered_data, output_file)
        else:
            logging.error("Product processing failed. No data to save.")
    else:
        logging.error("Failed to download or process the product data.")

'''

'''
import json
from google.cloud import storage
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_json_from_gcs(bucket_name, blob_name):
    """
    Download a JSON file from a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the bucket.

    Returns:
        list: JSON data as a list of dictionaries or None if download fails.
    """
    try:
        logging.info(f"Downloading JSON from GCS bucket '{bucket_name}', blob '{blob_name}'")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Check if the blob exists
        if not blob.exists():
            logging.error(f"The object '{blob_name}' does not exist in the bucket '{bucket_name}'.")
            return None

        # Download and parse JSON
        json_data = json.loads(blob.download_as_text())

        # Handle cases where JSON is not a list
        if isinstance(json_data, dict):
            # Attempt to extract a list of products from a known key
            if 'products' in json_data:
                json_data = json_data['products']
            else:
                logging.error("The JSON data is a dictionary but does not contain the expected 'products' key.")
                return None

        if not isinstance(json_data, list):
            logging.error("The JSON data is not in the expected list format.")
            return None

        logging.info("JSON data successfully downloaded.")
        return json_data
    except Exception as e:
        logging.error(f"Failed to download JSON from GCS: {e}")
        return None

def process_products(json_data):
    """
    Process the products JSON data to extract required fields, normalize, and filter rows.

    Args:
        json_data (list): List of JSON objects.

    Returns:
        pd.DataFrame: Cleaned and filtered DataFrame or None if processing fails.
    """
    if not json_data:
        logging.error("No data to process.")
        return None

    try:
        # Normalize JSON data into a DataFrame
        logging.info("Normalizing JSON data into a DataFrame.")
        products_df = pd.json_normalize(json_data)

        # Ensure the required fields are present
        required_fields = ['id', 'title', 'category', 'brand', 'price']
        missing_fields = [field for field in required_fields if field not in products_df.columns]

        if missing_fields:
            logging.error(f"Missing required fields: {', '.join(missing_fields)}")
            return None

        # Select required fields
        products_df = products_df[required_fields]

        # Handle missing or invalid price values
        products_df['price'] = pd.to_numeric(products_df['price'], errors='coerce')
        products_df = products_df.dropna(subset=['price'])

        # Filter out products with price <= 50
        filtered_df = products_df[products_df['price'] > 50]

        logging.info(f"Filtered products count: {len(filtered_df)}")
        return filtered_df
    except Exception as e:
        logging.error(f"Error processing products data: {e}")
        return None

def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_file (str): Path to save the CSV file.
    """
    try:
        logging.info(f"Saving filtered data to CSV file '{output_file}'.")
        df.to_csv(output_file, index=False)
        logging.info(f"Filtered products saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save DataFrame to CSV: {e}")

if __name__ == "__main__":
    # GCS bucket and file details
    bucket_name = "savannah_informatics_assesment"
    blob_name = "raw/products_raw.json"  # Ensure this is the correct path
    output_file = "products.csv"  # Output file name

    # Download JSON data from GCS
    json_data = download_json_from_gcs(bucket_name, blob_name)

    if json_data is not None:
        # Process the products data
        filtered_data = process_products(json_data)

        if filtered_data is not None:
            # Save the filtered data to CSV
            save_to_csv(filtered_data, output_file)
        else:
            logging.error("Product processing failed. No data to save.")
    else:
        logging.error("Failed to download or process the product data.")

'''


'''
import json
from google.cloud import storage
import pandas as pd


def download_json_from_gcs(bucket_name, blob_name):
    """
    Download a JSON file from a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the bucket.

    Returns:
        list: JSON data as a list of dictionaries.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        json_data = json.loads(blob.download_as_text())
        return json_data
    except Exception as e:
        raise RuntimeError(f"Error downloading or parsing JSON file: {e}")


def process_cart_data(json_data):
    """
    Process the cart JSON data to flatten the products array and calculate total cart value.

    Args:
        json_data (list): List of JSON objects representing carts.

    Returns:
        pd.DataFrame: Processed DataFrame with flattened product rows and total cart values.
    """
    try:
        # Convert JSON data to DataFrame
        carts_df = pd.json_normalize(json_data, record_path=['products'], meta=['cart_id', 'user_id'], sep='_')

        # Ensure required fields are present
        required_fields = ['cart_id', 'user_id', 'product_id', 'quantity', 'price']
        for field in required_fields:
            if field not in carts_df.columns:
                raise KeyError(f"Missing required field: {field}")

        # Calculate total_cart_value for each cart
        carts_df['total_price'] = carts_df['quantity'] * carts_df['price']
        total_cart_values = carts_df.groupby('cart_id')['total_price'].sum().reset_index()
        total_cart_values.rename(columns={'total_price': 'total_cart_value'}, inplace=True)

        # Merge total_cart_value back into the main DataFrame
        final_df = carts_df.merge(total_cart_values, on='cart_id')

        # Select and reorder columns
        final_df = final_df[['cart_id', 'user_id', 'product_id', 'quantity', 'price', 'total_cart_value']]

        return final_df

    except Exception as e:
        raise RuntimeError(f"Error processing cart data: {e}")


def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_file (str): Path to save the CSV file.
    """
    try:
        df.to_csv(output_file, index=False)
        print(f"Processed cart data saved to {output_file}")
    except Exception as e:
        raise RuntimeError(f"Error saving CSV file: {e}")


if __name__ == "__main__":
    # GCS bucket and file details
    bucket_name = "savannah_informatics_assesment"  
    blob_name = "raw/carts_raw.json"         
    output_file = "cart.csv"  

    try:
        # Download JSON data from GCS
        json_data = download_json_from_gcs(bucket_name, blob_name)

        # Process the cart data
        processed_data = process_cart_data(json_data)

        # Save the processed data to CSV
        save_to_csv(processed_data, output_file)

    except Exception as e:
        print(f"An error occurred: {e}")

'''

'''

import json
from google.cloud import storage
import pandas as pd


def download_json_from_gcs(bucket_name, blob_name):
    """
    Download a JSON file from a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the bucket.

    Returns:
        list: JSON data as a list of dictionaries.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Check if the blob exists
        if not blob.exists():
            raise RuntimeError(f"The blob '{blob_name}' does not exist in the bucket '{bucket_name}'.")

        # Download and parse JSON
        json_data = json.loads(blob.download_as_text())

        if not isinstance(json_data, list):
            raise ValueError("Expected JSON data to be a list of dictionaries.")
        
        return json_data

    except Exception as e:
        raise RuntimeError(f"Error downloading or parsing JSON file: {e}")


def process_cart_data(json_data):
    """
    Process the cart JSON data to flatten the products array and calculate total cart value.

    Args:
        json_data (list): List of JSON objects representing carts.

    Returns:
        pd.DataFrame: Processed DataFrame with flattened product rows and total cart values.
    """
    try:
        # Ensure the JSON has the expected structure
        if not json_data or not isinstance(json_data, list):
            raise ValueError("The JSON data is either empty or not in the expected list format.")
        
        # Convert JSON data to DataFrame
        carts_df = pd.json_normalize(json_data, record_path=['products'], meta=['cart_id', 'user_id'], sep='_')

        # Ensure required fields are present
        required_fields = ['cart_id', 'user_id', 'product_id', 'quantity', 'price']
        missing_fields = [field for field in required_fields if field not in carts_df.columns]
        if missing_fields:
            raise KeyError(f"Missing required fields: {', '.join(missing_fields)}")

        # Handle potential missing or invalid 'quantity' and 'price' values
        carts_df['quantity'] = pd.to_numeric(carts_df['quantity'], errors='coerce')
        carts_df['price'] = pd.to_numeric(carts_df['price'], errors='coerce')

        # Drop rows with NaN values in 'quantity' or 'price'
        carts_df = carts_df.dropna(subset=['quantity', 'price'])

        # Calculate total_cart_value for each cart
        carts_df['total_price'] = carts_df['quantity'] * carts_df['price']
        total_cart_values = carts_df.groupby('cart_id')['total_price'].sum().reset_index()
        total_cart_values.rename(columns={'total_price': 'total_cart_value'}, inplace=True)

        # Merge total_cart_value back into the main DataFrame
        final_df = carts_df.merge(total_cart_values, on='cart_id')

        # Select and reorder columns
        final_df = final_df[['cart_id', 'user_id', 'product_id', 'quantity', 'price', 'total_cart_value']]

        return final_df

    except Exception as e:
        raise RuntimeError(f"Error processing cart data: {e}")


def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_file (str): Path to save the CSV file.
    """
    try:
        df.to_csv(output_file, index=False)
        print(f"Processed cart data saved to {output_file}")
    except Exception as e:
        raise RuntimeError(f"Error saving CSV file: {e}")


if __name__ == "__main__":
    # GCS bucket and file details
    bucket_name = "savannah_informatics_assesment"  
    blob_name = "raw/carts_raw.json"  # Ensure this is the correct path
    output_file = "cart.csv"  # Output file name

    try:
        # Download JSON data from GCS
        json_data = download_json_from_gcs(bucket_name, blob_name)

        # Process the cart data
        processed_data = process_cart_data(json_data)

        # Save the processed data to CSV
        save_to_csv(processed_data, output_file)

    except Exception as e:
        print(f"An error occurred: {e}")

'''

'''
import json
from google.cloud import storage
import pandas as pd


def download_json_from_gcs(bucket_name, blob_name):
    """
    Download a JSON file from a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the bucket.

    Returns:
        list or dict: JSON data as a list of dictionaries or a dictionary.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Check if the blob exists
        if not blob.exists():
            raise RuntimeError(f"The blob '{blob_name}' does not exist in the bucket '{bucket_name}'.")

        # Download and parse JSON
        json_data = json.loads(blob.download_as_text())

        if not isinstance(json_data, (list, dict)):
            raise ValueError("Expected JSON data to be a list or dictionary.")
        
        return json_data

    except Exception as e:
        raise RuntimeError(f"Error downloading or parsing JSON file: {e}")


def normalize_json(json_data):
    """
    Normalize JSON data to ensure it's in the expected format (a list of dictionaries).

    Args:
        json_data: The raw JSON data from the GCS file.

    Returns:
        list: Normalized JSON data as a list of dictionaries.
    """
    if isinstance(json_data, dict):
        # If it's a dictionary, assume it contains a key with the list of carts
        # Adjust this key based on your JSON structure
        if "carts" in json_data:
            return json_data["carts"]
        else:
            raise ValueError("JSON data does not contain the expected 'carts' key.")
    elif isinstance(json_data, list):
        # If it's already a list, return as is
        return json_data
    else:
        raise ValueError("JSON data is not in a recognized format (list or dict).")


def process_cart_data(json_data):
    """
    Process the cart JSON data to flatten the products array and calculate total cart value.

    Args:
        json_data (list): List of JSON objects representing carts.

    Returns:
        pd.DataFrame: Processed DataFrame with flattened product rows and total cart values.
    """
    try:
        # Convert JSON data to DataFrame
        carts_df = pd.json_normalize(json_data, record_path=['products'], meta=['id', 'id'], sep='_')

        # Ensure required fields are present
        required_fields = ['id', 'id', 'product_id', 'quantity', 'price']
        missing_fields = [field for field in required_fields if field not in carts_df.columns]
        if missing_fields:
            raise KeyError(f"Missing required fields: {', '.join(missing_fields)}")

        # Handle potential missing or invalid 'quantity' and 'price' values
        carts_df['quantity'] = pd.to_numeric(carts_df['quantity'], errors='coerce')
        carts_df['price'] = pd.to_numeric(carts_df['price'], errors='coerce')

        # Drop rows with NaN values in 'quantity' or 'price'
        carts_df = carts_df.dropna(subset=['quantity', 'price'])

        # Calculate total_cart_value for each cart
        carts_df['total_price'] = carts_df['quantity'] * carts_df['price']
        total_cart_values = carts_df.groupby('id')['total_price'].sum().reset_index()
        total_cart_values.rename(columns={'total_price': 'total_cart_value'}, inplace=True)

        # Merge total_cart_value back into the main DataFrame
        final_df = carts_df.merge(total_cart_values, on='cart_id')

        # Select and reorder columns
        final_df = final_df[['cart_id', 'user_id', 'product_id', 'quantity', 'price', 'total_cart_value']]

        return final_df

    except Exception as e:
        raise RuntimeError(f"Error processing cart data: {e}")


def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_file (str): Path to save the CSV file.
    """
    try:
        df.to_csv(output_file, index=False)
        print(f"Processed cart data saved to {output_file}")
    except Exception as e:
        raise RuntimeError(f"Error saving CSV file: {e}")


if __name__ == "__main__":
    # GCS bucket and file details
    bucket_name = "savannah_informatics_assesment"
    blob_name = "raw/carts_raw.json"
    output_file = "cart.csv"

    try:
        # Download JSON data from GCS
        raw_json_data = download_json_from_gcs(bucket_name, blob_name)

        # Normalize JSON data
        normalized_json_data = normalize_json(raw_json_data)

        # Process the cart data
        processed_data = process_cart_data(normalized_json_data)

        # Save the processed data to CSV
        save_to_csv(processed_data, output_file)

    except Exception as e:
        print(f"An error occurred: {e}")

'''

'''
import json
from google.cloud import storage
import pandas as pd

def download_json_from_gcs(bucket_name, blob_name):
    """
    Download a JSON file from a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the bucket.

    Returns:
        list: JSON data as a list of dictionaries.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        json_data = json.loads(blob.download_as_text())
        return json_data
    except Exception as e:
        raise RuntimeError(f"Error downloading or parsing JSON file: {e}")

def process_cart_data(json_data):
    """
    Process the cart JSON data to flatten the products array and calculate total cart value.

    Args:
        json_data (list): List of JSON objects representing carts.

    Returns:
        pd.DataFrame: Processed DataFrame with flattened product rows and total cart values.
    """
    try:
        # Convert JSON data to DataFrame and flatten the 'products' array
        carts_df = pd.json_normalize(
            json_data, 
            record_path=['products'], 
            meta=['cart_id', 'user_id'], 
            sep='_', 
            meta_prefix='cart_',
            record_prefix='product_'
        )

        # Ensure required fields are present
        required_fields = ['cart_id', 'user_id', 'product_id', 'quantity', 'price']
        for field in required_fields:
            if field not in carts_df.columns:
                raise KeyError(f"Missing required field: {field}")

        # Calculate total_cart_value for each cart
        carts_df['total_price'] = carts_df['quantity'] * carts_df['price']
        total_cart_values = carts_df.groupby('cart_id')['total_price'].sum().reset_index()
        total_cart_values.rename(columns={'total_price': 'total_cart_value'}, inplace=True)

        # Merge total_cart_value back into the main DataFrame
        final_df = carts_df.merge(total_cart_values, on='cart_id')

        # Select and reorder columns
        final_df = final_df[['cart_id', 'user_id', 'product_id', 'quantity', 'price', 'total_cart_value']]

        return final_df

    except Exception as e:
        raise RuntimeError(f"Error processing cart data: {e}")

def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_file (str): Path to save the CSV file.
    """
    try:
        df.to_csv(output_file, index=False)
        print(f"Processed cart data saved to {output_file}")
    except Exception as e:
        raise RuntimeError(f"Error saving CSV file: {e}")

if __name__ == "__main__":
    # GCS bucket and file details
    bucket_name = "savannah_informatics_assesment"  
    blob_name = "raw/carts_raw.json"  # Ensure this is the correct path
    output_file = "cart.csv"  # Output file name

    try:
        # Download JSON data from GCS
        json_data = download_json_from_gcs(bucket_name, blob_name)

        # Process the cart data
        processed_data = process_cart_data(json_data)

        # Save the processed data to CSV
        save_to_csv(processed_data, output_file)

    except Exception as e:
        print(f"An error occurred: {e}")

'''
'''
import json
from google.cloud import storage
import pandas as pd

def download_json_from_gcs(bucket_name, blob_name):
    """
    Download a JSON file from a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the bucket.

    Returns:
        list: JSON data as a list of dictionaries.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        json_data = json.loads(blob.download_as_text())
        return json_data
    except Exception as e:
        raise RuntimeError(f"Error downloading or parsing JSON file: {e}")

def process_cart_data(json_data):
    """
    Process the cart JSON data to flatten the products array and calculate total cart value.

    Args:
        json_data (list): List of JSON objects representing carts.

    Returns:
        pd.DataFrame: Processed DataFrame with flattened product rows and total cart values.
    """
    try:
        # Check if 'products' key exists in any of the cart records
        for record in json_data:
            if 'title' not in record:
                raise KeyError("'title' key not found in one or more cart records.")

        # Convert JSON data to DataFrame and flatten the 'products' array
        carts_df = pd.json_normalize(
            json_data, 
            record_path=['products'], 
            meta=['cart_id', 'user_id'], 
            sep='_', 
            meta_prefix='cart_',
            record_prefix='product_'
        )

        # Ensure required fields are present
        required_fields = ['cart_id', 'user_id', 'product_id', 'quantity', 'price']
        for field in required_fields:
            if field not in carts_df.columns:
                raise KeyError(f"Missing required field: {field}")

        # Calculate total_cart_value for each cart
        carts_df['total_price'] = carts_df['quantity'] * carts_df['price']
        total_cart_values = carts_df.groupby('cart_id')['total_price'].sum().reset_index()
        total_cart_values.rename(columns={'total_price': 'total_cart_value'}, inplace=True)

        # Merge total_cart_value back into the main DataFrame
        final_df = carts_df.merge(total_cart_values, on='cart_id')

        # Select and reorder columns
        final_df = final_df[['cart_id', 'user_id', 'product_id', 'quantity', 'price', 'total_cart_value']]

        return final_df

    except Exception as e:
        raise RuntimeError(f"Error processing cart data: {e}")

def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_file (str): Path to save the CSV file.
    """
    try:
        df.to_csv(output_file, index=False)
        print(f"Processed cart data saved to {output_file}")
    except Exception as e:
        raise RuntimeError(f"Error saving CSV file: {e}")

if __name__ == "__main__":
    # GCS bucket and file details
    bucket_name = "savannah_informatics_assesment"  
    blob_name = "raw/carts_raw.json"  # Ensure this is the correct path
    output_file = "cart.csv"  # Output file name

    try:
        # Download JSON data from GCS
        json_data = download_json_from_gcs(bucket_name, blob_name)

        # Process the cart data
        processed_data = process_cart_data(json_data)

        # Save the processed data to CSV
        save_to_csv(processed_data, output_file)

    except Exception as e:
        print(f"An error occurred: {e}")

'''

import json
from google.cloud import storage
import pandas as pd

def download_json_from_gcs(bucket_name, blob_name):
    """
    Download a JSON file from a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob in the bucket.

    Returns:
        list: JSON data as a list of dictionaries.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        json_data = json.loads(blob.download_as_text())
        return json_data
    except Exception as e:
        raise RuntimeError(f"Error downloading or parsing JSON file: {e}")

def process_cart_data(json_data):
    """
    Process the cart JSON data to flatten the products array and calculate total cart value.

    Args:
        json_data (list): List of JSON objects representing carts.

    Returns:
        pd.DataFrame: Processed DataFrame with flattened product rows and total cart values.
    """
    try:
        # Check if 'products' key exists in any of the cart records
        for record in json_data:
            if 'products' not in record:
                raise KeyError("'products' key not found in one or more cart records.")

        # Convert JSON data to DataFrame and flatten the 'products' array
        carts_df = pd.json_normalize(
            json_data, 
            record_path=['products'], 
            meta=['cart_id', 'user_id'], 
            sep='_', 
            meta_prefix='cart_',
            record_prefix='product_'
        )

        # Ensure required fields are present
        required_fields = ['cart_id', 'user_id', 'product_id', 'quantity', 'price', 'product_title']
        for field in required_fields:
            if field not in carts_df.columns:
                raise KeyError(f"Missing required field: {field}")

        # Rename 'product_title' to 'name' to reflect the correct field
        carts_df.rename(columns={'product_title': 'name'}, inplace=True)

        # Calculate total_cart_value for each cart
        carts_df['total_price'] = carts_df['quantity'] * carts_df['price']
        total_cart_values = carts_df.groupby('cart_id')['total_price'].sum().reset_index()
        total_cart_values.rename(columns={'total_price': 'total_cart_value'}, inplace=True)

        # Merge total_cart_value back into the main DataFrame
        final_df = carts_df.merge(total_cart_values, on='cart_id')

        # Select and reorder columns
        final_df = final_df[['cart_id', 'user_id', 'product_id', 'name', 'quantity', 'price', 'total_cart_value']]

        return final_df

    except Exception as e:
        raise RuntimeError(f"Error processing cart data: {e}")

def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_file (str): Path to save the CSV file.
    """
    try:
        df.to_csv(output_file, index=False)
        print(f"Processed cart data saved to {output_file}")
    except Exception as e:
        raise RuntimeError(f"Error saving CSV file: {e}")

if __name__ == "__main__":
    # GCS bucket and file details
    bucket_name = "savannah_informatics_assesment"  
    blob_name = "raw/carts_raw.json"  # Ensure this is the correct path
    output_file = "cart.csv"  # Output file name

    try:
        # Download JSON data from GCS
        json_data = download_json_from_gcs(bucket_name, blob_name)

        # Process the cart data
        processed_data = process_cart_data(json_data)

        # Save the processed data to CSV
        save_to_csv(processed_data, output_file)

    except Exception as e:
        print(f"An error occurred: {e}")
