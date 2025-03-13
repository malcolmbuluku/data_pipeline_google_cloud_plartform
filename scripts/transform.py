'''
Cleaning Users Data

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
Cleaning Products Data

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

        # The required fields
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
Cleaning Carts Data
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

        # Required fields are present
        required_fields = ['cart_id', 'user_id', 'product_id', 'quantity', 'price', 'product_title']
        for field in required_fields:
            if field not in carts_df.columns:
                raise KeyError(f"Missing required field: {field}")

        # Renaming 'product_title' to 'name' to reflect the correct field
        carts_df.rename(columns={'product_title': 'name'}, inplace=True)

        # Calculating total_cart_value for each cart
        carts_df['total_price'] = carts_df['quantity'] * carts_df['price']
        total_cart_values = carts_df.groupby('cart_id')['total_price'].sum().reset_index()
        total_cart_values.rename(columns={'total_price': 'total_cart_value'}, inplace=True)

        # Merging total_cart_value back into the main DataFrame
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
    # GCS bucket and file credentials
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
