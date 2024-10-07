import requests
import json
import logging
import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError

def fetch_data(api_url):
    """
    Fetches data from the given API URL.

    Args:
        api_url (str): The URL of the API to fetch data from.

    Returns:
        dict: The data fetched from the API.

    Raises:
        requests.RequestException: If there is an issue with the HTTP request.
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        raise

def save_data_to_s3(data, bucket_name, bronze_path, AWS_ACCESS_KEY, AWS_SECRET_KEY):
    """
    Saves the fetched data to the specified S3 bucket and path.

    Args:
        data (dict): The data to be saved.
        bucket_name (str): The name of the S3 bucket.
        s3_bronze_path (str): The S3 path where the data will be saved.

    Raises:
        BotoCoreError, NoCredentialsError: If there is an issue with the S3 operation.
    """

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        s3_bronze_path = f"{bronze_path}/breweries.json"
        s3_client.put_object(Bucket=bucket_name, Key=s3_bronze_path, Body=json.dumps(data))
        logging.info(f"Data successfully saved to s3://{bucket_name}/{s3_bronze_path}")
    except (BotoCoreError, NoCredentialsError) as e:
        logging.error(f"Failed to save data to S3: {e}")
        raise

def extract_data_to_bronze(**kwargs):
    """
    Extracts data from a given API URL and saves it to a specified S3 bucket and path.

    Args:
        **kwargs: Arbitrary keyword arguments.
            api_url (str): The URL of the API to fetch data from.
            bucket_name (str): The name of the S3 bucket.
            raw_path (str): The S3 path where the fetched data will be saved.

    Raises:
        ValueError: If 'api_url', 'bucket_name', or 'raw_path' is not provided.
    """
    api_url = kwargs.get('api_url')
    bucket_name = kwargs.get('bucket_name')
    bronze_path = kwargs.get('bronze_path')
    AWS_ACCESS_KEY = kwargs.get('aws_access_key')
    AWS_SECRET_KEY = kwargs.get('aws_secret_key')

    if not api_url or not bucket_name or not bronze_path:
        raise ValueError("The 'api_url', 'bucket_name', and 'bronze_path' must be provided")

    try:
        data = fetch_data(api_url)
        save_data_to_s3(data, bucket_name, bronze_path, AWS_ACCESS_KEY, AWS_SECRET_KEY)
    except Exception as e:
        logging.critical(f"Critical error in extract_data_to_bronze: {e}")
        raise

if __name__ == "__main__":
    extract_data_to_bronze()