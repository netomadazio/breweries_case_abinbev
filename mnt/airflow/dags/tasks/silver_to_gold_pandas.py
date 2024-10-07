import pandas as pd
import os
import logging
import boto3
from io import BytesIO
import pyarrow.parquet as pq
from config.utils import get_aws_credentials

def read_silver_data(bucket_name, silver_path, AWS_ACCESS_KEY, AWS_SECRET_KEY):
    """
    Reads a parquet file from the specified S3 bucket and path into a pandas DataFrame.

    Args:
        bucket_name (str): The name of the S3 bucket.
        silver_path (str): The path to the input silver data parquet file in the S3 bucket.

    Returns:
        pd.DataFrame: The data read from the parquet file.

    Raises:
        IOError: If there is an issue reading the file.
    """
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        # List all objects in the specified S3 path
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=silver_path)
        if 'Contents' not in response:
            raise FileNotFoundError(f"No files found in s3://{bucket_name}/{silver_path}")

        # Read all parquet files from the S3 path
        parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in s3://{bucket_name}/{silver_path}")

        # Combine all parquet files into a single DataFrame
        df_list = []
        for file in parquet_files:
            obj = s3_client.get_object(Bucket=bucket_name, Key=file)
            table = pq.read_table(BytesIO(obj['Body'].read()))
            df = table.to_pandas()
            
            # Extract partition columns from the file path
            partitions = file.replace(silver_path, '').split('/')
            for partition in partitions:
                if '=' in partition:
                    col, val = partition.split('=')
                    df[col] = val
            
            df_list.append(df)
        
        df = pd.concat(df_list, ignore_index=True)
        logging.info(f"Data successfully read from s3://{bucket_name}/{silver_path}")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from s3://{bucket_name}/{silver_path}: {e}")
        raise

def transform_data(df):
    """
    Transforms the data by casting certain columns to appropriate data types and grouping the data.

    Args:
        df (pd.DataFrame): The DataFrame to be transformed.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    df = df.astype({
        'ID': 'string',
        'NAME': 'string',
        'BREWERY_TYPE': 'string',
        'ADDRESS_1': 'string',
        'ADDRESS_2': 'string',
        'ADDRESS_3': 'string',
        'CITY': 'string',
        'STATE_PROVINCE': 'string',
        'POSTAL_CODE': 'string',
        'COUNTRY': 'string',
        'LONGITUDE': 'float64',
        'LATITUDE': 'float64',
        'PHONE': 'string',
        'WEBSITE_URL': 'string',
        'STATE': 'string',
        'STREET': 'string'
    })

    df = df.groupby(['BREWERY_TYPE', 'COUNTRY', 'STATE', 'CITY']).size().reset_index(name='NUMBER_OF_BREWERIES')
    logging.info("Data transformation completed successfully")
    return df

def save_gold_data(df, bucket_name, gold_path, AWS_ACCESS_KEY, AWS_SECRET_KEY):
    """
    Saves the transformed DataFrame as a Parquet file to the specified S3 bucket and path.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        bucket_name (str): The name of the S3 bucket.
        gold_path (str): The path to the output gold data parquet file in the S3 bucket.

    Raises:
        IOError: If there is an issue writing the file.
    """
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket_name, Key=gold_path, Body=buffer.getvalue())
        logging.info(f"Data successfully saved to s3://{bucket_name}/{gold_path}")
    except Exception as e:
        logging.error(f"Failed to save data to s3://{bucket_name}/{gold_path}: {e}")
        raise

def transform_silver_to_gold_data(**kwargs):
    """
    Transforms silver data to gold data by performing the following steps:
    1. Reads a parquet file from the specified S3 bucket and path into a pandas DataFrame.
    2. Casts certain columns to appropriate data types.
    3. Groups the data by brewery type, country, state, and city, and counts the number of breweries in each group.
    4. Saves the resulting DataFrame to the specified S3 bucket and path as a parquet file.

    Args:
        **kwargs: Arbitrary keyword arguments.
            - bucket_name (str): The name of the S3 bucket.
            - silver_path (str): The path to the input silver data parquet file in the S3 bucket.
            - gold_path (str): The path to the output gold data parquet file in the S3 bucket.

    Returns:
        None
    """
    bucket_name = kwargs.get('bucket_name')
    silver_path = kwargs.get('silver_path')
    gold_path = kwargs.get('gold_path')
    AWS_ACCESS_KEY = kwargs.get('aws_access_key')
    AWS_SECRET_KEY = kwargs.get('aws_secret_key')

    if not bucket_name or not silver_path or not gold_path:
        raise ValueError("The 'bucket_name', 'silver_path', and 'gold_path' must be provided")

    try:
        df = read_silver_data(bucket_name, silver_path, AWS_ACCESS_KEY, AWS_SECRET_KEY)
        df = transform_data(df)
        save_gold_data(df, bucket_name, gold_path, AWS_ACCESS_KEY, AWS_SECRET_KEY)
    except Exception as e:
        logging.critical(f"Critical error in transform_silver_to_gold_data: {e}")
        raise

if __name__ == "__main__":
    transform_silver_to_gold_data()