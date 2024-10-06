import pandas as pd
import logging
import boto3
from io import BytesIO
import tempfile
import os

def read_bronze_data(bucket_name, bronze_path, AWS_ACCESS_KEY, AWS_SECRET_KEY):
    """
    Reads a JSON file from the specified S3 bucket and path into a pandas DataFrame.

    Args:
        bucket_name (str): The name of the S3 bucket.
        bronze_path (str): The file path to the bronze JSON data in the S3 bucket.

    Returns:
        pd.DataFrame: The data read from the JSON file.

    Raises:
        IOError: If there is an issue reading the file.
    """
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        print(AWS_SECRET_KEY)
        obj = s3_client.get_object(Bucket=bucket_name, Key=bronze_path)
        df = pd.read_json(BytesIO(obj['Body'].read()))
        logging.info(f"Data successfully read from s3://{bucket_name}/{bronze_path}")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from s3://{bucket_name}/{bronze_path}: {e}")
        raise

def transform_data(df):
    """
    Transforms the data by converting column names and specified string columns to uppercase,
    and ensuring specified columns are of the correct data type.

    Args:
        df (pd.DataFrame): The DataFrame to be transformed.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    df.columns = map(str.upper, df.columns)

    string_columns = ['NAME', 'BREWERY_TYPE', 'ADDRESS_1', 'ADDRESS_2', 'ADDRESS_3', 'CITY', 'STATE_PROVINCE', 'POSTAL_CODE', 'COUNTRY', 'PHONE', 'WEBSITE_URL', 'STATE', 'STREET']

    df[string_columns] = df[string_columns].astype(str)
    df[string_columns] = df[string_columns].apply(lambda x: x.str.strip())
    df[string_columns] = df[string_columns].applymap(lambda x: x.upper() if isinstance(x, str) else x)

    df['LATITUDE'].fillna(0.0, inplace=True)
    df['LONGITUDE'].fillna(0.0, inplace=True)
    df[string_columns] = df[string_columns].fillna('UNKNOWN')
    df = df[df['ID'].notna() & (df['ID'] != '')]

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
        'LONGITUDE': 'float',
        'LATITUDE': 'float',
        'PHONE': 'string',
        'WEBSITE_URL': 'string',
        'STATE': 'string',
        'STREET': 'string'
    })

    logging.info("Data transformation completed successfully")
    return df

def save_silver_data(df, bucket_name, silver_path, AWS_ACCESS_KEY, AWS_SECRET_KEY):
    """
    Saves the transformed DataFrame as a Parquet file to the specified S3 bucket and path.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        bucket_name (str): The name of the S3 bucket.
        silver_path (str): The file path to save the silver Parquet data in the S3 bucket.

    Raises:
        IOError: If there is an issue writing the file.
    """
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = os.path.join(temp_dir, 'silver')
            partition_cols = ['COUNTRY', 'STATE', 'CITY']
            
            # Salva o DataFrame particionado localmente
            df.to_parquet(local_path, index=False, partition_cols=partition_cols)
            
            # Faz o upload dos arquivos para o S3, excluindo _common_metadata e _metadata
            for root, _, files in os.walk(local_path):
                for file in files:
                    if file not in ['_common_metadata', '_metadata']:
                        file_path = os.path.join(root, file)
                        s3_key = os.path.relpath(file_path, local_path)
                        s3_client.upload_file(file_path, bucket_name, os.path.join(silver_path, s3_key))
                        
        logging.info(f"Data successfully saved to s3://{bucket_name}/{silver_path}")
    except Exception as e:
        logging.error(f"Failed to save data to s3://{bucket_name}/{silver_path}: {e}")
        raise

def transform_bronze_to_silver_data(**kwargs):
    """
    Transforms bronze data to silver data by performing the following steps:
    1. Reads a JSON file from the specified S3 bucket and path into a pandas DataFrame.
    2. Converts all column names to uppercase.
    3. Converts specified string columns to uppercase.
    4. Ensures specified columns are of the correct data type.
    5. Saves the transformed DataFrame as a Parquet file to the specified S3 bucket and path.

    Args:
        **kwargs: Arbitrary keyword arguments.
            - bucket_name (str): The name of the S3 bucket.
            - bronze_path (str): The file path to the bronze JSON data in the S3 bucket.
            - silver_path (str): The file path to save the silver Parquet data in the S3 bucket.

    Returns:
        None
    """
    bucket_name = kwargs.get('bucket_name')
    bronze_path = kwargs.get('bronze_path')
    silver_path = kwargs.get('silver_path')
    AWS_ACCESS_KEY = kwargs.get('aws_access_key')
    AWS_SECRET_KEY = kwargs.get('aws_secret_key')

    if not bucket_name or not bronze_path or not silver_path:
        raise ValueError("The 'bucket_name', 'bronze_path', and 'silver_path' must be provided")

    try:
        df = read_bronze_data(bucket_name, bronze_path, AWS_ACCESS_KEY, AWS_SECRET_KEY)
        df = transform_data(df)
        save_silver_data(df, bucket_name, silver_path, AWS_ACCESS_KEY, AWS_SECRET_KEY)
    except Exception as e:
        logging.critical(f"Critical error in transform_bronze_to_silver_data: {e}")
        raise

if __name__ == "__main__":
    transform_bronze_to_silver_data()