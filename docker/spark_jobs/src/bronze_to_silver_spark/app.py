from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging
import os

def read_bronze_data(spark, bronze_path):
    """
    Reads a JSON file from the specified bronze path into a Spark DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        bronze_path (str): The file path to the bronze JSON data.

    Returns:
        DataFrame: The data read from the JSON file.

    Raises:
        IOError: If there is an issue reading the file.
    """
    try:
        df = spark.read.json(bronze_path)
        logging.info(f"Data successfully read from {bronze_path}")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from {bronze_path}: {e}")
        raise

def transform_data(df):
    """
    Transforms the data by converting column names and specified string columns to uppercase,
    and ensuring specified columns are of the correct data type.

    Args:
        df (DataFrame): The DataFrame to be transformed.

    Returns:
        DataFrame: The transformed DataFrame.
    """
    df = df.toDF(*[col.upper() for col in df.columns])

    string_columns = ['NAME', 'BREWERY_TYPE', 'ADDRESS_1', 'ADDRESS_2', 'ADDRESS_3', 'CITY', 'STATE_PROVINCE', 'POSTAL_CODE', 'COUNTRY', 'PHONE', 'WEBSITE_URL', 'STATE', 'STREET']

    for col in string_columns:
        df = df.withColumn(col, F.upper(F.trim(F.col(col))))

    df = df.na.fill({'LATITUDE': 0.0, 'LONGITUDE': 0.0})
    df = df.na.fill('UNKNOWN', subset= string_columns)
    df = df.filter((F.col('ID').isNotNull()) & (F.col('ID') != ''))

    df = df.withColumn("ID", df["ID"].cast("string")) \
            .withColumn("NAME", df["NAME"].cast("string"))\
            .withColumn("BREWERY_TYPE", df["BREWERY_TYPE"].cast("string"))\
            .withColumn("ADDRESS_1", df["ADDRESS_1"].cast("string"))\
            .withColumn("ADDRESS_2", df["ADDRESS_2"].cast("string"))\
            .withColumn("ADDRESS_3", df["ADDRESS_3"].cast("string"))\
            .withColumn("CITY", df["CITY"].cast("string"))\
            .withColumn("STATE_PROVINCE", df["STATE_PROVINCE"].cast("string"))\
            .withColumn("POSTAL_CODE", df["POSTAL_CODE"].cast("string"))\
            .withColumn("COUNTRY", df["COUNTRY"].cast("string"))\
            .withColumn("LONGITUDE", df["LONGITUDE"].cast("float")) \
            .withColumn("LATITUDE", df["LATITUDE"].cast("float"))\
            .withColumn("PHONE", df["PHONE"].cast("string"))\
            .withColumn("WEBSITE_URL", df["WEBSITE_URL"].cast("string"))\
            .withColumn("STATE", df["STATE"].cast("string"))\
            .withColumn("STREET", df["STREET"].cast("string"))

    logging.info("Data transformation completed successfully")
    return df

def save_silver_data(df, silver_path):
    """
    Saves the transformed DataFrame as a Parquet file partitioned by 'COUNTRY', 'STATE', and 'CITY'.

    Args:
        df (DataFrame): The DataFrame to be saved.
        silver_path (str): The file path to save the silver Parquet data.

    Raises:
        IOError: If there is an issue writing the file.
    """
    try:
        df.write.mode('overwrite').partitionBy("COUNTRY", "STATE", "CITY").parquet(silver_path)
        logging.info(f"Data successfully saved to {silver_path}")
    except Exception as e:
        logging.error(f"Failed to save data to {silver_path}: {e}")
        raise

def transform_bronze_to_silver_data():
    """
    Transforms bronze data to silver data by performing the following steps:
    1. Reads a JSON file from the specified bronze path into a Spark DataFrame.
    2. Converts all column names to uppercase.
    3. Converts specified string columns to uppercase.
    4. Ensures specified columns are of the correct data type.
    5. Saves the transformed DataFrame as a Parquet file partitioned by 'COUNTRY', 'STATE', and 'CITY'.

    Envs:
        - bronze_path (str): The file path to the bronze JSON data.
        - silver_path (str): The file path to save the silver Parquet data.
        - aws_access_key (str): The AWS access key.
        - aws_secret_key (str): The AWS secret key.

    Returns:
        None
    """

    bronze_path = os.getenv("bronze_path")
    silver_path = os.getenv("silver_path")

    if not bronze_path or not silver_path:
        raise ValueError("The paths 'bronze_path' and 'silver_path' must be provided")
    
    AWS_ACCESS_KEY_ID = os.getenv("aws_access_key")
    AWS_SECRET_ACCESS_KEY = os.getenv("aws_secret_key")
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("S3 Integration") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.path.style.access", "false") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
            .getOrCreate()
        df = read_bronze_data(spark, bronze_path)
        df = transform_data(df)
        save_silver_data(df, silver_path)
    except Exception as e:
        logging.critical(f"Critical error in transform_bronze_to_silver_data: {e}")
        raise
    finally:
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    transform_bronze_to_silver_data()