from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
import os

def read_silver_data(spark, silver_path):
    """
    Reads a parquet file from the specified silver path into a Spark DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        silver_path (str): The path to the input silver data parquet file.

    Returns:
        DataFrame: The data read from the parquet file.

    Raises:
        IOError: If there is an issue reading the file.
    """
    try:
        df = spark.read.parquet(silver_path)
        logging.info(f"Data successfully read from {silver_path}")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from {silver_path}: {e}")
        raise

def transform_data(df):
    """
    Transforms the data by casting certain columns to appropriate data types and grouping the data.

    Args:
        df (DataFrame): The DataFrame to be transformed.

    Returns:
        DataFrame: The transformed DataFrame.
    """
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

    df = df.groupBy("BREWERY_TYPE", "COUNTRY", "STATE", "CITY").count().withColumnRenamed("count", "NUMBER_OF_BREWERIES")
    logging.info("Data transformation completed successfully")
    return df

def save_gold_data(df, gold_path):
    """
    Saves the transformed DataFrame as a Parquet file.

    Args:
        df (DataFrame): The DataFrame to be saved.
        gold_path (str): The path to the output gold data parquet file.

    Raises:
        IOError: If there is an issue writing the file.
    """
    try:
        df.write.mode('overwrite').parquet(gold_path)
        logging.info(f"Data successfully saved to {gold_path}")
    except Exception as e:
        logging.error(f"Failed to save data to {gold_path}: {e}")
        raise

def transform_silver_to_gold_data():
    """
    Transforms silver data to gold data by performing the following steps:
    1. Reads a parquet file from the specified silver path into a Spark DataFrame.
    2. Casts certain columns to appropriate data types.
    3. Groups the data by brewery type, country, state, and city, and counts the number of breweries in each group.
    4. Saves the resulting DataFrame to the specified gold path as a parquet file.

    Envs:
        - silver_path (str): The path to the input silver data parquet file.
        - gold_path (str): The path to the output gold data parquet file.
        - aws_access_key (str): The AWS access key.
        - aws_secret_key (str): The AWS secret key.

    Returns:
        None
    """

    silver_path = os.getenv("silver_path")
    gold_path = os.getenv("gold_path")
    if not silver_path or not gold_path:
        raise ValueError("Both 'silver_path' and 'gold_path' must be provided")
    
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
        df = read_silver_data(spark, silver_path)
        df = transform_data(df)
        save_gold_data(df, gold_path)
    except Exception as e:
        logging.critical(f"Critical error in transform_silver_to_gold_data: {e}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    transform_silver_to_gold_data()