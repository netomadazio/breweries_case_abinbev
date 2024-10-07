"""
This python file contains all tasks used in the ETL-breweries-process-pandas DAG
(breweries_etc_process_pandas.py).
"""
from datetime import datetime
from config.utils import get_aws_credentials

AWS_ACCESS_KEY, AWS_SECRET_KEY = get_aws_credentials('aws_access_abinbev')

current_date = datetime.now()

task_dict_pandas = {
    "init_process_authentication": {
        "depends_on": [],
        "module_path": "tasks.init_process_authentication",
        "function_name": "init_process_authentication",
    },
    "extract_data_to_bronze": {
        "depends_on": ["init_process_authentication"],
        "module_path": "tasks.extract_data_to_bronze",
        "function_name": "extract_data_to_bronze",
        "kwargs": {
            "api_url": "https://api.openbrewerydb.org/breweries",
            "bucket_name": "abinbev-case",
            "bronze_path": f"bronze_pandas/{current_date.year}/{current_date.month}/{current_date.day}",
            "aws_access_key": AWS_ACCESS_KEY,
            "aws_secret_key": AWS_SECRET_KEY,        
        },
    },
    "transform_bronze_to_silver": {
        "depends_on": ["extract_data_to_bronze"],
        "module_path": "tasks.bronze_to_silver_pandas",
        "function_name": "transform_bronze_to_silver_data",
        "kwargs": {
            "bucket_name": "abinbev-case",
            "bronze_path": f"bronze_pandas/{current_date.year}/{current_date.month}/{current_date.day}/breweries.json",
            "silver_path": "silver_pandas/",
            "aws_access_key": AWS_ACCESS_KEY,
            "aws_secret_key": AWS_SECRET_KEY,
        },
    },
    "transform_silver_to_gold": {
        "depends_on": ["transform_bronze_to_silver"],
        "module_path": "tasks.silver_to_gold_pandas",
        "function_name": "transform_silver_to_gold_data",
        "kwargs": {
            "bucket_name": "abinbev-case",
            "silver_path": "silver_pandas/",
            "gold_path": "gold_pandas/breweries.parquet",
            "aws_access_key": AWS_ACCESS_KEY,
            "aws_secret_key": AWS_SECRET_KEY,
        },
    }
}