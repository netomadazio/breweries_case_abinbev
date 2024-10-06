"""
This python file contains all tasks used in the ETL-breweries-process-pandas DAG
(breweries_etc_process_spark.py).
"""
from datetime import datetime
from config.utils import get_aws_credentials

AWS_ACCESS_KEY, AWS_SECRET_KEY = get_aws_credentials('aws_access_abinbev')

current_date = datetime.now()

task_dict_spark = {
    "init_process_authentication": {
        "depends_on": [],
        "module_path": "tasks.init_process_authentication",
        "function_name": "init_process_authentication",
        "python_operator": True,
    },
    "extract_data_to_bronze": {
        "depends_on": ["init_process_authentication"],
        "module_path": "tasks.extract_data_to_bronze",
        "function_name": "extract_data_to_bronze",
        "kwargs": {
           "api_url": "https://api.openbrewerydb.org/breweries",
            "bucket_name": "abinbev-case",
            "bronze_path": f"bronze_spark/{current_date.year}/{current_date.month}/{current_date.day}",
            "aws_access_key": AWS_ACCESS_KEY,
            "aws_secret_key": AWS_SECRET_KEY,        
        },
        "python_operator": True,
    },
    "transform_bronze_to_silver": {
        "depends_on": ["extract_data_to_bronze"],
        "entrypoint": "sh /app/bronze_to_silver_spark/entrypoint.sh",  
        "kwargs": {
            "bronze_path": f"s3a://abinbev-case/bronze_spark/{current_date.year}/{current_date.month}/{current_date.day}/breweries.json",
            "silver_path": "s3a://abinbev-case/silver_spark/",
            "aws_access_key": AWS_ACCESS_KEY,
            "aws_secret_key": AWS_SECRET_KEY,
        },
        "python_operator": False,
    },
    "transform_silver_to_gold": {
        "depends_on": ["transform_bronze_to_silver"],
        "entrypoint": "sh /app/silver_to_gold_spark/entrypoint.sh",
        "kwargs": {
            "silver_path": "s3a://abinbev-case/silver_spark/",
            "gold_path": "s3a://abinbev-case/gold_spark/",
            "aws_access_key": AWS_ACCESS_KEY,
            "aws_secret_key": AWS_SECRET_KEY,
        },
        "python_operator": False,
    }
}