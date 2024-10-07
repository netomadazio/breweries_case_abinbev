"""
This function retrieves AWS credentials using the specified Airflow connection ID.
It utilizes the AwsBaseHook from the Airflow Amazon provider to fetch the credentials.
    tuple: A tuple containing the AWS access key and secret key.
"""

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

def get_aws_credentials(conn_id):
    """
    Get AWS credentials from Airflow connections.

    Args:
        conn_id (str): Connection ID in Airflow.

    Returns:
        dict: Dictionary containing AWS credentials.
    """
    hook = AwsBaseHook(aws_conn_id=conn_id, client_type='s3')
    credentials = hook.get_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    
    return access_key, secret_key