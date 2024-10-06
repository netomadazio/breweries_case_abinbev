from datetime import datetime, timedelta
import importlib
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

from config.task_config_spark import task_dict_spark

default_args = {
    "owner": "Irineu Madazio Neto",
    "start_date": datetime(2024, 9, 22),
    "depends_on_past": False,
    "email": ["netomaadazio@hotmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def import_py_callable_and_kwargs(task):
    """
    Imports a module and function dynamically based on task settings.

    Args:
        task (dict): Dictionary containing task settings.

    Returns:
        function: Dynamically imported function.

    """
    def _made_function(**kwargs):
        try:
            module_path = task["module_path"]
            module = importlib.import_module(module_path)
            function_name = task["function_name"]
            function = getattr(module, function_name)
            if "kwargs" in task:
                task_kwargs = task["kwargs"]
                task_kwargs.update(kwargs)
                function(**task_kwargs)
            else:
                function(**kwargs)
        except ImportError as e:
            logging.error(f"Error importing module: {e}")
            raise
        except AttributeError as e:
            logging.error(f"Error accessing function: {e}")
            raise
    return _made_function
        

with DAG(
    "ETL-breweries-process-spark",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=["ETL", "breweries", "spark"],
) as dag:
    
    for task_id, task_params in task_dict_spark.items():

        if task_params.get("python_operator", False):
            py_callable = import_py_callable_and_kwargs(task_params)

            task_dict_spark[task_id]["dag_instance"] = PythonOperator(
                task_id=task_id,
                python_callable=py_callable,
                op_kwargs=task_params.get("kwargs", {})
            )

        else:
            container_name = f'spark_worker_{task_id}'
            task_dict_spark[task_id]["dag_instance"] = DockerOperator(
                task_id=task_id,
                image='spark_apps:1.0.0',
                entrypoint=task_params.get("entrypoint", ""),
                docker_url="unix:/var/run/docker.sock",
                network_mode='airflow-network',
                xcom_all=True,
                auto_remove=True,
                tty=True,
                mount_tmp_dir=False,
                environment=task_params.get("kwargs", {})
            )

        dependecies = task_params["depends_on"]

        for dependece_id in dependecies:
            (
                task_dict_spark[task_id]["dag_instance"]
                << task_dict_spark[dependece_id]["dag_instance"]
            )
