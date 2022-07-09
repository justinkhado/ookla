from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import logging
import geopandas

def print_geopandas_version():
    logging.info(geopandas.__version__)

default_args = {
    'retries': 1,
    'start_date': days_ago(1)
}

with DAG(
    'ookla_full_load',
    default_args=default_args
) as dag:

    test_task = PythonOperator(
        task_id='test_task',
        python_callable=print_geopandas_version
    )

    test_task_2 = PythonOperator(
        task_id='test_task_2',
        python_callable=print_geopandas_version
    )

    test_task >> test_task_2