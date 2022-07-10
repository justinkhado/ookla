from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import logging
import geopandas

OOKLA_S3_URL = 'https://ookla-open-data.s3.amazonaws.com/parquet/performance/type={fixed,mobile}/year=2020/quarter=[1-4]/2020-{01,04,07,10}-01_performance_{fixed,mobile}_tiles.parquet'

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

    download_ookla_data_op = BashOperator(
        task_id='download_dataset',
        bash_command=f'curl -sSLf {OOKLA_S3_URL}'
    )

    test_task >> download_ookla_data_op