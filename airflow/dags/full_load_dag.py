from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

import os

BUCKET = os.environ.get('GCP_GCS_BUCKET')

default_args = {
    'retries': 1,
    'start_date': days_ago(1),
    'schedule_interval': '@once'
}

with DAG(
    'ookla_full_load',
    default_args=default_args
) as dag:

    s3_to_gcs_op = S3ToGCSOperator(
        task_id='s3_to_gcs',
        bucket='ookla-open-data',
        prefix='parquet/performance/',
        dest_gcs=f'gs://{BUCKET}/raw/ookla/',
        replace=True
    )

    s3_to_gcs_op