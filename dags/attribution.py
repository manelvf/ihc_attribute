"""Main module for processing customer journey data through the IHC Attribution API."""
from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags.lib.batch_processor import process_batches, process_responses
from dags.lib.db import fill_channel_reporting
from dags.lib.ihc_attribution_client import ConfigError
from dags.lib.report import save_channel_metrics


DB_PATH = os.environ.get("DB_PATH", "challenge.db")

CONV_TYPE_ID = os.getenv('IHC_CONV_TYPE_ID')  # Optional: can also get conv_type_id from env
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))  # Optional: can configure batch size in env
CSV_FILE = os.getenv('CSV_FILE', 'output/channel_metrics.csv')


with DAG(
    dag_id='attibution',
    start_date=datetime(2025, 1, 20),  # Start date
    schedule_interval='@hourly',       
    tags=["ihc"],
    catchup=False                     # Skip missed runs
) as dag:
    batches = PythonOperator(
        task_id='process_batches',
        python_callable=process_batches,
        op_kwargs={
            'db_path': DB_PATH,
            'conv_type_id': CONV_TYPE_ID,
            'batch_size': BATCH_SIZE,
        }
    )

    responses = PythonOperator(
        task_id='process_responses',
        python_callable=process_responses,
    )

    channel_reporting = PythonOperator(
        task_id='fill_channel_reporting',
        python_callable=fill_channel_reporting,
        op_kwargs={'db_path': DB_PATH}
    )

    save_metrics = PythonOperator(
        task_id='save_channel_metrics',
        python_callable=save_channel_metrics,
        op_kwargs={'db_path': DB_PATH, 'csv_file': CSV_FILE}
    )


batches >> responses >> channel_reporting >> save_metrics
