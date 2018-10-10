import datetime
import io
import logging

from airflow import models
from airflow.contrib.operators import gcs_to_bq
from airflow.operators import dummy_operator


# --------------------------------------------------------------------------------
# Set default arguments
# --------------------------------------------------------------------------------

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'comoser_sample_bq_copy_across_locations',
        default_args=default_args,
        schedule_interval=None) as dag:

    start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )

    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_success'
    )

    GCS_to_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        # Replace ":" with valid character for Airflow task
        task_id='copy_csv_from_storage_to_bq',
        bucket='baketto1',
        source_objects='avocado.csv',
        destination_project_dataset_table='micro-store-218714.avocadoDataset.avocado5',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE'
    )

    start >> GCS_to_BQ >> end