import datetime
import logging

from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

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
    'dataflow_default_options': {
        "project": 'micro-store-218714',
        "zone": 'us-east1-b',
        "stagingLocation": 'gs://baketto1/staging'}
}

dag = DAG(
    dag_id='juliaset', 
    default_args=default_args, 
    schedule_interval=None
    )

task1 = DataFlowPythonOperator(
    task_id='trigger_dataflow_from_airflow',
    py_file='gs://baketto1/juliaset/juliaset_main.py',
    dag=dag
)
