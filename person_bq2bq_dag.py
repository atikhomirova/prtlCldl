import datetime
import logging

from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator


# --------------------------------------------------------------------------------
# Set default arguments
# --------------------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime.utcnow(),
    'dataflow_default_options': {
        "project": 'micro-store-218714',
        'runner':
        'DataflowRunner',
        'setup_file':
        '/home/airflow/gcs/dags/person_bq2bq_module/setup.py',
        'start_date':
        '2018-01-01',
        'end_date':
        '2018-01-02'
        }
}

dag = DAG(
    dag_id='person_bq2bq',
    default_args=default_args,
    schedule_interval='@once'
    )

task1 = DataFlowPythonOperator(
    task_id='person_bq2bq_dataflow',
    py_file='/home/airflow/gcs/dags/person_bq2bq_module/person_bq2bq_config.py',
    dag=dag
)

#task2 = DataFlowPythonOperator(
#    task_id='person_bq2bq_dataflow_copy',
#    py_file='/home/airflow/gcs/dags/person_bq2bq_module/person_bq2bq_copy.py',
#    dag=dag
#)