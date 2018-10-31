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
        #'extra_package':
        #'/home/airflow/gcs/dags/airflow_with_dataflow/dist/Common-1.0.0.tar.gz',
        'setup_file':
        '/home/airflow/gcs/dags/persons_w_module/setup.py',
        'start_date':
        '2018-01-01',
        'end_date':
        '2018-01-02'
        }
}

dag = DAG(
    dag_id='person',
    default_args=default_args,
    schedule_interval='@once'
    )

task1 = DataFlowPythonOperator(
    task_id='person_dataflow_from_airflow',
    py_file='/home/airflow/gcs/dags/persons_w_module/personCsvToBq.py',
    dag=dag
)