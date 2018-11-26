import datetime
import io
import logging

from airflow import models
#from airflow.contrib.operators import gcs_to_gcs
from airflow.hooks import GoogleDriveHook
from airflow.operators import dummy_operator, PythonOperator, BashOperator

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False
}


def load_to_local_bucket(**kwargs):
    logging.info(kwargs)
    gdriveHook = GoogleDriveHook()
    service = gdriveHook.get_service()
    logging.info(service)
    fileIDlist = gdriveHook.get_file_IDs_list()
    idList = str(fileIDlist).split(',')
    logging.info(idList)
    for id in idList:
        logging.info(id)
        gdriveHook.get_file(id)
    #gdriveHook.get_file('1wfITXaEVpq70pgXZjQ-zdtJKD2G3vJXy')

with models.DAG(
        'gdrive2gcs',
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

    load_from_gdrive = PythonOperator(task_id='load_to_local_bucket',
                    provide_context=True,
                    python_callable=load_to_local_bucket,
                    dag=dag)

    # copy_to_gcp = gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator(
        # source_bucket='us-central1-microstore21871-37272379-bucket',
        # source_object='data/gcs/*',
        # desination_bucket='baketto1',
        # destination_object='data/',
        # move_object=True
        # )

    copy_to_gcp = BashOperator(
            task_id='copy_to_gcp',
            bash_command='''
                gsutil -m cp \
                gs://us-central1-micro-store-218-f06e2a8d-bucket/data/* \
                gs://baketto1/data/
                '''
        )

    start >> load_from_gdrive >> copy_to_gcp >> end
    #start >> load_from_gdrive >> end