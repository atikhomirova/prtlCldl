from __future__ import print_function

import datetime

from airflow import models
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


default_dag_args = {
    'start_date': datetime.datetime(2018, 1, 1),
}

with models.DAG(
        'Copy table from storage to big query',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    schema = [{"name": "Index", "type": "INTEGER"},
              {"name": "Date", "type": "STRING"},
              {"name": "AveragePrice", "type": "FLOAT"},
              {"name": "TotalVolume", "type": "FLOAT"},
              {"name": "N4046", "type": "FLOAT"},
              {"name": "N4225", "type": "FLOAT"},
              {"name": "N4770", "type": "FLOAT"},
              {"name": "TotalBags", "type": "FLOAT"},
              {"name": "N4046", "type": "FLOAT"},
              {"name": "N4225", "type": "FLOAT"},
              {"name": "N4770", "type": "FLOAT"},
              {"name": "TotalBags", "type": "FLOAT"},
              {"name": "LargeBags", "type": "FLOAT"},
              {"name": "XLargeBags", "type": "FLOAT"},
              {"name": "type", "type": "STRING"},
              {"name": "year", "type": "INTEGER"},
              {"name": "region", "type": "STRING"}]

    from_csv_to_bigquery  = GoogleCloudStorageToBigQueryOperator(
        bucket='baketto1',
        source_objects='avocado.csv',
        destination_project_dataset_table='micro-store-218714.avocadoDataset.avocado5',
        schema_fields=schema,
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=0,
        write_disposition='WRITE_EMPTY',
        max_bad_records=0, 
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default', 
        schema_update_options=(),
        time_partitioning)

    from_csv_to_bigquery
