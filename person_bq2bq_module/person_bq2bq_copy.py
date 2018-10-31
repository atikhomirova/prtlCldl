from __future__ import absolute_import

import json
import argparse
import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import (GoogleCloudOptions,
                                                  PipelineOptions,
                                                  SetupOptions,
                                                  StandardOptions,
                                                  WorkerOptions)

from cleansing.cleansing import *

schema = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING')

input = 'micro-store-218714:person.personFull42'
output = input + 'TransformedCopy'


BUCKET_URL = 'gs://baketto1'  
PROJECT_ID = 'micro-store-218714'  
JOB_NAME = 'person-bq2bq-test-copy'
CONFIG_PATH = '/home/airflow/gcs/dags/person_bq2bq_module/config.json'


with open(CONFIG_PATH) as json_data:
    config = json.load(json_data)
    logging.info(config)


def call_function_from_str(str_function, dct, name):
    if ',' in str_function:
        function, arg = str_function.split(',') #How an argument should be put in config?
        dct = eval(function)(dct, name, arg)
    else:
        dct = eval(str_function)(dct, name)
    return dct


class FormatAsTableRow(beam.DoFn):
    def process(self, line):
        dct = line.copy()

        for name in config.keys():
            functions = config[name]
            for function in functions:
                dct = call_function_from_str(function, dct, name)
                if dct is None:
                    return None

        return [dct]


def run(argv=None):
    """Build and run the pipeline."""
    
    '''pipeline_options = [
        '--project={0}'.format(PROJECT_ID),
        '--job_name={0}'.format(JOB_NAME),
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/temp/'.format(BUCKET),
        #'--runner=DirectRunner',
        '--runner=DataflowRunner'
        #'--setup_file=./setup.py'
        ]'''

    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '--runner',
        dest='runner',
        default='DirectRunner',
        help='DirectRunner or DataflowRunner')

    known_args, extra_pipeline_options = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True

    logging.info('Basic pipeline options ready')

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.staging_location = BUCKET_URL + '/staging'
    google_cloud_options.temp_location = BUCKET_URL + '/temp'

    logging.info('Google cloud pipeline options are ready')
    logging.info(known_args.runner, type(known_args.runner))    

    pipeline_options.view_as(StandardOptions).runner = known_args.runner

    logging.info('Runner is set') 


    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text from CSV file.
        lines = p | 'ReadFromBQ' >> beam.io.Read(beam.io.BigQuerySource(input))

        transformed = lines | 'Transform' >> beam.ParDo(FormatAsTableRow())

        # Write to BigQuery.
        transformed | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            output,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
