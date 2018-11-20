from __future__ import absolute_import

import argparse
import logging
import apache_beam as beam
import json

from apache_beam.options.pipeline_options import (GoogleCloudOptions,
                                                  PipelineOptions,
                                                  SetupOptions,
                                                  StandardOptions,
                                                  WorkerOptions)

schema = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING')


input_topic = 'projects/micro-store-218714/topics/test-microstore218714-topic'

output_table = 'micro-store-218714:person.personsReduced42'


BUCKET_URL = 'gs://baketto1'
PROJECT_ID = 'micro-store-218714'  
JOB_NAME = 'streaming-test' 

colNames = ["ID", "FirstName", "LastName", "Address"]


class ParsePubsub(beam.DoFn):
    @staticmethod
    def process(line):
        values = line.split(',')
        logging.info(values)
        
        try:
            values[0] = int(values[0])
        except:
            return None
        else:
            dct = dict(zip(colNames, values))
            
        return [dct]

def run(argv=None):
    """Build and run the pipeline."""

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True    

    logging.info('Basic pipeline options ready')

    google_cloud_options = pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.staging_location = BUCKET_URL + '/staging'
    google_cloud_options.temp_location = BUCKET_URL + '/temp'

    logging.info('Google cloud pipeline options are ready')


    logging.info('Runner is set')    


    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text from CSV file.
        table_row = (p | 'ReadTableName'>> beam.io.ReadFromPubSub(input_topic)
        
                       | 'ParseTableName' >> beam.ParDo(ParsePubsub())
                       
                       | 'Write' >> beam.io.WriteToBigQuery(
                             output_table,
                             schema=schema,
                             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                             )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
