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


JOB_NAME = 'person-bq2bq-config-bq'

BUCKET_URL = 'gs://baketto1'  

PROJECT_ID = 'micro-store-218714'  
DATASET_ID = 'person'

INPUT_TABLE = PROJECT_ID + ':' + DATASET_ID + '.' + 'personFull42'
OUTPUT_TABLE = INPUT_TABLE + 'TransformedConf'
CONFIG_TABLE = PROJECT_ID + ':' + DATASET_ID + '.' + 'config'

SCHEMA = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING')


class Conf(beam.DoFn):
    def process(self, line):
        #logging.info(dct)
        yield line


class ApplyConfig(beam.DoFn):
    config = None
    
    @classmethod
    def process(cls, row, config_list):
        if cls.config is None:
            cls.create_config(config_list)     
            
        dct = row.copy()   

        for name, functions_ in cls.config.items():
            functions = [v for k, v in sorted(functions_.items())]
            
            for function in functions:
                dct = call_function_from_str(function, dct, name)
                if not dct:
                    return None
        return [dct]
        

    @classmethod    
    def create_config(cls, config_list):
        config = {} 
        #logging.info(config_list)
        for config_row in config_list:
            #logging.info(config_row)
            order = config_row["Order"]##
            colName = config_row["ColName"]##
            funcName = config_row["FuncName"]##
            
            if colName not in config:
                config[colName] = {}

            config[colName][order] = funcName
 
        cls.config = config       
        

def run(argv=None):

    """Build and run the pipeline."""
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    #pipeline_options.view_as(SetupOptions).setup_file = './setup.py'

    logging.info('Basic pipeline options ready')

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.staging_location = BUCKET_URL + '/staging'
    google_cloud_options.temp_location = BUCKET_URL + '/temp'
    
    logging.info('Google cloud pipeline options are ready')  

    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    #pipeline_options.view_as(StandardOptions).streaming = True
    logging.info('Runner is set')


    with beam.Pipeline(options=pipeline_options) as p:

        # Read config table from BQ 
        config_list = p | 'ConfigReadFromBQ' >> beam.io.Read(beam.io.BigQuerySource(CONFIG_TABLE))
        trans_config = config_list | 'ConfigTransform' >> beam.ParDo(Conf())   
        

        # Read main table from BQ
        lines = p | 'DataReadFromBQ' >> beam.io.Read(beam.io.BigQuerySource(INPUT_TABLE))

        #transformed = lines | 'Transform' >> beam.ParDo(ApplyConfig(config))   
        transformed = lines | 'DataTransform' >> beam.ParDo(ApplyConfig(), beam.pvalue.AsList(trans_config))

        # Write to BQ
        transformed | 'DataWriteToBQ' >> beam.io.WriteToBigQuery(
            OUTPUT_TABLE,
            schema=SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
