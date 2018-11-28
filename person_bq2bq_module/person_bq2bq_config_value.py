from __future__ import absolute_import

import json
import argparse
import logging
import apache_beam as beam

#import google.cloud.dataflow as df
from google.cloud.dataflow import pvalue


from apache_beam.options.pipeline_options import (GoogleCloudOptions,
                                                  PipelineOptions,
                                                  SetupOptions,
                                                  StandardOptions,
                                                  WorkerOptions)

from cleansing.cleansing_value import *


JOB_NAME = 'person-bq2bq-config-bq'

BUCKET_URL = 'gs://baketto1'  

PROJECT_ID = 'micro-store-218714'  
DATASET_ID = 'person'

INPUT_TABLE = PROJECT_ID + ':' + DATASET_ID + '.' + 'personFull42'
OUTPUT_TABLE = INPUT_TABLE + 'TransformedConf'
OUTPUT_TABLE_FILTERED = INPUT_TABLE + 'TransformedConf' + 'Filtered'

CONFIG_TABLE = PROJECT_ID + ':' + DATASET_ID + '.' + 'config'



SCHEMA = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING')


class Config(beam.DoFn):
    def process(self, line):
        #logging.info(dct)
        yield line


class ApplyConfig(beam.DoFn):
    config = None

    SIDE_OUTPUT_TAG_FILTERED_ROW = 'tag_filtered_row'

    @classmethod
    def process(cls, row, config_list):

        if cls.config is None:
            cls.create_config(config_list)

        dct = row.copy()

        for name, functions_ in cls.config.items():
            functions = [v for k, v in sorted(functions_.items())]
            value = dct[name]
            for function in functions:
                value = call_function_from_str(function, value)
                if not value:
                    yield beam.pvalue.TaggedOutput(
                        cls.SIDE_OUTPUT_TAG_FILTERED_ROW, dct)
                else:
                    dct[name] = value
        yield dct

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
    pipeline_options.view_as(SetupOptions).setup_file = './setup_value.py'

    logging.info('Basic pipeline options ready')

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.staging_location = BUCKET_URL + '/staging'
    google_cloud_options.temp_location = BUCKET_URL + '/temp'
    
    logging.info('Google cloud pipeline options are ready')  

    pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
    logging.info('Runner is set')


    with df.Pipeline(options=pipeline_options) as p:

        # Read config table from BQ 
        config_list = p | 'ConfigReadFromBQ' >> beam.io.Read(
            beam.io.BigQuerySource(CONFIG_TABLE))
        trans_config = config_list | 'ConfigTransform' >> beam.ParDo(Config())


        # Read main table from BQ
        lines = p | 'DataReadFromBQ' >> beam.io.Read(beam.io.BigQuerySource(
            INPUT_TABLE))

        #transformed = lines | 'Transform' >> beam.ParDo(ApplyConfig(config))   
        transformed = lines | 'DataTransform' >> beam.ParDo(
            ApplyConfig().with_outputs(ApplyConfig.SIDE_OUTPUT_TAG_FILTERED_ROW,
            main='dct'
            ), beam.pvalue.AsList(trans_config))

        transformed_main, _ = transformed
        transformed_filtered = transformed[
            ApplyConfig.SIDE_OUTPUT_TAG_FILTERED_ROW]

        # Write to BQ
        transformed_main | 'DataWriteToBQ' >> beam.io.WriteToBigQuery(
            OUTPUT_TABLE,
            schema=SCHEMA,
            create_disposition=df.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=df.io.BigQueryDisposition.WRITE_TRUNCATE)

        transformed_filtered | 'FilteredDataWriteToBQ' >> beam.io.WriteToBigQuery(
            OUTPUT_TABLE_FILTERED,
            schema=SCHEMA,
            create_disposition=df.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=df.io.BigQueryDisposition.WRITE_TRUNCATE)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
