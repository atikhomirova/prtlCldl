from __future__ import absolute_import

import argparse
import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import (GoogleCloudOptions,
                                                  PipelineOptions,
                                                  SetupOptions,
                                                  StandardOptions,
                                                  WorkerOptions)


schema = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING')

input = 'gs://baketto1/persons_reduced_42.csv'

output = 'micro-store-218714:person.personsReduced42'



#job_id = ''.join(e for e in str(datetime.datetime.now()) if e.isalnum())
#CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
BUCKET_URL = 'gs://baketto1'  # replace with your bucket name
PROJECT_ID = 'micro-store-218714'  # replace with your project id
JOB_NAME = 'person-test'  # replace with your job name



class FormatAsTableRow(beam.DoFn):
    def process(self, line):
        l = line.split(',')
        dct = {}
        dct["ID"] = int(l[0])
        dct["FirstName"] = str(l[1])
        dct["LastName"] = str(l[2])
        dct["Address"] = str(l[3])
        return [dct]


def run(argv=None):
    """Build and run the pipeline."""
    
    '''pipelineOptions = [
        '--project={0}'.format(PROJECT),
        '--job_name={0}'.format(JOB_NAME),
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/temp/'.format(BUCKET),
        #'--runner=DirectRunner',
        '--runner=DataflowRunner'
        #'--setup_file=./setup.py'
        ]'''

    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--runner',
        dest='runner',
        default='DirectRunner',
        help='DirectRunner or DataflowRunner')

    parser.add_argument(
        '--start_date',
        dest='start_date',
        default='2018-01-01',
        help='Start date')

    parser.add_argument(
        '--end_date',
        dest='end_date',
        default='2018-01-02',
        help='End date')

    known_args, extra_pipeline_options = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True

    logging.info('Basic pipeline options ready')

    google_cloud_options = pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.staging_location = BUCKET_URL + '/staging'
    google_cloud_options.temp_location = BUCKET_URL + '/temp'

    logging.info('Google cloud pipeline options are ready')

    pipeline_options.view_as(StandardOptions).runner = known_args.runner

    logging.info('Runner is set')    

    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text from CSV file.
        lines = p | 'Read' >> beam.io.ReadFromText(input, skip_header_lines=1)

        transformed = lines | 'Transform' >> beam.ParDo(FormatAsTableRow())

        # Write to BigQuery.
        transformed | 'Write' >> beam.io.WriteToBigQuery(
            output,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info('start run')
    logging.info(dir())
    run()