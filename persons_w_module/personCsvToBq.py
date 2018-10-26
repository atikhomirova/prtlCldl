from __future__ import absolute_import

import logging
import apache_beam as beam

from cdb_lib.cdb_lib import hello_world

schema = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING')

input = 'gs://baketto1/persons_reduced_42.csv'

output = 'micro-store-218714:person.personsReduced42'

PROJECT = 'micro-store-218714'
BUCKET = 'baketto1'
JOB_NAME = 'person-dataflow_through-airflow'

class FormatAsTableRow(beam.DoFn):
    def process(self, line):
        dct = {}
        l = line.split(',')
        dct["ID"] = int(l[0])
        dct["FirstName"] = str(l[1])
        dct["LastName"] = str(l[2])
        dct["Address"] = str(l[3])
        hello_world()
        return [dct]


def run():
    """Build and run the pipeline."""

    pipelineOptions = [
        '--project={0}'.format(PROJECT),
        '--job_name={0}'.format(JOB_NAME)',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/staging/'.format(BUCKET),
        '--setup_file ./setup.py',
        '--streaming',
        #'--runner=DirectRunner'
        '--runner=DataflowRunner'
        ]

    with beam.Pipeline(argv=pipelineOptions) as p:

        # Read the text from CSV file.
        lines = p | 'Read' >> beam.io.ReadFromText(input, skip_header_lines=1)

        transformed = lines | 'Transform' >> beam.ParDo(FormatAsTableRow())

        # Write to BigQuery.
        transformed | 'Write' >> beam.io.WriteToBigQuery(
            output,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
