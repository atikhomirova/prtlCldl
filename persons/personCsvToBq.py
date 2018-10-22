import sys
import logging
import apache_beam as beam

schema = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING')

input = 'gs://baketto1/person.csv'

output = 'micro-store-218714:person.personOriginal'

PROJECT = 'micro-store-218714'
BUCKET = 'baketto1'


class FormatAsTableRow(beam.DoFn):
    def process(self, line):
        dct = {}
        l = line.split(',')
        dct["ID"] = int(l[0])
        dct["FirstName"] = str(l[1])
        dct["LastName"] = str(l[2])
        dct["Address"] = str(l[3])
        yield [dct]


def run():
    """Build and run the pipeline."""

    # pipelineOptions = [
        # '--project={0}'.format(PROJECT),
        # '--job_name=personJob',
        # '--save_main_session',
        # '--staging_location=gs://{0}/staging/'.format(BUCKET),
        # '--temp_location=gs://{0}/staging/'.format(BUCKET),
        # '--runner=DataflowRunner'
    # ]

    with beam.Pipeline(argv=sys.argv) as p:

        # Read the text from CSV file.
        lines = p | 'Read' >> beam.io.ReadFromText(input, skip_header_lines=1)

        transformed = lines | beam.ParDo(FormatAsTableRow())

        # Write to BigQuery.
        transformed | 'Write' >> beam.io.WriteToBigQuery(
            output,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()