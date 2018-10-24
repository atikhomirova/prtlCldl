import sys
import logging
import apache_beam as beam

schema = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING')

input = 'micro-store-218714:person.personsExtendedOriginalDataflow'
output = 'micro-store-218714:person.personsExtendedTransformedDataflow2'

PROJECT = 'micro-store-218714'
BUCKET = 'baketto1'


def trim_whitespaces(line):
    return line.strip()

def remove_extra_whitespaces(line):
    return ' '.join(line.split())

def skip_row_with_value(line, value_to_skip):
    if value_to_skip in line:
        return None
    else:
        return line

def call_function_from_str(str_function, dct, name):
    try:
        function, arg = str_function.split(',') #How an argument should be put in config?
        dct = eval(function)(dct, name, arg)
    except:
        function = str_function
        dct = eval(function)(dct, name)

    return dct


config = {'FirstName': ('trim_whitespaces'),
          'LastName': ('trim_whitespaces'),
          'Address': ('trim_whitespaces', 'remove_extra_whitespaces', 'skip_row_with_value,42')}


class FormatAsTableRow(beam.DoFn):
    def process(self, line):
        dct = line.copy()

        for name in config.keys():
            functions = config[name]
            for function in functions:
                if dct is not None:
                    dct = call_function_from_str(function, dct, name)

        return [dct]


def run():
    """Build and run the pipeline."""

    pipelineOptions = [
        '--project={0}'.format(PROJECT),
        '--job_name=person-extended-transform-job',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/staging/'.format(BUCKET),
        #'--runner=DirectRunner'
        '--runner=DataflowRunner'
    ]

    with beam.Pipeline(argv=pipelineOptions) as p:

        # Read the text from CSV file.
        lines = p | 'ReadFromBQ' >> beam.io.Read(beam.io.BigQuerySource(input))

        transformed = lines | 'Transform' >> beam.ParDo(FormatAsTableRow())

        # Write to BigQuery.
        transformed | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            output,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
