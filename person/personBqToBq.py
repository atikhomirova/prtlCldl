import sys
import logging
import apache_beam as beam

schema = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING')

input = 'micro-store-218714:person.personFull42'
output = 'micro-store-218714:person.personFull42Transformed'

PROJECT = 'micro-store-218714'
BUCKET = 'baketto1'


def trim_whitespaces(dct, name):
    line = dct[name]
    dct[name] = line.strip()
    return dct

def remove_extra_whitespaces(dct, name):
    line = dct[name]
    dct[name] = ' '.join(line.split())
    return dct

def skip_row_with_value(dct, name, arg):
    if arg in dct[name]:
        return None
    else:
        return dct

def call_function_from_str(str_function, dct, name):
    if ',' in str_function:
        function, arg = str_function.split(',') #How an argument should be put in config?
        dct = eval(function)(dct, name, arg)
    else:
        dct = eval(str_function)(dct, name)

    return dct


config = {'FirstName': ('trim_whitespaces',),
          'LastName': ('trim_whitespaces',),
          'Address': ('skip_row_with_value,42','trim_whitespaces', 'remove_extra_whitespaces')}


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


def run():
    """Build and run the pipeline."""

    pipelineOptions = [
        '--project={0}'.format(PROJECT),
        '--job_name=person-full-42-transform',
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
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
