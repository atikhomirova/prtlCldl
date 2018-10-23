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


config = {'FirstName': ('trim_whitespaces'),
          'LastName': ('trim_whitespaces'),
          'Address': ('trim_whitespaces', 'remove_extra_whitespaces', 'skip_row_with_value(42)')}


class FormatAsTableRow(beam.DoFn):
    def process(self, line):
        dct = line.copy()

        #for name, value in dct.iteritems():
        #    if name in config.keys():
        #        functions = config[name]
        #        for function in functions:
        #            value  = eval(function + '(value)')
        #        dct[name] = value

        #return [dct]

        for name in config.keys():
            if name in dct.keys():
                functions = config[name]
                value = dct[name]
                for function in functions:
                    value  = eval(function + '(value)')
                dct[name] = value

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
        #query = 'SELECT * FROM person.personOriginal'
        #lines = p | 'ReadFromBQ' >> beam.io.Read(beam.io.BigQuerySource(project=PROJECT, use_standard_sql=False, query=query))

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
