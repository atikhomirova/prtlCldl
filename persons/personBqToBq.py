import sys
import logging
import apache_beam as beam

schema = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING')

input = 'micro-store-218714:person.personOriginal'
output = 'micro-store-218714:person.personProcessed'

PROJECT = 'micro-store-218714'
BUCKET = 'baketto1'


def trim_whitespaces(line):
    return line.strip()

def remove_extra_whitespaces(line):
    return ' '.join(line.split())


config = {'FirstName': ['strip'], 
          'LastName': ['strip'],
		  'Address': ['strip', 'remove_extra_whitespaces']}


class FormatAsTableRow(beam.DoFn):
    def process(self, line):
        dct = {}
        l = line.split(',')	
        dct["ID"] = int(l[0])
        dct["FirstName"] = str(l[1])
        dct["LastName"] = str(l[2])
        dct["Address"] = str(l[3])
		
        for name, value in dct.iteritems():
		    functions = config[name]
            for function in functions:
                new_value  = eval(function + '(value)')
            dct[name] = new_value 

        yield [dct]
	
		
def run():
    """Build and run the pipeline."""

    pipelineOptions = [
        '--project={0}'.format(PROJECT),
        '--job_name=personJob',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/staging/'.format(BUCKET),
		'--runner=DirectRunner'
        #'--runner=DataflowRunner'
    ]

    with beam.Pipeline(argv=sys.argv) as p:

        # Read the text from CSV file.
        lines = p | 'ReadFromBQ' >> beam.io.Read(beam.io.BigQuerySource(input)

        transformed = lines | beam.ParDo(FormatAsTableRow())

        # Write to BigQuery.
        transformed | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            output,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()