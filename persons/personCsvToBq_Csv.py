
import logging
import apache_beam as beam

schema = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING')

input = 'gs://baketto1/person_extended_42.csv'

output = 'micro-store-218714:person.personFull42'

PROJECT = 'micro-store-218714'
BUCKET = 'baketto1'


name_mapping = {'LastName': 'lastname', 'FirstName': 'firstname', 'Address': 'address'}

class CsvFileSource(beam.io.filebasedsource.FileBasedSource):
    def __init__(self, file_pattern,
                 compression_type=beam.io.filesystem.CompressionTypes.AUTO):
        super(self.__class__, self).__init__(file_pattern,
                                             compression_type=compression_type, splittable=False)

    def read_records(self, file_name, range_tracker):
        self._file = self.open_file(file_name)

        names = csv.reader(self._file, delimiter=',').next()

        fieldnames = names

        reader = csv.DictReader(self._file, delimiter=',', quotechar='"',
                                fieldnames=fieldnames)

        for rec in reader:
            rec['Uuid'] = uuid.uuid1().hex
            yield rec


def run():
    """Build and run the pipeline."""

    pipelineOptions = [
        '--project={0}'.format(PROJECT),
        '--job_name=person-full-42',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/staging/'.format(BUCKET),
        # '--runner=DirectRunner'
        '--runner=DataflowRunner'
        ]

    with beam.Pipeline(argv=pipelineOptions) as p:

        lines = p | 'Read CSV' >> beam.io.Read(CsvFileSource(input))

        lines | 'Write Products into staging' >> beam.io.Write(beam.io.BigQuerySink(table='personCsv', dataset='person',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
