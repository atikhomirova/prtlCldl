import csv
import uuid
import logging
import apache_beam as beam

#schema = ('ID:INTEGER, FirstName:STRING, LastName:STRING, Address:STRING, UUID:STRING')

input = 'gs://baketto1/persons_reduced_names.csv'


PROJECT = 'micro-store-218714'
BUCKET = 'baketto1'



name_mapping = {'ID':'ID', 'LAST_NAME': 'LastName', 'FIRST_NAME': 'FirstName', 'ADDR': 'Address'}

class CsvFileSource(beam.io.filebasedsource.FileBasedSource):
    def __init__(self, file_pattern,
                 compression_type=beam.io.filesystem.CompressionTypes.AUTO):
        super(self.__class__, self).__init__(file_pattern,
                                             compression_type=compression_type, splittable=False)

    def read_records(self, file_name, range_tracker):
        self._file = self.open_file(file_name)

        names = csv.reader(self._file, delimiter=',').next()

        #fieldnames = tuple([name_mapping[name] if name in name_mapping else name for name in names])
        #print fieldnames

        reader = csv.DictReader(self._file, delimiter=',', quotechar='"', fieldnames=names)

        for rec in reader:
           # print rec
            print rec.keys()
            new_rec = {name_mapping[k] : rec[k] for k in rec.keys() if k in name_mapping}
            new_rec['UUID'] = uuid.uuid1().hex
            yield new_rec


def run():
    """Build and run the pipeline."""

    pipelineOptions = [
        '--project={0}'.format(PROJECT),
        '--job_name=person-csv_no_schema',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/staging/'.format(BUCKET),
        '--runner=DirectRunner'
        # '--runner=DataflowRunner'
        ]

    with beam.Pipeline(argv=pipelineOptions) as p:

        lines = p | 'Read CSV' >> beam.io.Read(CsvFileSource(input))

        lines | 'Write Products into staging' >> beam.io.Write(beam.io.BigQuerySink(table='personCsv', dataset='person', #schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
