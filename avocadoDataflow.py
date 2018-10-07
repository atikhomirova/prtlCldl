from __future__ import absolute_import

import logging
import apache_beam as beam

schema = ('Index:INTEGER, Date:DATE, AveragePrice:FLOAT, Total Volume:FLOAT, 4046:FLOAT, 4225:FLOAT, 4770:FLOAT,'
              'Total Bags:FLOAT, Small Bags:FLOAT, Large Bags:FLOAT, XLarge Bags:FLOAT, type:STRING, year:INTEGER, region:STRING')

input = 'gs:\\....'

output = 'micro-store-218714:avocadoDataset.avocado'


class FormatAsTableRow(beam.DoFn):
    def process(self, line):
        k_ = 'Index,Date,AveragePrice,Total Volume,4046,4225,4770,Total Bags,Small Bags,Large Bags,XLarge Bags,type,year,region'
        k = k_.split(',')
        z = dict(zip(k, line))
        yield [z]


def run(argv=None):
    """Build and run the pipeline."""

    with beam.Pipeline(argv=sys.argv) as p:
    
        # Read the text from CSV file.
        lines = p | 'Read' >> beam.io.ReadFromText(input)
    
        transformed = (lines
                       | beam.ParDo(FormatAsTableRow())
    
        # Write to BigQuery.
        transformed | 'Write' >> beam.io.WriteToBigQuery(
            output,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()