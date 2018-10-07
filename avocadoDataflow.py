from __future__ import absolute_import

import logging
import apache_beam as beam

schema = ('Index:INTEGER, Date:STRING, AveragePrice:FLOAT, Total Volume:FLOAT, 4046:FLOAT, 4225:FLOAT, 4770:FLOAT, Total Bags:FLOAT, Small Bags:FLOAT, Large Bags:FLOAT, XLarge Bags:FLOAT, type:STRING, year:INTEGER, region:STRING')

input = 'gs:\\....'

output = 'micro-store-218714:avocadoDataset.avocado'


class FormatAsTableRow(beam.DoFn):
    def process(self, line):
        z = {}
        l = line.split(',')
        z["Index"] = int(l[0])
        z["Date"] = str(l[1])
        z["AveragePrice"] = float(l[2])
        z["Total Volume"] = float(l[3])
        z["4046"] = float(l[4])
        z["4225"] = float(l[5])
        z["4770"] = float(l[6])
        z["Total Bags"] = float(l[7])
        z["Small Bags"] = float(l[8])
        z["Large Bags"] = float(l[9])
        z["XLarge Bags"] = float(l[10])
        z["type"] = str(l[11])
        z["year"] = int(l[12])
        z["region"] = str(l[13])
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