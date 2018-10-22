from __future__ import absolute_import

import sys
import logging
import apache_beam as beam

schema = ('Index:INTEGER, Date:STRING, AveragePrice:FLOAT, TotalVolume:FLOAT, N4046:FLOAT, N4225:FLOAT, N4770:FLOAT, TotalBags:FLOAT, SmallBags:FLOAT, LargeBags:FLOAT, XLargeBags:FLOAT, type:STRING, year:INTEGER, region:STRING')

input = 'gs://baketto1/avoado.csv'

output = 'micro-store-218714:avocadoDataset.avocado'


class FormatAsTableRow(beam.DoFn):
    def process(self, line):
        z = {}
        l = line.split(',')
        z["Index"] = int(l[0])
        z["Date"] = str(l[1])
        z["AveragePrice"] = float(l[2])
        z["TotalVolume"] = float(l[3])
        z["N4046"] = float(l[4])
        z["N4225"] = float(l[5])
        z["N4770"] = float(l[6])
        z["TotalBags"] = float(l[7])
        z["SmallBags"] = float(l[8])
        z["LargeBags"] = float(l[9])
        z["XLargeBags"] = float(l[10])
        z["type"] = str(l[11])
        z["year"] = int(l[12])
        z["region"] = str(l[13])
        yield [z]


def run(argv=None):
    """Build and run the pipeline."""

    with beam.Pipeline(argv=sys.argv) as p:

        # Read the text from CSV file.
        lines = p | 'Read' >> beam.io.ReadFromText(input)

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