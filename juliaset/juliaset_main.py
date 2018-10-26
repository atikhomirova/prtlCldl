"""
python juliaset_main.py \
  --job_name juliaset-$USER \
  --project YOUR-PROJECT \
  --runner DataflowRunner \
  --setup_file ./setup.py \
  --staging_location gs://YOUR-BUCKET/juliaset/staging \
  --temp_location gs://YOUR-BUCKET/juliaset/temp \
  --coordinate_output gs://YOUR-BUCKET/juliaset/out \
  --grid_size 20
"""

from __future__ import absolute_import

import logging

from juliaset.juliaset import juliaset

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  juliaset.run()