from __future__ import absolute_import

import google.cloud.storage

def llist_blobs(bucket_name, prfx):
    storage_client = google.cloud.storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prfx)
    return blobs

def hello_world():
    print "Hello world!"
