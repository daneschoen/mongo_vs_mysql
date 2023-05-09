
import os, sys
import csv
import re
import logging
from chain import ImporterLink
from importer_global import *

def import_(db, meta, data):

data = f
data.seek(0)
sample = data.read(4096)
data.seek(0)
data.next()

data.next()
size_rec = len(data.next())
data.seek(0)
data.next()


bucket_tot = os.stat(meta['filepath']).st_size/MONGO_DOC_MAX_SIZE
if bucket_tot == 0:
    bucket_tot = 1
    bucket_rows = sys.maxint
    bucket_size = os.stat(meta['filepath']).st_size + 1024
else:
    bucket_rows = int((float(os.stat(meta['filepath']).st_size)/size_rec - 2) / bucket_tot) + 1000
    bucket_size = float(os.stat(meta['filepath']).st_size) / bucket_tot


_dialect = csv.Sniffer().sniff(sample, "\t")
reader = csv.DictReader(
    data,
    restkey='_unknown_field',
    dialect=_dialect
)


db.maccor2.insert(reader)


"""
data_arr = []
i=0
rec_beg=0
rec_end=0
bucket_cur_size=0
for rec in reader:
    if i < bucket_rows:
        data_arr.append(rec)
        i += 1
    else:
        rec_end = rec_beg+i-1
        bucket = {"rec_beg": rec_beg,
                  "rec_end": rec_end,
                  "data": data_arr
        }
        db.maccor.insert(bucket)
        
        data_arr = [rec]
        rec_beg = rec_end+1
        i=0
        
if i > 0:
    rec_end = rec_beg+i-1
    bucket = {"rec_beg": rec_beg,
              "rec_end": rec_end,
              "data": data_arr
    }
    db.maccor.insert(bucket)

"""
