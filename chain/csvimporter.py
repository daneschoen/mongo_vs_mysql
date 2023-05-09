import os, sys
import csv
import re
import logging
from chain import ImporterLink
from importer_global import *

class CsvImporter(ImporterLink):

    def next(self, db, meta, data):
        logging.debug("Attempting to identify as MACCOR format.")
        data.seek(0)
        maccor_header_regex = re.compile("^Today's Date\s*(?P<todays_date>.*?)\s*Date of Test:\s*(?P<date_of_test>.*?)\s*Filename:\s*(?P<filename>.*?)\s*Procedure:\s*(?P<procedure>.*?)\s*Comment/Barcode:\s*(?P<comment>.*?)\s*$")
        meta_match = maccor_header_regex.match(data.next())
        if meta_match:
            meta['type'] = 'maccor'
            meta['maccor_header'] = meta_match.groupdict()
            db.meta.save(meta)
            logging.debug("Identified as MACCOR format. Meta record in mongo updated.")
            return self.import_(db, meta, data)
        else:
            logging.debug("Did not identify as MACCOR format.")
            return self.call_next(db, meta, data)    

    def import_(self, db, meta, data):
        logging.debug("Importing data into maccor collection.")
        data.seek(0)
        sample = data.read(4096)
        data.seek(0)
        data.next()
        
        _dialect = csv.Sniffer().sniff(sample, "\t")
        reader = csv.DictReader(
            data,
            restkey='_unknown_field',
            dialect=_dialect
        )
        
        db.maccor.insert(reader)
        
        meta['imported'] = True
        db.meta.save(meta)
        logging.debug("MACCOR data imported. Meta record in mongo updated.")
        return True
    
    