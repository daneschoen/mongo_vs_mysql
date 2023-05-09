import logging
from chain import ImporterLink

class DefaultImporter(ImporterLink):
    def next(self, db, meta, data):
        meta['type'] = 'unknown'
        meta['imported'] = True
        db.meta.save(meta)
        logging.debug("Identified as unknown format. Meta record in mongo updated.")
        return True
