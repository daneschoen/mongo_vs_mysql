import yaml
import re
import logging
from chain import ImporterLink

def dict_yaml_constructor(loader, node):
    return loader.construct_mapping(node)

class ApdImporter(ImporterLink):

    def next(self, db, meta, data):
        logging.debug("Attempting to identify as APD format.")
        data.seek(0)
        if re.search('^--- !test_results', data.next()):
            meta['type'] = 'apd'
            db.meta.save(meta)
            logging.debug("Identified as APD format. Meta record in mongo updated.")
            return self.import_(db, meta, data)
        else:
            logging.debug("Did not identify as APD format.")
            return self.call_next(db, meta, data) 

    def import_(self, db, meta, data):
        logging.debug("Importing data into apd collection.")
        data.seek(0)
        yaml.add_constructor('!test_results', dict_yaml_constructor)
        yaml.add_constructor('!dictionary_result', dict_yaml_constructor)
        db.apd.insert(yaml.load(data))
        meta['imported'] = True
        db.meta.save(meta)
        logging.debug("APD data imported. Meta record in mongo updated.")
        return True
