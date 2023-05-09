
import os

from pymongo.son_manipulator import SONManipulator
from pymongo import MongoClient

import logging
from bson.objectid import ObjectId
from time import sleep
from chain import ImporterChain
from apdimporter import ApdImporter
from csvimporter import CsvImporter
from defaultimporter import DefaultImporter

from mongo_transformers import MetaIdInjector

logging.basicConfig(level = logging.DEBUG)


meta_id_injector = MetaIdInjector()
connection = MongoClient()
db = connection.import_test
db.add_son_manipulator(meta_id_injector)

importerChain = ImporterChain()
importerChain.add_link(ApdImporter())
importerChain.add_link(CsvImporter())
importerChain.add_link(DefaultImporter())

logging.info("Connected to mongo and ready for importing.")

while True:
    for doc_meta in db.meta.find({"imported": False}):
        
        logging.info("Processing data file " + doc_meta['filepath'])
        
        meta_id_injector.set_meta_id(ObjectId(doc_meta['_id']))
        
        try:
            f = open(doc_meta['filepath'], 'rb')
        except IOError as e:
            logging.info("I/O error({0}): {1}".format(e.errno, e.strerror))
        else:    
            importerChain.begin(db, doc_meta, f)
            f.close()
            
            # Rename archived file to unique name
            objIdstr = str(doc_meta['_id'])
            #filenameOrig = os.path.basename(archive_path)
            doc_meta['filepath'] =  doc_meta['filepath'] + "." + objIdstr
            db.meta.save(doc_meta)
        finally:
            if f: f.close()
    sleep(5)




