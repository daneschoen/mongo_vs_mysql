
from pymongo.son_manipulator import SONManipulator
from pymongo import MongoClient

import logging
from bson.objectid import ObjectId
from time import sleep

from chainResp import IngestChain
from pvcIngest import PvcIngest
from dmpIngest import DmpIngest
from defaultIngest import DefaultIngest

from mongo_transformers import MetaIdInjector
from app_global import *

logging.basicConfig(level = logging.DEBUG)


meta_id_injector = MetaIdInjector()
conn = MongoClient()
db = conn[COLL_DATA]
db.add_son_manipulator(meta_id_injector)

ingestChain = IngestChain()
ingestChain.add_link(PvcIngest())
ingestChain.add_link(DmpIngest())
ingestChain.add_link(DefaultIngest())

while True:
    for meta in db.meta.find({"processed": False}):
        
        logging.info("Processing data file " + meta['filepath'])
        
        meta_id_injector.set_meta_id(ObjectId(meta['_id']))
        
        try:
            f = open(meta['filepath'], 'rb')
        except IOError as e:
            logging.info("I/O error({0}): {1}".format(e.errno, e.strerror))
        else:    
            ingestChain.begin(db, meta, f)
        finally:
            if f: f.close()
    sleep(5)




