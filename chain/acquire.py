#!/usr/bin/python
import os, errno, shutil
import time
from datetime import datetime
import logging

from watchdog.observers import Observer 
from watchdog.events import FileSystemEventHandler 
from watchdog.events import LoggingEventHandler

from pymongo import MongoClient


class fs_event_handler(FileSystemEventHandler):
    def __init__(self):
        FileSystemEventHandler.__init__(self)
    
    def on_created(self, event):
        if not event.is_directory:
            # create path based on date if it does not yet exist
            date_path = os.path.abspath('./data/archive/' + datetime.utcnow().strftime('%Y/%m/%d'))
            
            try:
                os.makedirs(date_path)   
            except OSError as exc:  
                if exc.errno == errno.EEXIST and os.path.isdir(date_path):
                    pass
            else: raise
            
            # move the file from incoming to archive
            incoming_path = event.src_path
            archive_path = os.path.join(date_path, os.path.basename(incoming_path))
            shutil.move(incoming_path, archive_path)
            logging.info("new file moved to archive: " + archive_path)
            
            # save initial meta document
            archive_record = {
                'filepath': archive_path,
                'imported': False
            }
            coll_meta.save(archive_record)

if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    
    coll_meta = MongoClient().import_test.meta
    
    event_handler = fs_event_handler()
    observer = Observer()
    observer.schedule(event_handler, path=r"./data/incoming", recursive=True)
    
    observer.start()
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    
    observer.join()
    
    