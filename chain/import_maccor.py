
import sys
from pymongo import MongoClient, son_manipulator
import datetime
import ast
import re
#import csv

DELIMITER = "|"
db_name = 'dept'

FILE_BING_NEWS = 'bing_news_p.csv'
FILE_DOWNLOADED_URLS = 'downloaded_urls_p.csv'

COLL_BING_NEWS = 'bing_news'
COLL_DOWNLOADED_URLS = 'downloaded_urls'

BING_NEWS_KEYS = ['uuid', 'url', 'creation_date', 'url_downloaded', 'result']
BING_NEWS_KEYS_RESULT = ['Description', 'Title', 'Url', '__metadata', 'Source', 'Date', 'ID']
BING_NEWS_KEYS_RESULT_METADATA = ['type', 'uri']


def get_db():
    
  client = MongoClient('localhost', 27017)
  db = client[db_name]
  return db


def test():
    doc = {"author": "Mike",
           "text": "sample text",
           "tags": ["mongodb", "python", "pymongo"],
           "date": datetime.datetime.utcnow()
           
          }
    p = {"foo":"bar"}
    doc["results"] = p
    print doc
    db = get_db()
    coll = db[COLL_BING_NEWS]    # coll = db.bing_news
    col_id = coll.insert(doc)


def clean_unicode(s):
    # clean incorrect escaping by postgres export
    s = s.replace('\\u','\u')
    return s


def clean_escape(s):
    # clean incorrect escaping by postgres export
    s = s.replace('\\\\"','')
    s = s.replace('\\",', '",')    
    s = s.replace(r'\\"', '')
    return s


def test_escapes():
    n=0
    with open(FILE_BING_NEWS,'r') as f:
        for row in f:
            cols = row.split(DELIMITER, 4)
            
            # construct json
            doc = {}
            for k in range(4):
                doc[BING_NEWS_KEYS[k]] = cols[k].strip()
            # parse 'result'            
            doc_result = {}
            result = cols[4].strip()
            # clean incorrect escaping by postgres export
            result = clean_escape(result)            
            #print result
            try:
                doc_result = ast.literal_eval(result)
            except Exception:    
            #except ValueError:
                """
                print row
                print
                print result
                sys.exit("ast literal eval error")
                """
                doc = {}
                doc_result = {}
                doc[BING_NEWS_KEYS[0]] = cols[0].strip()
                # url
                x = row.find(DELIMITER)
                s = row[x+1:] 
                x = s.find('|t|{"Description"')
                if x == -1:
                    x = s.find('|f|{"Description"')
                    if x == -1:
                        print "Error for this record"
                        continue
                    else:
                        doc[BING_NEWS_KEYS[3]] = "f"
                else:
                    doc[BING_NEWS_KEYS[3]] = "t"
                result = s[x+3:].strip()
                result = clean_escape(result)
                try:
                    doc_result = ast.literal_eval(result)
                except Exception:
                    print "> malformed"
                    continue
                s = s[:x]
                x = s.rfind(DELIMITER)
                doc[BING_NEWS_KEYS[2]] = s[x+1:].strip()
                doc[BING_NEWS_KEYS[1]] = s[:x].strip()
                print ">", doc[BING_NEWS_KEYS[0]]
            else:
                doc[BING_NEWS_KEYS[-1]] = doc_result
              
            print n
            n += 1
            

def run_import_bing_news():
    
    db = get_db()
    coll = db[COLL_BING_NEWS]    # coll = db.bing_news
    #col_id = coll.insert(doc)
    
    n=0
    with open(FILE_BING_NEWS,'r') as f:
        #for row in csv.reader(f):
        for row in f:
            cols = row.split(DELIMITER, 4)
            
            # construct json
            doc = {}
            for k in range(4):
                doc[BING_NEWS_KEYS[k]] = cols[k].strip()
            # parse 'result'
            doc_result = {}
            result = cols[4].strip()
            result = clean_escape(result)
            """
            result = result[1:-1]
            doc_result ={}
            for k in range(3):
                doc_result[BING_NEWS_KEYS[k]] = cols[k].strip()
            for k in range(4,7):
                doc_result[BING_NEWS_KEYS_RESULT[k]] = cols_result[]    
            """
            try:
                doc_result = ast.literal_eval(result)
            except Exception:                
            #except ValueError:
                """
                print row
                print
                print result
                sys.exit("ast literal eval error - probably delimiter problem")
                """
                doc = {}
                doc_result = {}
                doc[BING_NEWS_KEYS[0]] = cols[0].strip()
                # url
                x = row.find(DELIMITER)
                s = row[x+1:] 
                x = s.find('|t|{"Description"')
                if x == -1:
                    x = s.find('|f|{"Description"')
                    if x == -1:
                        print "> malformed for this record"
                        continue
                    else:
                        doc[BING_NEWS_KEYS[3]] = "f"
                else:
                    doc[BING_NEWS_KEYS[3]] = "t"
                result = s[x+3:].strip()
                result = clean_escape(result)
                try:
                    doc_result = ast.literal_eval(result)
                except Exception:
                    print "> malformed for this record - result"
                    continue
                s = s[:x]
                x = s.rfind(DELIMITER)
                doc[BING_NEWS_KEYS[2]] = s[x+1:].strip()
                doc[BING_NEWS_KEYS[1]] = s[:x].strip()
                print ">", doc[BING_NEWS_KEYS[0]]
            else:
                doc[BING_NEWS_KEYS[-1]] = doc_result
              
            doc_id = coll.insert(doc)
            print n, doc_id
            n += 1
            
"""
DOWNLOADED_URLS_KEYS = ['uuid', 'url', 'downloaded_date', 'contents']

'0df6b263-d2dc-3430-82c2-60acb6f3f603|http://www.bloomberg.com/news/2012-12-05/e-cigarette-maker-njoy-seen-as-takeover-target-amid-innovation.html
|2012-12-13 04:29:37.647641|<!DOCTYPE ...'
"""
def run_import_downloaded_urls():
    
    db = get_db()
    coll = db[COLL_DOWNLOADED_URLS]    # coll = db.bing_news
    #col_id = coll.insert(doc)
    
    n=0
    with open(FILE_DOWNLOADED_URLS,'r') as f:
        for row in f:
            cols = row.split(DELIMITER, 3)
            
            # construct json
            doc = {}
            x = row.find(DELIMITER)
            doc[DOWNLOADED_URLS_KEYS[0]] = row[:x]
            s = row[x+1:]
            x = re.search(r'\|\d{4}-\d{2}-\d{2}',s).start()   # timestamp
            doc[DOWNLOADED_URLS_KEYS[1]] = s[:x]
            s = s[x+1:]
            x = s.find(DELIMITER)
            doc[DOWNLOADED_URLS_KEYS[2]] = s[:x]
            doc[DOWNLOADED_URLS_KEYS[3]] = s[x+1:]
            try:
                doc_id = coll.insert(doc)
            except Exception:
                print doc
            print n, doc_id
            n += 1
            
def run_import_maccor(filename):
    db = get_db()
    
"""
- Start by subclassing pymongo.son_manipulator.SONManipulator and
- define transform_incoming/transform_outgoing to modify docs as they come out and go into mongodb.Then you just
- need to register this with the Database object by calling Database().add_son_manipulator(...).

In my case, I use a document class which subclasses dict and supports attribute-style access, type checking, custom validators, and a few
other small things.
"""
def insert(db, ):
    
    db.project.insert( [ { item: "pencil", qty: 50, type: "no.2" },
                         {          item: "pen", qty: 20 },
                         {          item: "eraser", qty: 25 }
                       ] )


    post = {"author": "Mike",
            "text": "My first blog post!",
            "tags": ["mongodb", "python", "pymongo"],
            "date": datetime.datetime.utcnow()
            "series:": {
                time0:     
            }
           }

    posts = db.posts
    post_id = posts.insert(post)     

if __name__ == '__main__':
    #test_escapes()
    
    #run_import_bing_news()
    #run_import_downloaded_urls()
    
    filename = 'MACCOR\ STATION\ 1_K19_Sanyo_PORATV_25C.113'
    run_import_maccor(filename)    
    

