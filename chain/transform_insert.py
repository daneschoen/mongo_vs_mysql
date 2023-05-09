
import pymongo 
import MySQLdb

import numpy

#SET autocommit=0;

def insert():
    cur_analyticsAbc.executemany("INSERT INTO sessionCount VALUES(?, ?, ?, ?, ?)", initValues)
    
    
if __name__ == "__main__":
    insert()