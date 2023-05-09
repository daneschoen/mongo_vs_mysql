#!/usr/bin/env python3

import sys
import json
import argparse
import perfplot
import numpy as np
import pandas as pd

from bson.objectid import ObjectId

from db import MongoDBConnectionManager, mysql_conn
from constants import *
from decorators import timer

sys.path.append('..')
import windesco as we


def setup(n):
    A = np.random.rand(n, n)
    x = np.random.rand(n)
    return A, x


def at(data):
    A, x = data
    return A @ x


def np_dot(data):
    A, x = data
    return np.dot(A, x)


def np_matmul(data):
    A, x = data
    return np.matmul(A, x)


perfplot.show(
    setup=setup,
    kernels=[at, np_dot, np_matmul],
    n_range=[2 ** k for k in range(15)],
)


def get_mysql_db():
    return we.db.Database()

def get_mongo_db():
    return get_mongodb(MONGODB_MAIN)

def get_data(db):
    return db.scada_turbine.get_data(turbine_id=14, start_ts="2021-08-01", end_ts="2021-08-01 01:00")

def get_data_raw(db, lim=10000):
    return db.dataframe_query(f"select * from scada_turbine_nrw limit {lim}")


def to_dict_records(df):
    data = df.values.tolist()
    columns = df.columns.tolist()
    return [
        dict(zip(columns, datum))
        for datum in data
    ]

def to_dict_records_index(df):
    data = df.values.tolist()
    for l, y in zip(data, df.index):
        l.append(y)
    columns = df.columns.tolist()
    columns.append('ts')
    return [
        dict(zip(columns, datum))
        for datum in data
    ]


@timer
def time_mysql_ingest(lst_data):
    with mysql_conn:
        with mysql_conn.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO ingest(turbine_id, signal_def_id, ts, value) VALUES (%s, %s, %s, %s)"
            cursor.executemany(sql, lst_data)

        mysql_conn.commit()

@timer
def time_mysql_records_index(cols, lst_data):
    with mysql_conn:
        with mysql_conn.cursor() as cursor:
            # Create a new record
            sql = f"INSERT INTO scada_turbine({cols}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s)"
            cursor.executemany(sql, lst_data)

        mysql_conn.commit()

@timer
def time_mysql_seek():
    with mysql_conn:
        with mysql_conn.cursor() as cursor:
            # Create a new record
            # sql = "select * from ingest where value <= 22.7;"
            sql = "select * from ingest where value > 25.5;"
            cursor.execute(sql)
            result = cursor.fetchone()


def run_benchmark_mysql(df_raw, df):
    print("\n" + "-"*80 + "\n" + "Starting benchmark for MySQL")
    print(df_raw.head())
    lst_data = df_raw.values.tolist()
    time_mysql_ingest(lst_dct)
    print("Elapsed time: 0.2551 seconds")

    df = df.replace({np.nan: None})
    print(df.head())

    cols = ', '.join(df.columns.tolist()) + ', ts'
    lst_data = df.values.tolist()
    for l, y in zip(lst_data, df.index):
        l.append(y)
    time_mysql_records_index(cols, lst_data)
    print('Elapsed time: 0.0443 seconds')

    time_mysql_seek()
    print("Elapsed time: 0.0724 seconds")


@timer
def time_mongo_ingest(lst_data):
    with MongoDBConnectionManager() as mongo:
        col = mongo.connection[MONGODB_MAIN][COL_INGEST]
        col.insert_many(lst_data)

@timer
def time_mongo_records_index(lst_data):
    with MongoDBConnectionManager() as mongo:
        col = mongo.connection[MONGODB_MAIN][COL_MAIN]
        col.insert_many(lst_data)

@timer
def time_mongo_seek():
    with MongoDBConnectionManager() as mongo:
        col = mongo.connection[MONGODB_MAIN][COL_INGEST]
        # cur = col.find({'value': { '$lte': 22.7 }})
        cur = col.find({'value': { '$gt': 25.5 }})
        print(cur[0])

        #col.find( { 'nacelledirection_deg': { '$lte': 302.9 } } )
        # db.scada_turbine.find( { 'nacelledirection_deg': { '$lte': 302.9 } } )
        # "select count(*) from scada_turbine where nacelledirection_deg <= 302.9;"

def run_benchmark_mongo(df_raw, df):
    print("\n" + "-"*80 + "\n" + "Starting benchmark for MongoDB")
    print(df_raw.head())
    lst_dct = to_dict_records(df_raw)
    time_mongo_ingest(lst_dct)
    print('Elapsed time: 0.0379 seconds')  # .1108

    print(df.head())
    lst_dct = to_dict_records_index(df)
    time_mongo_records_index(lst_dct)
    print('Elapsed time: 0.0365 seconds')

    time_mongo_seek()
    print("Elapsed time: 0.0196 seconds")


def run_benchmark_all(df_raw, df):
    run_benchmark_mysql(df_raw, df)
    run_benchmark_mongo(df_raw, df)


if __name__ == "__main__":
    ''' Usage:
    ./mongo_benchmark.py
    ./mongo_benchmark.py --dest mysql
    ./mongo_benchmark.py --dest mongodb
    '''

    parser = argparse.ArgumentParser()
    parser.add_argument('--dest', '-d', nargs='?',
        choices={"all", "mysql", "mongodb"}, default='all')
    args = parser.parse_args()

    mysqldb = get_mysql_db()
    mongodb = get_mongo_db()

    df_raw = get_data_raw(mysqldb)
    df = get_data(mysqldb)

    if args.dest == 'all':
        run_benchmark_all(df_raw, df)
    elif args.dest == 'mysql':
        run_benchmark_mysql(df_raw, df)
    elif args.dest == 'mongodb':
        run_benchmark_mongo(df_raw, df)
