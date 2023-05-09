from pymongo import MongoClient
import pymysql, pymysql.cursors


mysql_conn = pymysql.connect(host='localhost',
                             port=3306,
                             database='sensor_turbine',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


class MongoDBConnectionManager():
    def __init__(self, hostname='localhost', port=27017):
        self.hostname = hostname
        self.port = port
        self.connection = None

    def __enter__(self):
        self.connection = MongoClient(self.hostname, self.port)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.connection.close()
