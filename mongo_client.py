from pymongo import MongoClient

class Connect(object):
    @staticmethod    
    def get_connection():
        return MongoClient("mongodb://admin:password@192.168.86.99:27017/stocks")