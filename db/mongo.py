from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from config import settings

class MongoDBClient:
    def __init__(self, db_name: str):
        self.client = MongoClient(settings.MONGO_URI)
        self.db = self.client[db_name]

    def get_collection(self, collection_name):
        return self.db[collection_name]
    
    def insert_post(self, data:dict, collection_name:str):
        collection = self.db[collection_name]
        try:
            collection.insert_one(data)
        except DuplicateKeyError:
            print("Duplicate key detected")

    def close(self):
        self.client.close()
