"""
Utility functions for ETL job of mflix.
"""
# import required libraries
import os
from dotenv import load_dotenv
from pymongo import MongoClient
# from pyspark.sql import SparkSession


def get_spark_session(dev_mode, app_name):
    pass   

def mongodb_env_dict():
    load_dotenv()
    return {
        'user' : os.getenv('mongodb_user'),
        'password' : os.getenv('mongodb_password'),
        'db_name' : os.getenv('mongodb_name')
    }

def connect_with_mongodb():
    cred_dict = mongodb_env_dict()
    user = cred_dict['user']
    password = cred_dict['password']
    db_name = cred_dict['db_name']

    mongo_uri = f"mongodb+srv://{user}:{password}@mflix-cluster.buei4.mongodb.net/{db_name}?retryWrites=true&w=majority"
    mongo_client = MongoClient(str(mongo_uri))
    
    return mongo_client


if __name__ == "__main__":
    client = connect_with_mongodb()
    for name in client.list_database_names():
        print(name)
