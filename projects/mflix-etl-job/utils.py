"""
Utility functions for ETL job of mflix.
"""

# import required libraries
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession


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
    pass
