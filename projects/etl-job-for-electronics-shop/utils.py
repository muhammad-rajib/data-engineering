"""
Utility functions for ETL job of sales data.
"""
# import required libraries
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession


def spark_session_env_dict():
    """
    function: return dictionary wrapped with spark-session credentials.
    :credentials are loaded from .env file.
    """
    load_dotenv()
    return {
        'mode'     : os.getenv('spark_session_mode'),
        'app_name' : os.getenv('spark_app_name')
    }

def s3_env_dict():
    """
    function: return dictionary wrapped with aws s3 credentials.
    :credentials are loaded from .env file.
    """
    load_dotenv()
    return {
        'file_name'     : os.getenv('s3_file_name'),
        'bucket_name'   : os.getenv('s3_bucket_name'),
        'aws_iam_role'  : os.getenv('s3_aws_iam_role')
    }

def redshift_env_dict():
    """
    function: return dictionary wrapped with aws redshift credentials.
    :credentials are loaded from .env file.
    """
    load_dotenv()
    return {
        'endpoint'  : os.getenv('redshift_endpoint'),
        'user'      : os.getenv('redshift_user'),
        'password'  : os.getenv('redshift_password'),
        'port'      : os.getenv('redshift_port'),
        'db_name'   : os.getenv('redshift_db_name'),
        'table_name': os.getenv('redshift_db_table_name')
    }

def mongodb_connection_env_dict():
    """
    function: return dictionary wrapped with mongodb credentials.
    :credentials are loaded from .env file.
    """
    load_dotenv()
    return {
        'user'      : os.getenv('mongodb_user'),
        'password'  : os.getenv('mongodb_password'),
        'db_name'   : os.getenv('mongodb_name'),
        'host'      : os.getenv('mongodb_host_address')
    }

def get_spark_session(spark_cred):
    """
    Create spark session.

    :param --> spark_cred: credentials wrapped with dictionary.
    :return newly created spark-session.
    """
    if spark_cred['mode'] == 'DEV':
        spark = SparkSession. \
            builder. \
            master('local'). \
            appName(spark_cred['mode']). \
            getOrCreate()
        return spark
    elif spark_cred['mode'] == 'PROD':
        spark = SparkSession. \
            builder. \
            master('yarn'). \
            appName(spark_cred['app_name']). \
            getOrCreate()
        return spark
    return

def connect_with_mongodb(mongodb_cred):
    """
    Connect with MongoDB Cluster.

    :param --> mongodb_cred: credentials wrapped with dictionary.
    :return mongodb connection obejct
    """
    user        = mongodb_cred['user']
    password    = mongodb_cred['password']
    db_name     = mongodb_cred['db_name']
    db_host     = mongodb_cred['host']

    mongo_uri = f"mongodb+srv://{user}:{password}{db_host}{db_name}?retryWrites=true&w=majority"
    mongo_client = MongoClient(mongo_uri)
    
    return mongo_client
