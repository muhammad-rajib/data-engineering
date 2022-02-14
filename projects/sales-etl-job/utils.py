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
        'db_name': os.getenv('redshift_db_name')
    }

def mongodb_env_dict():
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

def mongodb_env_dict2():
    """
    function: return dictionary wrapped with mongodb credentials.
    :credentials are loaded from .env file.
    """
    load_dotenv()
    return {
        'db_name'   : os.getenv('mongodb_name'),
        'table_name': os.getenv('mongodb_table_name')
    }

def get_spark_session():
    """
    function: return newly created spark session.
    :required informations are loaded from .env file.
    """
    cred_dict = spark_session_env_dict() 
    dev_mode = cred_dict['mode']
    app_name = cred_dict['app_name']

    if dev_mode == 'DEV':
        spark = SparkSession. \
            builder. \
            master('local'). \
            appName(app_name). \
            getOrCreate()
        return spark
    elif dev_mode == 'PROD':
        spark = SparkSession. \
            builder. \
            master('yarn'). \
            appName(app_name). \
            getOrCreate()
        return spark
    return

def connect_with_mongodb():
    """
    function: return dictionary wrapped with aws s3 credentials.
    :required informations are loaded from .env file by functions.
    """
    cred_dict = mongodb_env_dict()
    user = cred_dict['user']
    password = cred_dict['password']
    db_name = cred_dict['db_name']
    db_host = cred_dict['host']

    mongo_uri = f"mongodb+srv://{user}:{password}{db_host}{db_name}?retryWrites=true&w=majority"
    mongo_client = MongoClient(mongo_uri)
    
    return mongo_client
