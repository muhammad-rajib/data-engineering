"""
Call required functions to complete the ETL Job.
"""
# import required libraries
from utils import (s3_env_dict,
                    redshift_env_dict,
                    mongodb_connection_env_dict,
                    spark_session_env_dict,
                    get_spark_session,
                    connect_with_mongodb
                )
from collect_data import collect_data_into_list
from process_data import transform_sales_data
from load_data import load_data_into_s3_bucket
from load_data import load_data_s3_to_redshift


def main():
    """
    Complete the ETL Process.

    :after complete the execution of main function
    :sales data will be available in aws s3 Data-Lake
    :and aws s3 redshift Data-Warehouse
    """
    # mongodb connectioncredentials from .env file
    mongodb_conn_cred = mongodb_connection_env_dict()
    # create mongodb connection client object
    mongodb_client = connect_with_mongodb(mongodb_conn_cred)
    # collect sales data from mongodb
    sales_data_list = collect_data_into_list(mongodb_client)

    # spark credentials from .env file
    spark_cred = spark_session_env_dict()
    # create spark session
    spark = get_spark_session(spark_cred)
    
    # transform sales data using pyspark
    sales_df = transform_sales_data(spark, sales_data_list)

    # convert pyspark dataframe to pandas dataframe
    sales_pd_df = sales_df.toPandas()

    # s3 credentials from .env file
    s3_cred = s3_env_dict()
    # load data into s3 bucket
    load_data_into_s3_bucket(sales_pd_df, s3_cred)

    # redshift credentials from .env file
    redshift_cred = redshift_env_dict()
    # load data into redshift data-warehouse
    load_data_s3_to_redshift(s3_cred, redshift_cred)


if __name__ == "__main__":
    main()
