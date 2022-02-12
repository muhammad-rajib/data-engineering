"""
Call required functions to complete the ETL Job.
"""
# import required libraries
from utils import get_spark_session
from collect_data import collect_data_into_list
from process_data import process_data
from load_data import load_data_into_warehouse


def main():
    
    # create spark session
    spark = get_spark_session()

    # collect movies data list
    sales_data_list = collect_data_into_list()

    # process movies data
    # sales_output_df = process_data(spark, sales_data_list)

    # load data into warehouse
    # load_data_into_warehouse(movies_output_df)


if __name__ == "__main__":
    main()
