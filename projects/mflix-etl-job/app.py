"""
Call required functions to complete the ETL Job.
"""
# import required libraries
from utils import get_spark_session
from collect_data import collect_movies_data_into_list
from process_data import process_movies_data
from load_data import load_data_into_warehouse


def main():
    
    # create spark session
    spark = get_spark_session()

    # collect movies data list
    movies_list = collect_movies_data_into_list()

    # process movies data
    movies_output_df = process_movies_data(spark, movies_list)

    # load data into warehouse
    load_data_into_warehouse(movies_output_df)


if __name__ == "__main__":
    main()
