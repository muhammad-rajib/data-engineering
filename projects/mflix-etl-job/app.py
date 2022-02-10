"""
Call required functions to complete the ETL Job.
"""
# import required libraries
import os
from dotenv import load_dotenv

from utils import get_spark_session, \
                    connect_with_mongodb
from collect_data import collect_mflix_data
from process_data import create_dataframe, \
                            process_mflix_data
from load_data import load_mflix_data


def main():
    
    # create spark session

    # collect data from mongodb

    # store collected data into dataframe

    # process data followed by requirements

    # load data into warehouse

    pass


if __name__ == "__main__":
    main()
