import os
from dotenv import load_dotenv

from util import get_spark_session
from read import from_files
from process import transform
from write import to_files

load_dotenv()


def main():
    # values from .env files
    dev_mode = os.getenv('DEV_MODE')
    src_dir = os.getenv('SRC_DIR')
    src_file_pattern = f"{os.getenv('SRC_FILE_PATTERN')}-*"
    src_file_format = os.getenv('SRC_FILE_FORMAT')
    target_dir = os.getenv('TARGET_DIR')
    target_file_format = os.getenv('TARGET_FILE_FORMAT')
    
    spark = get_spark_session(dev_mode, 'GitHub Activity - Reading Data')
    
    # read data
    df = from_files(spark, src_dir, src_file_pattern, src_file_format)

    # transform data
    df_transformed = transform(df)

    # write data
    to_files(df_transformed, target_dir, target_file_format)


def show_final_data():
    final_data_dir = os.getenv('FINAL_DATA_DIR')
    dev_mode = os.getenv('DEV_MODE')

    spark = get_spark_session(dev_mode, 'GitHub Activity - Showing Data')
    
    df = spark.read.parquet(final_data_dir)
    df.show()


if __name__ == '__main__':
    main()
    