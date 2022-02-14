"""
Process sales data for analytics.
"""
# import libraries
from datetime import datetime
from pyspark.sql.types import (StructField, StructType, StringType, 
                                IntegerType, DateType, FloatType)
from pyspark.sql.functions import when


def make_tuple_list(data_list):
    """
    create tuple from sales data dictionary.

    :param --> data_list: list container of sales data dictionaries. 
    :return tuples of sales data wrapped with list.
    """
    sales_data_list = []

    for data in data_list:
        purchase_date           = datetime(data['saleDate'].year, data['saleDate'].month, data['saleDate'].day)
        store_location          = data['storeLocation']
        purchase_method         = data['purchaseMethod']
        customer_gender         = data['customer']['gender']
        customer_age            = data['customer']['age']
        customer_satisfaction   = data['customer']['satisfaction']

        for item in data['items']:
            purchase_item       = item['name']
            price               = float(str(item['price']))
            quantity            = item['quantity']
            single_sales_data   = (purchase_date, purchase_item, price, quantity,
                                        store_location, purchase_method, customer_gender, 
                                            customer_age, customer_satisfaction)
            
            sales_data_list.append(single_sales_data)

    return sales_data_list


def transform_sales_data(spark, data_list):
    """
    Process sales data using pyspark.

    :param1 --> spark: spark context
    :param2 --> data_list: list container of sales data dictionaries. 
    :return spark dataframe of processed sales data.
    """
    # make list of tuples of sales data
    data_list = make_tuple_list(data_list)
    
    # create a schema for the dataframe
    schema = StructType([
        StructField('Date', DateType(), True),
        StructField('Item', StringType(), True),
        StructField('Price', FloatType(), True),
        StructField('Quantity', IntegerType(), True),
        StructField('Store Location', StringType(), True),
        StructField('Purchase Method', StringType(), True),
        StructField('Gender', StringType(), True),
        StructField('Age', IntegerType(), True),
        StructField('Satisfaction', IntegerType(), True)
    ])

    # convert list to rdd
    sales_rdd = spark.sparkContext.parallelize(data_list)

    # create dataframe from rdd
    sales_df = spark.createDataFrame(sales_rdd, schema)
    
    # add new 'Total Amount($)' Column into DataFrame
    sales_df = sales_df.withColumn("Total Amount($)", sales_df.Price * sales_df.Quantity)

    # rearrange the order of sales dataframe
    sales_df = sales_df.select('Date', 'Item', 'Price', 'Quantity', 'Total Amount($)',
                                'Store Location', 'Purchase Method', 'Gender', 'Age', 'Satisfaction')

    # set full form of gender column
    sales_df = sales_df.withColumn("Gender", when(sales_df.Gender == "M", "Male") \
                        .when(sales_df.Gender == "F", "Female") \
                        .otherwise(sales_df.Gender))

    return sales_df
