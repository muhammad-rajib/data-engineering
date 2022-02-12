"""
Extract mflix data from MongoDB Atlas Cluster
and return the list of collected Data.
"""
from utils import connect_with_mongodb


def collect_data_into_list():
    client = connect_with_mongodb()
    sales_data = client.sample_supplies.sales
    
    # prepare movies list
    sales_data_list = []
    for item in sales_data.find():
        del item['_id']
        sales_data_list.append(item)

    return sales_data_list
