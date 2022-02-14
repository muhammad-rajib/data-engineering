"""
Extraction part of ETL job. 
"""

def collect_data_into_list(mongodb_client):
    """
    Collect data from mongodb cluster and load data into the list.

    :param1 --> mongodb_client: connection object of mongodb atlas cluster.
    :return sales data dictinaries wrapped with list
    """
    sales_data = mongodb_client.sample_supplies.sales
    
    sales_data_list = []
    for item in sales_data.find():
        sales_data_list.append(item)

    return sales_data_list
