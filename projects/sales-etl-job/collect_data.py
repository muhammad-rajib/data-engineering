from utils import connect_with_mongodb
from utils import mongodb_env_dict2


def collect_data_into_list():
    """
    function: return sales data list.
    :Make a connection with mongodb atlas cluster
    :using connect_with_mongodb functions of utils.
    :Then extract data from cluster db and load into the list.
    """
    env_dict = mongodb_env_dict2()

    client = connect_with_mongodb()
    sales_data = client.env_dict['db_name'].env_dict['table_name']
    
    # prepare sales data list
    sales_data_list = []
    for item in sales_data.find():
        sales_data_list.append(item)

    return sales_data_list
