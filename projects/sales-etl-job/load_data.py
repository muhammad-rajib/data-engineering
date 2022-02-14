"""
Load Data into Redshift (AWS Data Warehouse).
"""
# import required libraries
import boto3
import psycopg2
from io import StringIO 


def load_data_into_s3_bucket(sales_df, s3_cred):
    """
    load sales into aws s3 bucket.

    :param --> sales_df: pandas dataframe of sales data
    :required informations are loaded from .env file by utils function.
    """

    filename    = s3_cred['file_name']
    bucket_name = s3_cred['bucket_name']

    csv_buffer  = StringIO()
    sales_df.to_csv(csv_buffer)

    client = boto3.client('s3')

    client.put_object(
        ACL     = 'private',
        Body    = csv_buffer.getvalue(),
        Bucket  = bucket_name,
        Key     = filename
    )


def load_data_s3_to_redshift(s3_cred, redshift_cred):
    """
    load sales data from aws s3 bucket to redshift.

    :param --> redshift_cred: redshift credentials wrapped with dictionary
    :after successfull execution, data will be available in redshift data-warehouse
    """
    conn = psycopg2.connect(
                host        = redshift_cred['endpoint'],
                user        = redshift_cred['user'],
                port        = redshift_cred['port'],
                password    = redshift_cred['password'],
                dbname      = redshift_cred['db_name']
            )

    cur = conn.cursor()

    cur.execute("begin;")
    conn.commit()

    try:
        cur.execute(f"""DROP TABLE IF EXISTS {redshift_cred['table_name']};""")
    except psycopg2.Error as e:
        print("Error: table drop time.")
        print(e)

    try:
        cur.execute(
            f"""
            create table {redshift_cred['table_name']} (
            id INTEGER,
            purchase_date VARCHAR(20),
            item VARCHAR(30),
            price REAL,
            quantity INTEGER,
            total_price REAL,
            store_location VARCHAR(30),
            purchase_method VARCHAR(30),
            gender VARCHAR(15),
            age INTEGER,
            satisfaction INTEGER
            );"""
        )
    except psycopg2.Error as e:
        print("Error: Issue creating table")
        print(e)
    
    sql = f'''
        COPY {redshift_cred['table_name']} FROM 's3://{s3_cred['bucket_name']}/{s3_cred['file_name']}'
        credentials 'aws_iam_role={s3_cred['aws_iam_role']}'
        DATEFORMAT 'yyyy-mm-dd'
        IGNOREHEADER 1
        CSV
        '''

    try:
        cur.execute(sql)
    except psycopg2.Error as e:
        print("Error: In copy time")
        print(e)
    
    conn.commit()
