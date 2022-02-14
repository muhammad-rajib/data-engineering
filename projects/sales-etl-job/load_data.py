"""
Load Data into Redshift (AWS Data Warehouse).
"""
# import required libraries
import boto3
import psycopg2
from utils import s3_env_dict, redshift_env_dict
from io import StringIO 


def load_data_into_s3_bucket(sales_df):

    # write dataframe as a csv to S3
    filename = 'salesrecords.csv'
    bucket_name = 'redshift-sales-data'
    csv_buffer = StringIO()
    sales_df.to_csv(csv_buffer)

    client = boto3.client('s3')

    response = client.put_object(
        ACL='private',
        Body=csv_buffer.getvalue(),
        Bucket = bucket_name,
        Key = filename
    )


def load_data_into_redshift(sales_df):

    load_data_into_s3_bucket(sales_df)

    env = redshift_env_dict()

    conn = psycopg2.connect(
    host='redshift-sales-cluster.cmypa1emn3fe.us-east-1.redshift.amazonaws.com',
    user='awsuser',
    port=5439,
    password= env['password'],
    dbname='dev'
    )

    cur = conn.cursor()

    cur.execute("begin;")
    conn.commit()

    try:
        cur.execute(f"""DROP TABLE IF EXISTS salesinfos;""")
    except psycopg2.Error as e:
        print("Error: table drop time.")
        print(e)

    try:
        cur.execute(
            """
            create table salesinfos (
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

    sql = f'''COPY salesdata FROM 's3://redshift-sales-data/salesrecords.csv'
        credentials 'aws_access_key_id='AKIAU77AZSSHIBCAXGPP;aws_secret_access_key=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'
        CSV;
        '''
    
    sql2 = f'''
        COPY salesinfos FROM 's3://redshift-sales-data/salesrecords.csv'
        credentials 'aws_iam_role=arn:aws:iam::343531951246:role/redshift-s3-access'
        DATEFORMAT 'yyyy-mm-dd'
        IGNOREHEADER 1
        CSV
        '''

    try:
        cur.execute(sql2)
    except psycopg2.Error as e:
        print("Error: In copy time")
        print(e)
    
    conn.commit()

