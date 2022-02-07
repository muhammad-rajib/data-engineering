# import python modules
from datetime import datetime

# import pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f

# import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

# parameters
glue_db = "virtual"
glue_tbl = "input"
s3_write_path = "s3://testglue8521/output"

########################
### EXTRACT (READ DATA)
########################
dynamic_frame_read = glue_context. \
                    create_dynamic_frame. \
                    from_catalog(database = glue_db, table_name = glue_tbl)

# convert dynamic frame to data frame
data_frame = dynamic_frame_read.toDF()

############################
### TRANSFORM (MODIFY DATA)
############################
data_frame_aggregated = data_frame.groupBy("VendorID").agg(
    f.mean(f.col("total_amount")).alias('avg_total_amount'),
    f.mean(f.col("tip_amount")).alias('avg_tip_time'),
)

data_frame_aggregated = data_frame_aggregated.orderBy(f.desc("avg_total_amount"))


########################
### LOAD (WRITE DATA)
########################

# create just 1 partitions, because there is so little data
data_frame_aggregated = data_frame_aggregated.repartition(1)

# convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggregated, glue_context, "dynamic_frame_write")

# write data back to s3
glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = 's3',
    connection_options = {
        "path": s3_write_path,
    },
    format = 'csv'
)