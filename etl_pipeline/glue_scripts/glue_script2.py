
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *

# Initialize Glue and Spark contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read in data from data source
source = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "path": "s3://<bucket-name>/<path-to-data-file>"
    },
    format="csv"
)

# Perform transformations on the data using Spark functions
processed_data = source.toDF()

# Save the processed data as parquet format to S3
processed_data.write.format("parquet").mode("overwrite").save("s3://<bucket-name>/<path-to-processed-data>")

# Convert Spark DataFrame back to DynamicFrame
results = DynamicFrame.fromDF(processed_data, glueContext, "results")

# Write results to database or data warehouse
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=results,
    catalog_connection="my_jdbc_connection",
    connection_options={
        "database": "my_database",
        "user": "my_username",
        "password": "my_password",
        "host": "my_database_host",
        "port": "my_database_port",
    },
    redshift_tmp_dir="s3://<bucket-name>/<path-to-temporary-folder>",
    transformation_ctx="results"
)
