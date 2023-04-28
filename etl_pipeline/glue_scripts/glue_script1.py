python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])

# Initialize the Spark context and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)

# Extract data from input path
input_data = glueContext.create_dynamic_frame_from_options(
    "s3",
    {
        "paths": [args['input_path']],
        "format": "csv",
        "separator": ","
    }
)

# Transform the data
transformed_data = input_data.toDF().groupBy('category').agg({'price':'mean'})

# Convert transformed data back to DynamicFrame
transformed_frame = DynamicFrame.fromDF(
    transformed_data,
    glueContext,
    "transformed_frame"
)

# Write transformed data to output path
glueContext.write_dynamic_frame.from_options(
    frame = transformed_frame,
    connection_type = "s3",
    connection_options = {
        "path": args['output_path'],
        "partitionKeys": ["category"]
    },
    format = "csv"
)
