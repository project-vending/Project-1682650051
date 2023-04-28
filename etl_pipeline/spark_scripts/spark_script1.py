python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Set up Spark configurations
conf = SparkConf().setAppName("ETL Pipeline Job 1")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("ETL Pipeline Job 1").getOrCreate()

# Load data from input sources
data_source1 = spark.read.csv("etl_pipeline/data/data_source1.csv", header=True)
data_source2 = spark.read.json("etl_pipeline/data/data_source2.json")

# Transform data using Spark SQL
data_source1.createOrReplaceTempView("source1_table")
data_source2.createOrReplaceTempView("source2_table")
transformed_data = spark.sql("""
    SELECT source1.*, source2.some_field
    FROM source1_table as source1
    JOIN source2_table as source2
    ON source1.key = source2.key 
""")

# Write transformed data to output location
transformed_data.write.parquet("etl_pipeline/data/transformed_data.parquet")
