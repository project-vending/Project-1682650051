python
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("SparkScript2").getOrCreate()

# Load data from a CSV file
df = spark.read.csv("data/data_source1.csv", header=True)

# Print schema and first few rows of data
df.printSchema()
df.show(5)

# Perform data transformations
df2 = df.selectExpr("col1 + col2 as sum", "col3 * col4 as product")

# Write transformed data to a new CSV file
df2.write.mode("overwrite").csv("data/transformed_data.csv", header=True)

# Stop spark session
spark.stop()
