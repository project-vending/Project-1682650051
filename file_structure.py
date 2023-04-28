 
import os

# Define file and folder names
root_dir = "etl_pipeline"
data_dir = "data"
glue_dir = "glue_scripts"
spark_dir = "spark_scripts"

# Create root directory if it does not exist
if not os.path.exists(root_dir):
    os.mkdir(root_dir)

# Create subdirectories for data, Glue scripts, and Spark scripts
os.mkdir(os.path.join(root_dir, data_dir))
os.mkdir(os.path.join(root_dir, glue_dir))
os.mkdir(os.path.join(root_dir, spark_dir))

# Create empty files for data sources, Glue scripts, and Spark scripts
open(os.path.join(root_dir, data_dir, "data_source1.csv"), 'a').close()
open(os.path.join(root_dir, data_dir, "data_source2.json"), 'a').close()
open(os.path.join(root_dir, glue_dir, "glue_script1.py"), 'a').close()
open(os.path.join(root_dir, glue_dir, "glue_script2.py"), 'a').close()
open(os.path.join(root_dir, spark_dir, "spark_script1.py"), 'a').close()
open(os.path.join(root_dir, spark_dir, "spark_script2.py"), 'a').close()
