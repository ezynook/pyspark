import os
import shutil
from pyspark.sql import SparkSession

# create a SparkSession
spark = SparkSession.builder.appName("Read and Save CSV Files").getOrCreate()

# set the number of files to read at a time
batch_size = 10

# get a list of all CSV files in the directory
csv_files = os.listdir("path/to/csv/files")

# loop through the CSV files in batches of batch_size
for i in range(0, len(csv_files), batch_size):
    # get the next batch of CSV files
    batch = csv_files[i:i+batch_size]
    
    # read the batch of CSV files into a DataFrame
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load([f"path/to/csv/files/{file}" for file in batch])
    
    # save the DataFrame as a Parquet file in HDFS
    df.write.mode("append").parquet("path/to/hdfs/parquet/file")
    
    # move the batch of CSV files to another directory
    for file in batch:
        shutil.move(f"path/to/csv/files/{file}", "path/to/destination/directory")
