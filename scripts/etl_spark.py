import os
from pyspark.sql import SparkSession

# Fix for Windows
os.environ["HADOOP_HOME"] = "C:\\Tools\\Hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + "\\bin;" + os.environ["PATH"]

spark = SparkSession.builder \
    .appName("ETL Project") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.sql.warehouse.dir", "file:///C:/temp") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

df = spark.read.csv("data/input.csv", header=True, inferSchema=True)

df.show()

df_filtered = df.filter(df.age > 18)

df_filtered.show()

df_filtered.coalesce(1).write.mode("overwrite").option("header", True).csv("output/spark_output")

print("ETL Completed using PySpark")