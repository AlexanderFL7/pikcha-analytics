from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.master("local[*]").appName("check-python").getOrCreate()
print(f"Driver Python version: {sys.version}")

def worker_version(_):
    import sys
    return sys.version

versions = spark.sparkContext.parallelize(range(1)).map(worker_version).collect()
print(f"Worker Python version: {versions[0]}")

spark.stop()
