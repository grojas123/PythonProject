from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.9'
# Initialize Spark
spark = SparkSession \
    .builder \
    .appName("DataFrame Example") \
    .config("spark.pyspark.python", "/usr/bin/python3.9") \
    .config("spark.pyspark.driver.python", "/usr/bin/python3.9") \
    .master("spark://vmuser-VirtualBox:7077") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.cores", "2") \
    .config("spark.executor.memory", "4g") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "30s") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Sample data - ensure data is properly formatted as a list of tuples
# From a list of data
data = [(1, "John"), (2, "Jane")]
df = spark.createDataFrame(data, ["id", "name"])
df.show()

# Clean up Spark session
spark.stop()