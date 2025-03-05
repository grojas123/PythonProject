from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
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
    .getOrCreate()

# Sample data - ensure data is properly formatted as a list of tuples
data = [
    ("John", 25, 5000.0),
    ("Alice", 30, 6000.0),
    ("Bob", 35, 7000.0)
]

# Create schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.show()

# Clean up Spark session
spark.stop()