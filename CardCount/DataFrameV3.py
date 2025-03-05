from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark
spark = SparkSession \
    .builder \
    .appName("DataFrame Example") \
    .master("local[*]") \
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