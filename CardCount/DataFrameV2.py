from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Initialize Spark
spark = SparkSession \
    .builder \
    .appName("DataFrame Example") \
    .master("spark://vmuser-VirtualBox:7077") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.cores", "2") \
    .config("spark.executor.memory", "4g") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "30s") \
    .getOrCreate()

# Sample data
data = [
    ("John",25,5000.0),
    ("Alice",30,6000.0),
    ("Bob",35,7000.0)
]
# Create DataFrame with schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

# Create DataFrame directly
df = spark.createDataFrame(data=data, schema=schema)
df.show()
# Filter employees with age > 28 using DataFrame
#filtered_df = df.filter("age > 28")

# Calculate average salary using DataFrame
#avg_salary_df = df.select("salary").agg(avg("salary"))

#print("DataFrame Average Salary:", avg_salary_df)

# Show results
print("DataFrame Output:")
#filtered_df.show()

# Clean up Spark session
spark.stop()