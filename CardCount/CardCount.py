from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.9'

def main(input_file, output):
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Card Count") \
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
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    # Read TSV file and create a DataFrame
    cards = spark.read \
        .option("delimiter", "\t") \
        .option("header", "true") \
        .csv(f"hdfs://localhost:9000{input_file}")

    # Group by suit and sum the numbers
    suit_sums = cards \
        .groupBy("suit") \
        .agg(sum("number").alias("total")) \
        .orderBy("suit")

    # Show the results
    suit_sums.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 3:
        input_file = sys.argv[1]
        output = sys.argv[2]
        main(input_file, output)
    else:
        print("Expected: input output")