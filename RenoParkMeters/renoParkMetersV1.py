import os
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
from handyspark import *
from pyspark.sql.functions import col, count, when
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
    schema = StructType() \
        .add("STREET", StringType()) \
        .add("METER_ADDRESS", StringType()) \
        .add("ZONE",StringType()) \
        .add("AREA", StringType()) \
        .add("HOURS", IntegerType()) \
        .add("STREET_ABBR", StringType()) \
        .add("CODE", StringType()) \
        .add("ADDRESS", StringType()) \
        .add("GEOCODE_ADDRESS", StringType()) \
        .add("LAT", FloatType()) \
        .add("LON", FloatType()) \
        .add("NUM_CASH_2014H1", IntegerType()) \
        .add("DOLLAR_CASH_2014H1", FloatType()) \
        .add("NUM_CREDIT_2014H1", IntegerType()) \
        .add("DOLLAR_CREDIT_2014H1", FloatType()) \
        .add("NUM_SMART_CARD_2014H1", IntegerType()) \
        .add("DOLLAR_SMART_CARD_2014H1", FloatType())
    # Read CSV file and create a DataFrame
    parking_tkt = spark.read \
            .option("delimiter", ",") \
            .option("header", "true") \
            .schema(schema) \
            .csv(f"hdfs://localhost:9000{input_file}")

    parking_tkt.show(10)
    parking_tkt.describe().show()
    parking_tkt.printSchema()
    # Count nulls for all columns
    null_counts = parking_tkt.select([count(when(col(c).isNull(), c)).alias(c) for c in parking_tkt.columns])
    null_counts.show()
    hdf = parking_tkt.toHandy()
    hdf.show(10)
    fig, ax = plt.subplots(figsize=(10, 7))
    hdf.cols['AREA'].hist(ax=ax)
    plt.show()
if __name__ == "__main__":
    import sys

    if len(sys.argv) == 3:
        input_file = sys.argv[1]
        output = sys.argv[2]
        main(input_file, output)
    else:
        print("Expected: input output")