from pyspark.sql import SparkSession

import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.9'

def main():
    spark = SparkSession.builder \
        .appName("Yelp Reviews Analysis") \
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

 
# Read dimension tables
    dim_time_df = spark.read.parquet("hdfs:///user/vmuser/yelp_analytics/dim_time")
    dim_location_df = spark.read.parquet("hdfs:///user/vmuser/yelp_analytics/dim_location")
    dim_rating_df = spark.read.parquet("hdfs:///user/vmuser/yelp_analytics/dim_rating")
    fact_business_reviews_df = spark.read.parquet("hdfs:///user/vmuser/yelp_analytics/fact_business_reviews")
# Now you can query using the database name
    dim_time_df.createOrReplaceTempView("dim_time")
    dim_location_df.createOrReplaceTempView("dim_location")
    dim_rating_df.createOrReplaceTempView("dim_rating")
    fact_business_reviews_df.createOrReplaceTempView("fact_business_reviews")

    dim_time_df.show(10)
    dim_location_df.show(10)
    dim_rating_df.show(10)
    fact_business_reviews_df.show(10)
    
if __name__ == "__main__":
        main()