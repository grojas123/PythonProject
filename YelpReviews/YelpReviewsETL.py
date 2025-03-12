from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.9'

def main(input_file, output):
    spark = SparkSession.builder \
        .appName("Yelp Reviews ETL") \
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

    yelp_schema = StructType([
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("stars", DoubleType(), True),
        StructField("review_count", IntegerType(), True),
        StructField("hours", StructType([
            StructField("Monday", StringType(), True),
            StructField("Tuesday", StringType(), True),
            StructField("Wednesday", StringType(), True),
            StructField("Thursday", StringType(), True),
            StructField("Friday", StringType(), True),
            StructField("Saturday", StringType(), True),
            StructField("Sunday", StringType(), True)
        ]))
    ])
    reviews = spark.read.json(f"hdfs://localhost:9000{input_file}",schema=yelp_schema)
    
    reviews.createOrReplaceTempView("yelp_businesses")
    # Time Dimension
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW dim_time AS
        SELECT 
          DISTINCT
          explode(array('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')) as day_of_week
        FROM yelp_businesses
    """)
    #Location Dimension
    spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW dim_location AS
    SELECT 
      DISTINCT city,
      dense_rank() OVER (ORDER BY city) as location_id
    FROM yelp_businesses
    """)
    #Rating Dimension
    spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW dim_rating AS
    SELECT 
      DISTINCT stars as rating_value,
      dense_rank() OVER (ORDER BY stars) as rating_id
    FROM yelp_businesses
    """)
    #Create the fact table
    spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW fact_business_reviews AS
    SELECT 
      b.name as business_name,
      l.location_id,
      r.rating_id,
      b.review_count,
      t.day_of_week,
      CASE 
        WHEN t.day_of_week = 'Monday' THEN hours.Monday
        WHEN t.day_of_week = 'Tuesday' THEN hours.Tuesday
        WHEN t.day_of_week = 'Wednesday' THEN hours.Wednesday
        WHEN t.day_of_week = 'Thursday' THEN hours.Thursday
        WHEN t.day_of_week = 'Friday' THEN hours.Friday
        WHEN t.day_of_week = 'Saturday' THEN hours.Saturday
        WHEN t.day_of_week = 'Sunday' THEN hours.Sunday
      END as business_hours
    FROM yelp_businesses b
    JOIN dim_location l ON b.city = l.city
    JOIN dim_rating r ON b.stars = r.rating_value
    CROSS JOIN dim_time t
    WHERE CASE 
        WHEN t.day_of_week = 'Monday' THEN hours.Monday
        WHEN t.day_of_week = 'Tuesday' THEN hours.Tuesday
        WHEN t.day_of_week = 'Wednesday' THEN hours.Wednesday
        WHEN t.day_of_week = 'Thursday' THEN hours.Thursday
        WHEN t.day_of_week = 'Friday' THEN hours.Friday
        WHEN t.day_of_week = 'Saturday' THEN hours.Saturday
        WHEN t.day_of_week = 'Sunday' THEN hours.Sunday
      END IS NOT NULL
    """)

    spark.sql("CREATE DATABASE IF NOT EXISTS yelp_analytics")

    # Save dimension tables
# Create your database first
    spark.sql("CREATE DATABASE IF NOT EXISTS yelp_analytics")

    # Save dimension tables as Parquet files in HDFS
    spark.table("dim_time").write.mode("overwrite").parquet("hdfs:///user/vmuser/yelp_analytics/dim_time")
    spark.table("dim_location").write.mode("overwrite").parquet("hdfs:///user/vmuser/yelp_analytics/dim_location")
    spark.table("dim_rating").write.mode("overwrite").parquet("hdfs:///user/vmuser/yelp_analytics/dim_rating")
    spark.table("fact_business_reviews").write.mode("overwrite").parquet("hdfs:///user/vmuser/yelp_analytics/fact_business_reviews")
  

if __name__ == "__main__":
    import sys

    if len(sys.argv) == 3:
        input_file = sys.argv[1]
        output = sys.argv[2]
        main(input_file, output)
    else:
        print("Expected: input output")