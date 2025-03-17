from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import dense_rank, lit,array,explode,col,when
from pyspark.sql.window import Window

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
    reviews.cache()
    reviews.createOrReplaceTempView("yelp_businesses")
    # Time Dimension


    # Create an array of days of the week
    days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    # Create a DataFrame for yelp_businesses (assuming this already exists)
    # If you already have a DataFrame called yelp_businesses, use that instead
    # yelp_businesses_df = spark.table("yelp_businesses")

    # Use explode to create the time dimension
    dim_time_df = (
        spark.read.table("yelp_businesses")
        .select(explode(array([lit(day) for day in days_of_week])).alias("day_of_week"))
        .distinct()
    )

    # Register as a temporary view if needed
    dim_time_df.createOrReplaceTempView("dim_time")
  # Location dimension with dummy partition
    location_df = spark.sql("SELECT DISTINCT city FROM yelp_businesses")
    location_df = location_df.withColumn("dummy_partition", lit(1))
    window_spec = Window.partitionBy("dummy_partition").orderBy("city")
    location_dim = location_df.withColumn("location_id", dense_rank().over(window_spec)).drop("dummy_partition")
    location_dim.createOrReplaceTempView("dim_location")

    # Rating dimension with dummy partition
    rating_df = spark.sql("SELECT DISTINCT stars as rating_value FROM yelp_businesses")
    rating_df = rating_df.withColumn("dummy_partition", lit(1))
    window_spec = Window.partitionBy("dummy_partition").orderBy("rating_value")
    rating_dim = rating_df.withColumn("rating_id", dense_rank().over(window_spec)).drop("dummy_partition")
    rating_dim.createOrReplaceTempView("dim_rating")
    
    spark.table("dim_location").cache()
    spark.table("dim_rating").cache()
    # Read required tables into DataFrames
    # Assuming these tables or views are already created
    businesses_df = spark.table("yelp_businesses")
    dim_location_df = spark.table("dim_location")
    dim_rating_df = spark.table("dim_rating")
    dim_time_df = spark.table("dim_time")
    #Create the fact table
    # Define the business hours case statement as a function for reuse
    def business_hours_expr(day_of_week_col):
        return when(day_of_week_col == 'Monday', businesses_df['hours.Monday']) \
            .when(day_of_week_col == 'Tuesday', businesses_df['hours.Tuesday']) \
            .when(day_of_week_col == 'Wednesday', businesses_df['hours.Wednesday']) \
            .when(day_of_week_col == 'Thursday', businesses_df['hours.Thursday']) \
            .when(day_of_week_col == 'Friday', businesses_df['hours.Friday']) \
            .when(day_of_week_col == 'Saturday', businesses_df['hours.Saturday']) \
            .when(day_of_week_col == 'Sunday', businesses_df['hours.Sunday'])

    # Create the fact table
    fact_business_reviews_df = (
        businesses_df
        .join(dim_location_df, businesses_df.city == dim_location_df.city)
        .join(dim_rating_df, businesses_df.stars == dim_rating_df.rating_value)
        .crossJoin(dim_time_df)
        .withColumn("business_hours", business_hours_expr(dim_time_df.day_of_week))
        .filter(col("business_hours").isNotNull())
        .select(
            businesses_df["name"].alias("business_name"),
            dim_location_df["location_id"],
            dim_rating_df["rating_id"],
            businesses_df["review_count"],
            dim_time_df["day_of_week"],
            col("business_hours")
        )
    )

    # Register as a temporary view
    fact_business_reviews_df.createOrReplaceTempView("fact_business_reviews")

    # Save dimension tables and fact table as Parquet files in HDFS
    spark.table("dim_time").write.mode("overwrite").parquet(f"{output}dim_time")
    spark.table("dim_location").write.mode("overwrite").parquet(f"{output}dim_location")
    spark.table("dim_rating").write.mode("overwrite").parquet(f"{output}dim_rating")
    spark.table("fact_business_reviews").write.mode("overwrite").parquet(f"{output}fact_business_reviews")
  

if __name__ == "__main__":
    import sys

    if len(sys.argv) == 3:
        input_file = sys.argv[1]
        output = sys.argv[2]
        main(input_file, output)
    else:
        print("Expected: input output")