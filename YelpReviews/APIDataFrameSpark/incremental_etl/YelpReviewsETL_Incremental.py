from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import re
from datetime import datetime
import glob
import subprocess

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.9'


def main(input_path, output_base):
    spark = SparkSession.builder \
        .appName("Yelp Reviews Incremental ETL") \
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

    # Define schema
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

    # Check for existing processed dates
    checkpoint_file = f"hdfs://localhost:9000{output_base}processed_dates.txt"
    processed_dates = []

    # Try to read the checkpoint file if it exists
    try:
        processed_dates_df = spark.read.text(checkpoint_file)
        processed_dates = [row.value for row in processed_dates_df.collect()]
        print(f"Found {len(processed_dates)} previously processed dates")
    except:
        print("No previous checkpoint file found. Starting fresh.")

    # Use Spark's FileSystem API to list files
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    # List all files in the directory
    path = spark._jvm.org.apache.hadoop.fs.Path(input_path)
    if fs.exists(path):
        file_statuses = fs.listStatus(path)
        input_files = []

        # Filter for JSON files and extract paths
        for file_status in file_statuses:
            file_path = str(file_status.getPath())
            if file_path.endswith('.json'):
                input_files.append(file_path)

        print(f"Found {len(input_files)} JSON files")
        # Process your files here...
    else:
        print(f"Path {input_path} does not exist in HDFS")

    # Filter only the JSON files with the correct naming pattern
    pattern = re.compile(r'yelp_(\d{4}-\d{2}-\d{2})\.json$')
    files_to_process = []

    for file_path in input_files:
        file_name = os.path.basename(file_path)
        match = pattern.search(file_name)
        if match:
            date_str = match.group(1)
            if date_str not in processed_dates:
                files_to_process.append((date_str, file_path))

    # Sort files by date to process in chronological order
    files_to_process.sort()

    if not files_to_process:
        print("No new files to process")
        return

    print(f"Processing {len(files_to_process)} new files")

    # Process each file incrementally
    for date_str, file_path in files_to_process:
        print(f"Processing file for date: {date_str}, path: {file_path}")

        # Read the current file
        current_file_df = spark.read.json(file_path, schema=yelp_schema)
        current_file_df.cache()
        current_file_df.createOrReplaceTempView("yelp_businesses")

        # Time Dimension
        days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        dim_time_df = (
            spark.read.table("yelp_businesses")
            .select(F.explode(F.array([F.lit(day) for day in days_of_week])).alias("day_of_week"))
            .distinct()
        )
        dim_time_df.createOrReplaceTempView("dim_time")

        # Location dimension
        location_df = spark.sql("SELECT DISTINCT city FROM yelp_businesses")
        location_df = location_df.withColumn("dummy_partition", F.lit(1))
        window_spec = Window.partitionBy("dummy_partition").orderBy("city")

        # Check if location dimension exists and merge with it
        location_path = f"hdfs://localhost:9000{output_base}dim_location"
        try:
            # Try to read the existing dimension table
            existing_locations = spark.read.parquet(location_path)
            # Merge new locations with existing ones
            combined_locations = existing_locations.union(location_df.select("city")).distinct()
            combined_locations = combined_locations.withColumn("dummy_partition", F.lit(1))
            location_dim = combined_locations.withColumn("location_id",
                                                         F.dense_rank().over(window_spec)).drop("dummy_partition")
            print("Merged location dimension with existing data")
        except:
            # If it doesn't exist, create a new one
            location_dim = location_df.withColumn("location_id",
                                                  F.dense_rank().over(window_spec)).drop("dummy_partition")
            print("Created new location dimension")

        location_dim.createOrReplaceTempView("dim_location")

        # Rating dimension
        rating_df = spark.sql("SELECT DISTINCT stars as rating_value FROM yelp_businesses")
        rating_df = rating_df.withColumn("dummy_partition", F.lit(1))
        window_spec = Window.partitionBy("dummy_partition").orderBy("rating_value")

        # Check if rating dimension exists and merge with it
        rating_path = f"hdfs://localhost:9000{output_base}dim_rating"
        try:
            # Try to read the existing dimension table
            existing_ratings = spark.read.parquet(rating_path)
            # Merge new ratings with existing ones
            combined_ratings = existing_ratings.union(rating_df.select("rating_value")).distinct()
            combined_ratings = combined_ratings.withColumn("dummy_partition", F.lit(1))
            rating_dim = combined_ratings.withColumn("rating_id",
                                                     F.dense_rank().over(window_spec)).drop("dummy_partition")
            print("Merged rating dimension with existing data")
        except:
            # If it doesn't exist, create a new one
            rating_dim = rating_df.withColumn("rating_id",
                                              F.dense_rank().over(window_spec)).drop("dummy_partition")
            print("Created new rating dimension")

        rating_dim.createOrReplaceTempView("dim_rating")

        # Cache dimension tables for better performance
        spark.table("dim_location").cache()
        spark.table("dim_rating").cache()

        # Create the fact table
        businesses_df = spark.table("yelp_businesses")
        dim_location_df = spark.table("dim_location")
        dim_rating_df = spark.table("dim_rating")
        dim_time_df = spark.table("dim_time")

        # Define the business hours case statement as a function for reuse
        def business_hours_expr(day_of_week_col):
            return F.when(day_of_week_col == 'Monday', businesses_df['hours.Monday']) \
                .when(day_of_week_col == 'Tuesday', businesses_df['hours.Tuesday']) \
                .when(day_of_week_col == 'Wednesday', businesses_df['hours.Wednesday']) \
                .when(day_of_week_col == 'Thursday', businesses_df['hours.Thursday']) \
                .when(day_of_week_col == 'Friday', businesses_df['hours.Friday']) \
                .when(day_of_week_col == 'Saturday', businesses_df['hours.Saturday']) \
                .when(day_of_week_col == 'Sunday', businesses_df['hours.Sunday'])

        # Create fact table with a date column to track the date of each record
        fact_business_reviews_df = (
            businesses_df
            .join(dim_location_df, businesses_df.city == dim_location_df.city)
            .join(dim_rating_df, businesses_df.stars == dim_rating_df.rating_value)
            .crossJoin(dim_time_df)
            .withColumn("business_hours", business_hours_expr(dim_time_df.day_of_week))
            .filter(F.col("business_hours").isNotNull())
            .select(
                businesses_df["name"].alias("business_name"),
                dim_location_df["location_id"],
                dim_rating_df["rating_id"],
                businesses_df["review_count"],
                dim_time_df["day_of_week"],
                F.col("business_hours"),
                F.lit(date_str).alias("data_date")
            )
        )

        fact_business_reviews_df.createOrReplaceTempView("fact_business_reviews")

        # Save dimension tables (overwrite since we've merged them)
        spark.table("dim_time").write.mode("overwrite").parquet(f"hdfs://localhost:9000{output_base}dim_time")
        spark.table("dim_location").write.mode("overwrite").parquet(f"hdfs://localhost:9000{output_base}dim_location")
        spark.table("dim_rating").write.mode("overwrite").parquet(f"hdfs://localhost:9000{output_base}dim_rating")

        # For fact table, check if it exists and append or create as needed
        fact_path = f"hdfs://localhost:9000{output_base}fact_business_reviews"

        # Check if fact table exists by trying to read it
        fact_table_exists = False
        try:
            spark.read.parquet(fact_path)
            fact_table_exists = True
        except:
            fact_table_exists = False

        if fact_table_exists:
            # Append new data to existing fact table
            spark.table("fact_business_reviews").write.mode("append").partitionBy("data_date").parquet(fact_path)
            print(f"Appended new data for {date_str} to fact table")
        else:
            # First run, create the fact table
            spark.table("fact_business_reviews").write.mode("overwrite").partitionBy("data_date").parquet(fact_path)
            print(f"Created new fact table with data for {date_str}")

        # Add the processed date to our list
        processed_dates.append(date_str)

        # Clean up
        current_file_df.unpersist()

    # Update the checkpoint file with all processed dates
    processed_dates_df = spark.createDataFrame([(date,) for date in processed_dates], ["value"])
    processed_dates_df.write.mode("overwrite").text(checkpoint_file)

    print(f"Successfully processed {len(files_to_process)} files. Total processed files: {len(processed_dates)}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 3:
        input_path = sys.argv[1]
        output_base = sys.argv[2]
        main(input_path, output_base)
    else:
        print("Expected: input_path output_base_path")