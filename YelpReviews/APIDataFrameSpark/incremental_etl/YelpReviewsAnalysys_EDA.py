from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from ydata_profiling import ProfileReport

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
    base_hdfs_path = "hdfs:///user/vmuser/yelp_analytics_incremental/"
    # Read dimension tables
    dim_time_df = spark.read.parquet(f"{base_hdfs_path}dim_time")
    dim_location_df = spark.read.parquet(f"{base_hdfs_path}/dim_location")
    dim_rating_df = spark.read.parquet(f"{base_hdfs_path}/dim_rating")
    fact_business_reviews_df = spark.read.parquet(f"{base_hdfs_path}/fact_business_reviews")
    fact_business_reviews_df = fact_business_reviews_df.withColumn("data_date",
                                                                   F.to_date(F.col("data_date"), "yyyy-MM-dd"))
    dim_time_report = ProfileReport(dim_time_df, title="Dim Time Profile", minimal=True)
    dim_time_report.to_file("dim_time_profile.html")
    dim_location_report = ProfileReport(dim_location_df, title="Dim Location Profile", minimal=True)
    dim_location_report.to_file("dim_location_profile.html")
    dim_rating_report = ProfileReport(dim_rating_df , title="Dim Location Profile", minimal=True)
    dim_rating_report.to_file("dim_rating_profile.html")

    #_______________________________________________________________________


    # Using window function with 1 month duration
    # Extract year and month components directly
    from pyspark.sql.functions import month, year, count
    # Extract month and year from your date column
    monthly_counts = fact_business_reviews_df.withColumn("month", month("data_date")) \
        .withColumn("year", year("data_date")) \
        .groupBy("year", "month") \
        .count() \
        .orderBy("year", "month")

    # Show the results
    monthly_counts.show()
    # Drop a single column because
    fact_business_reviews_report_df = fact_business_reviews_df.drop("data_date")
    fact_business_reviews_report = ProfileReport(fact_business_reviews_report_df , title="Fact Business Reviews Profile")
    fact_business_reviews_report.to_file("fact_business_reviews_profile.html")

if __name__ == "__main__":
    main()