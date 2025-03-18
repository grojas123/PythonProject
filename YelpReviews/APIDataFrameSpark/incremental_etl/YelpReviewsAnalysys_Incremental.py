from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pyspark.sql.window import Window

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
    # Now you can query using the database name
    dim_time_df.createOrReplaceTempView("dim_time")
    dim_location_df.createOrReplaceTempView("dim_location")
    dim_rating_df.createOrReplaceTempView("dim_rating")
    fact_business_reviews_df.createOrReplaceTempView("fact_business_reviews")

    # Analyze the distribution of business hours by day of week

    hours_distribution_df = (
        fact_business_reviews_df
        .filter(F.col("business_hours").isNotNull())
        .withColumn("opening_time", F.substring(F.col("business_hours"), 1, 5))
        .groupBy("day_of_week", "opening_time")
        .agg(
            F.countDistinct("business_name").alias("total_businesses"),
            F.count("*").alias("businesses_with_opening_time")
        )
        .orderBy("day_of_week", F.col("businesses_with_opening_time").desc())
    )

    # Show the results

    hours_distribution_df.show(20, False)

    # Find businesses that are open late (after 9 PM)
    # Define a function to extract closing hour from business_hours
    def extract_closing_hour(business_hours_col):
        return F.split(
            F.split(business_hours_col, '-').getItem(1),
            ':'
        ).getItem(0).cast('int')

    # Create the late_night_businesses DataFrame
    late_night_businesses_df = (
        fact_business_reviews_df
        .filter(F.col("business_hours").isNotNull())
        .withColumn("closing_hour", extract_closing_hour(F.col("business_hours")))
        .join(dim_location_df, fact_business_reviews_df.location_id == dim_location_df.location_id)
        .join(dim_rating_df, fact_business_reviews_df.rating_id == dim_rating_df.rating_id)
        .filter(F.col("closing_hour") >= 21)
        .select(
            dim_location_df["city"],
            fact_business_reviews_df["business_name"],
            dim_rating_df["rating_value"],
            fact_business_reviews_df["day_of_week"],
            fact_business_reviews_df["business_hours"],
            F.col("closing_hour")
        )
        .orderBy(F.col("closing_hour").desc(), F.col("rating_value").desc())
    )

    # Show the results
    late_night_businesses_df.show(20, False)

    # Analyze which days businesses are most commonly open
    # Step 1: Create the business_days DataFrame
    business_days_df = (
        fact_business_reviews_df
        .groupBy("business_name")
        .agg(
            F.collect_list("day_of_week").alias("days_open"),
            F.countDistinct("day_of_week").alias("num_days_open")
        )
    )

    # Step 2: Calculate the day availability
    total_businesses = fact_business_reviews_df.select(F.countDistinct("business_name")).first()[0]

    day_availability_df = (
        business_days_df
        .groupBy("num_days_open")
        .agg(
            F.count("*").alias("num_businesses"),
            (F.round(F.count("*") * 100.0 / total_businesses, 2)).alias("percentage")
        )
        .orderBy(F.col("num_days_open").desc())
    )

    # Show the results
    day_availability_df.show(20)

    # Analyze if higher ratings correlate with more reviews
    # Calculate the rating-review correlation
    rating_review_correlation_df = (
        fact_business_reviews_df
        .join(dim_rating_df, fact_business_reviews_df.rating_id == dim_rating_df.rating_id)
        .filter(fact_business_reviews_df.day_of_week == 'Monday')  # Using one day to avoid duplicates
        .groupBy(dim_rating_df.rating_value)
        .agg(
            F.countDistinct(fact_business_reviews_df.business_name).alias("num_businesses"),
            F.avg(fact_business_reviews_df.review_count).alias("avg_review_count"),
            F.min(fact_business_reviews_df.review_count).alias("min_review_count"),
            F.max(fact_business_reviews_df.review_count).alias("max_review_count")
        )
        .orderBy(F.col("rating_value").desc())
    )

    # Show the results
    rating_review_correlation_df.show()

    # Find businesses with consistent hours all week vs. variable hours
    # First create a DataFrame with business_hours_patterns
    business_hours_patterns = (
        spark.table("fact_business_reviews")
        .groupBy("business_name")
        .agg(
            F.countDistinct("business_hours").alias("unique_hour_patterns"),
            F.max("business_hours").alias("sample_hours")
        )
    )

    # Then categorize and aggregate
    hours_consistency = (
        business_hours_patterns
        .withColumn(
            "hours_pattern",
            F.when(F.col("unique_hour_patterns") == 1, "Consistent Hours")
            .when((F.col("unique_hour_patterns") >= 2) & (F.col("unique_hour_patterns") <= 3), "Slightly Variable")
            .otherwise("Highly Variable")
        )
        .groupBy("hours_pattern")
        .agg(
            F.count("*").alias("num_businesses"),
            F.round(F.avg("unique_hour_patterns"), 2).alias("avg_unique_patterns")
        )
        .orderBy("avg_unique_patterns")
    )

    hours_consistency.show()

    # Create a comprehensive city business profile
    # First create city_stats DataFrame
    city_stats = (
        spark.table("fact_business_reviews").alias("f")
        .join(
            spark.table("dim_location").alias("l"),
            F.col("f.location_id") == F.col("l.location_id")
        )
        .join(
            spark.table("dim_rating").alias("r"),
            F.col("f.rating_id") == F.col("r.rating_id")
        )
        .filter(F.col("day_of_week") == "Monday")  # Using one day to avoid duplicates
        .groupBy("l.city")
        .agg(
            F.countDistinct("f.business_name").alias("num_businesses"),
            F.avg("r.rating_value").alias("avg_rating"),
            F.sum("f.review_count").alias("total_reviews"),
            F.expr("percentile(r.rating_value, 0.5)").alias("median_rating")
        )
    )

    # Define window specifications for rankings
    window_by_rating = Window.orderBy(F.desc("avg_rating"))
    window_by_size = Window.orderBy(F.desc("num_businesses"))
    window_by_popularity = Window.orderBy(F.desc("total_reviews"))

    # Create final city_profile DataFrame with rankings
    city_profile = (
        city_stats
        .withColumn("rating_rank", F.rank().over(window_by_rating))
        .withColumn("size_rank", F.rank().over(window_by_size))
        .withColumn("popularity_rank", F.rank().over(window_by_popularity))
        .orderBy(F.desc("avg_rating"))
    )

    city_profile.show(20, False)

    # Find businesses that open early (before 8 AM)
    early_openers = (
        spark.table("fact_business_reviews").alias("f")
        .join(
            spark.table("dim_location").alias("l"),
            F.col("f.location_id") == F.col("l.location_id")
        )
        .join(
            spark.table("dim_rating").alias("r"),
            F.col("f.rating_id") == F.col("r.rating_id")
        )
        .withColumn(
            "opening_hour",
            F.split(F.split(F.col("f.business_hours"), "-")[0], ":")[0].cast("int")
        )
        .filter(F.col("opening_hour") < 8)
        .select(
            "day_of_week",
            F.col("l.city"),
            F.col("f.business_name"),
            F.col("r.rating_value"),
            F.col("f.business_hours"),
            "opening_hour"
        )
        .orderBy("day_of_week", "opening_hour", F.desc("r.rating_value"))
    )

    early_openers.show(20, False)

    # Calculate and analyze business operating duration
    from pyspark.sql.functions import split, col, when, expr

    hours_duration = (
        spark.table("fact_business_reviews")
        .filter(col("business_hours").isNotNull())
        .withColumn("opening_time", split(split("business_hours", "-")[0], ":"))
        .withColumn("closing_time", split(split("business_hours", "-")[1], ":"))
        .withColumn("opening_hour", col("opening_time")[0].cast("integer"))
        .withColumn("closing_hour", col("closing_time")[0].cast("integer"))
        .withColumn("hours_open",
                    when(col("closing_hour") < col("opening_hour"),
                         col("closing_hour") + 24 - col("opening_hour"))
                    .otherwise(col("closing_hour") - col("opening_hour"))
                    )
        .select(
            "day_of_week",
            "business_name",
            "business_hours",
            "opening_hour",
            "closing_hour",
            "hours_open"
        )
    )

    # Create temporary view if needed
    hours_duration.createOrReplaceTempView("hours_duration")

    from pyspark.sql.functions import avg, min, max, expr, col

    duration_summary = (
        hours_duration
        .groupBy("day_of_week")
        .agg(
            avg("hours_open").alias("avg_hours_open"),
            min("hours_open").alias("min_hours_open"),
            max("hours_open").alias("max_hours_open"),
            expr("percentile(hours_open, 0.5)").alias("median_hours_open")
        )
        .orderBy("day_of_week")
    )

    duration_summary.show()

    # Identify the highest-rated businesses in each city with substantial review counts

    window_spec = Window.partitionBy("city").orderBy(
        F.col("rating_value").desc(),
        F.col("review_count").desc()
    )
    # Building the equivalent transformation using DataFrame API
    top_rated_by_city = (
        spark.table("fact_business_reviews")
        .join(
            spark.table("dim_location"),
            on="location_id"
        )
        .join(
            spark.table("dim_rating"),
            on="rating_id"
        )
        .filter(F.col("day_of_week") == "Monday")
        .select(
            F.col("city"),
            F.col("business_name"),
            F.col("rating_value"),
            F.col("review_count"),
            F.row_number().over(window_spec).alias("rank")
        )
        .filter(F.col("rank") <= 5)
        .orderBy("city", F.col("rating_value").desc(), F.col("review_count").desc())
        .select("city", "business_name", "rating_value", "review_count")
    )

    top_rated_by_city.show(50, False)


if __name__ == "__main__":
    main()