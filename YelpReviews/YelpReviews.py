from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.9'

def main(input_file, output):
    spark = SparkSession.builder \
        .appName("Yelp Reviews") \
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

# Analyze the distribution of business hours by day of week
    hours_distribution = spark.sql("""
    SELECT 
        day_of_week,
        COUNT(DISTINCT business_name) as total_businesses,
        SUBSTR(business_hours, 1, 5) as opening_time,
        COUNT(*) as businesses_with_opening_time
    FROM fact_business_reviews
    WHERE business_hours IS NOT NULL
    GROUP BY day_of_week, SUBSTR(business_hours, 1, 5)
    ORDER BY day_of_week, businesses_with_opening_time DESC
    """)

    hours_distribution.show(20, False)

    # Find businesses that are open late (after 9 PM)
    late_night_businesses = spark.sql("""
    WITH hour_extracted AS (
        SELECT 
            business_name,
            day_of_week,
            CAST(SPLIT(SPLIT(business_hours, '-')[1], ':')[0] AS INT) as closing_hour
        FROM fact_business_reviews
        WHERE business_hours IS NOT NULL
    )
    
    SELECT 
        l.city,
        f.business_name,
        r.rating_value,
        f.day_of_week,
        f.business_hours,
        CAST(SPLIT(SPLIT(f.business_hours, '-')[1], ':')[0] AS INT) as closing_hour
    FROM fact_business_reviews f
    JOIN dim_location l ON f.location_id = l.location_id
    JOIN dim_rating r ON f.rating_id = r.rating_id
    WHERE CAST(SPLIT(SPLIT(f.business_hours, '-')[1], ':')[0] AS INT) >= 21
    ORDER BY closing_hour DESC, r.rating_value DESC
    """)

    late_night_businesses.show(20, False)


    # Analyze which days businesses are most commonly open
    day_availability = spark.sql("""
    WITH business_days AS (
        SELECT 
            business_name,
            COLLECT_LIST(day_of_week) as days_open,
            COUNT(DISTINCT day_of_week) as num_days_open
        FROM fact_business_reviews
        GROUP BY business_name
    )

    SELECT 
        num_days_open,
        COUNT(*) as num_businesses,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT business_name) FROM fact_business_reviews), 2) as percentage
    FROM business_days
    GROUP BY num_days_open
    ORDER BY num_days_open DESC
    """)

    day_availability.show()
    # Analyze if higher ratings correlate with more reviews
    rating_review_correlation = spark.sql("""
    SELECT 
        r.rating_value,
        COUNT(DISTINCT f.business_name) as num_businesses,
        AVG(f.review_count) as avg_review_count,
        MIN(f.review_count) as min_review_count,
        MAX(f.review_count) as max_review_count
    FROM fact_business_reviews f
    JOIN dim_rating r ON f.rating_id = r.rating_id
    WHERE day_of_week = 'Monday' -- Using one day to avoid duplicates
    GROUP BY r.rating_value
    ORDER BY r.rating_value DESC
    """)

    rating_review_correlation.show()
    # Compare weekday vs weekend operation patterns
    weekday_weekend_analysis = spark.sql("""
    WITH weekday_businesses AS (
        SELECT DISTINCT business_name
        FROM fact_business_reviews
        WHERE day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
    ),
    weekend_businesses AS (
        SELECT DISTINCT business_name
        FROM fact_business_reviews
        WHERE day_of_week IN ('Saturday', 'Sunday')
    ),
    categorized_businesses AS (
        SELECT 
            b.name as business_name,
            b.city,
            b.stars,
            b.review_count,
            CASE
                WHEN wb.business_name IS NOT NULL AND we.business_name IS NOT NULL THEN 'All Week'
                WHEN wb.business_name IS NOT NULL THEN 'Weekday Only'
                WHEN we.business_name IS NOT NULL THEN 'Weekend Only'
                ELSE 'Unknown'
            END as operation_pattern
        FROM yelp_businesses b
        LEFT JOIN weekday_businesses wb ON b.name = wb.business_name
        LEFT JOIN weekend_businesses we ON b.name = we.business_name
    )

    SELECT 
        operation_pattern,
        COUNT(*) as num_businesses,
        AVG(stars) as avg_rating,
        AVG(review_count) as avg_review_count
    FROM categorized_businesses
    GROUP BY operation_pattern
    ORDER BY num_businesses DESC
    """)

    weekday_weekend_analysis.show()
    # Find businesses with consistent hours all week vs. variable hours
    hours_consistency = spark.sql("""
    WITH business_hours_patterns AS (
        SELECT 
            business_name,
            COUNT(DISTINCT business_hours) as unique_hour_patterns,
            MAX(business_hours) as sample_hours
        FROM fact_business_reviews
        GROUP BY business_name
    )

    SELECT 
        CASE
            WHEN unique_hour_patterns = 1 THEN 'Consistent Hours'
            WHEN unique_hour_patterns BETWEEN 2 AND 3 THEN 'Slightly Variable'
            ELSE 'Highly Variable'
        END as hours_pattern,
        COUNT(*) as num_businesses,
        ROUND(AVG(unique_hour_patterns), 2) as avg_unique_patterns
    FROM business_hours_patterns
    GROUP BY 
        CASE
            WHEN unique_hour_patterns = 1 THEN 'Consistent Hours'
            WHEN unique_hour_patterns BETWEEN 2 AND 3 THEN 'Slightly Variable'
            ELSE 'Highly Variable'
        END
    ORDER BY avg_unique_patterns
    """)

    hours_consistency.show()
    # Create a comprehensive city business profile
    city_profile = spark.sql("""
    WITH city_stats AS (
        SELECT 
            l.city,
            COUNT(DISTINCT f.business_name) as num_businesses,
            AVG(r.rating_value) as avg_rating,
            SUM(f.review_count) as total_reviews,
            PERCENTILE(r.rating_value, 0.5) as median_rating
        FROM fact_business_reviews f
        JOIN dim_location l ON f.location_id = l.location_id
        JOIN dim_rating r ON f.rating_id = r.rating_id
        WHERE day_of_week = 'Monday' -- Using one day to avoid duplicates
        GROUP BY l.city
    )

    SELECT 
        cs.*,
        RANK() OVER (ORDER BY avg_rating DESC) as rating_rank,
        RANK() OVER (ORDER BY num_businesses DESC) as size_rank,
        RANK() OVER (ORDER BY total_reviews DESC) as popularity_rank
    FROM city_stats cs
    ORDER BY avg_rating DESC
    """)

    #city_profile.write.mode("overwrite").saveAsTable("city_business_profile")
    city_profile.show(20, False)
    # Find businesses that open early (before 8 AM)
    early_openers = spark.sql("""
    SELECT 
        day_of_week,
        l.city,
        f.business_name,
        r.rating_value,
        f.business_hours,
        CAST(SPLIT(SPLIT(f.business_hours, '-')[0], ':')[0] AS INT) as opening_hour
    FROM fact_business_reviews f
    JOIN dim_location l ON f.location_id = l.location_id
    JOIN dim_rating r ON f.rating_id = r.rating_id
    WHERE CAST(SPLIT(SPLIT(f.business_hours, '-')[0], ':')[0] AS INT) < 8
    ORDER BY day_of_week, opening_hour, r.rating_value DESC
    """)

    early_openers.show(20, False)

    # Calculate and analyze business operating duration
    hours_duration = spark.sql("""
    SELECT 
        day_of_week,
        business_name,
        business_hours,
        CAST(SPLIT(SPLIT(business_hours, '-')[0], ':')[0] AS INT) as opening_hour,
        CAST(SPLIT(SPLIT(business_hours, '-')[1], ':')[0] AS INT) as closing_hour,
        CASE 
            WHEN CAST(SPLIT(SPLIT(business_hours, '-')[1], ':')[0] AS INT) < 
                 CAST(SPLIT(SPLIT(business_hours, '-')[0], ':')[0] AS INT) 
            THEN CAST(SPLIT(SPLIT(business_hours, '-')[1], ':')[0] AS INT) + 24 - 
                 CAST(SPLIT(SPLIT(business_hours, '-')[0], ':')[0] AS INT)
            ELSE CAST(SPLIT(SPLIT(business_hours, '-')[1], ':')[0] AS INT) - 
                 CAST(SPLIT(SPLIT(business_hours, '-')[0], ':')[0] AS INT)
        END as hours_open
    FROM fact_business_reviews
    WHERE business_hours IS NOT NULL
    """)

    hours_duration.createOrReplaceTempView("hours_duration")

    duration_summary = spark.sql("""
    SELECT
        day_of_week,
        AVG(hours_open) as avg_hours_open,
        MIN(hours_open) as min_hours_open,
        MAX(hours_open) as max_hours_open,
        PERCENTILE(hours_open, 0.5) as median_hours_open
    FROM hours_duration
    GROUP BY day_of_week
    ORDER BY day_of_week
    """)

    duration_summary.show()

    # Identify the highest-rated businesses in each city with substantial review counts
    top_rated_by_city = spark.sql("""
        SELECT 
        city, 
        business_name, 
        rating_value, 
        review_count
    FROM (
        SELECT 
            l.city, 
            f.business_name, 
            r.rating_value, 
            f.review_count,
            ROW_NUMBER() OVER (PARTITION BY l.city ORDER BY r.rating_value DESC, f.review_count DESC) as rank
        FROM fact_business_reviews f
        JOIN dim_location l ON f.location_id = l.location_id
        JOIN dim_rating r ON f.rating_id = r.rating_id
        WHERE day_of_week = 'Monday' -- Using one day to avoid duplicates
    ) ranked
    WHERE rank <= 5
    ORDER BY city, rating_value DESC, review_count DESC
        """)

    top_rated_by_city.show(50, False)

if __name__ == "__main__":
    import sys

    if len(sys.argv) == 3:
        input_file = sys.argv[1]
        output = sys.argv[2]
        main(input_file, output)
    else:
        print("Expected: input output")