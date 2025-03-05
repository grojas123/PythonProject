from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, TimestampType, StringType

import sys
import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.9'
if len(sys.argv) == 2:
    card_file = sys.argv[1]
else:
    print("Expected: cardsfile")
    sys.exit(1)

# Create SparkSession
spark = SparkSession.builder \
    .appName("Cards SQL") \
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
    .getOrCreate()

# Define schema
schema = StructType() \
    .add("DATE", TimestampType()) \
    .add("GAME_ID", StringType()) \
    .add("GAME_TYPE", StringType()) \
    .add("CARD", StringType())

try:
    # Create DataFrame from file
    cards = spark.read \
        .option("delimiter", "\t") \
        .option("header", "true") \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .schema(schema) \
        .csv("hdfs://localhost:9000" + card_file)

    # Register DataFrame as temp view
    cards.createOrReplaceTempView("CARDS_TABLE")

    # Execute SQL queries
    sql_result_simple = spark.sql("SELECT GAME_TYPE, CARD FROM CARDS_TABLE LIMIT 5")
    print("Simple query result showing game types and cards:")
    sql_result_simple.show()

    print("Simple query result showing game types equal to :PaiGow")
    sql_result_card_type = spark.sql("SELECT * FROM CARDS_TABLE WHERE GAME_TYPE = 'PaiGow'")
    sql_result_card_type.show(10)

    games_for_day = spark.sql("SELECT COUNT(*) FROM CARDS_TABLE WHERE DATE LIKE '2015-01-10%'")
    games_for_day.show()

    count_by_game_type = spark.sql("SELECT GAME_TYPE, COUNT(*) FROM CARDS_TABLE GROUP BY GAME_TYPE")
    count_by_game_type.show(10)

except Exception as e:
    print(f"Error executing SQL: {str(e)}")

finally:
    spark.stop()