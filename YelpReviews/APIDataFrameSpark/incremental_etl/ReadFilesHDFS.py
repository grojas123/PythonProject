from pyspark.sql import SparkSession
import os

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

    # Use Spark's FileSystem API to list files instead of subprocess
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

    print(input_files)
if __name__ == "__main__":
    import sys

    if len(sys.argv) == 3:
        input_path = sys.argv[1]
        output_base = sys.argv[2]
        main(input_path, output_base)
    else:
        print("Expected: input_path output_base_path")