from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Count Entities").getOrCreate()

    # Path to the HDFS directory containing CSV files
    hdfs_path = "hdfs://localhost:9000/user/hadoop/large"

    # Define schema for the CSV files
    schema = "arrival_timestamp BIGINT, user STRING, streamer STRING, msg STRING, processing_timestamp BIGINT"

    # Read the CSV files
    df = spark.read.format("csv").option("header", "false").schema(schema).load(hdfs_path)

    # Count the number of rows
    count = df.count()

    print(f"Number of entities: {count}")

    spark.stop()

if __name__ == "__main__":
    main()