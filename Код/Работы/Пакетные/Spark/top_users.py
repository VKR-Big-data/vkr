from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, StructField

def main():
    spark = SparkSession.builder.appName("Top Ten Users").getOrCreate()

    input_path = "hdfs://localhost:9000/user/hadoop/large"
    schema = StructType([
        StructField("arrival_timestamp", StringType(), True),
        StructField("user", StringType(), True),
        StructField("streamer", StringType(), True),
        StructField("msg", StringType(), True),
        StructField("processing_timestamp", StringType(), True)
    ])

    df = spark.read.format("csv").option("delimiter", "$").option("header", "false").schema(schema).load(input_path)

    user_counts = df.groupBy("user").count()
    top_ten_users = user_counts.orderBy(col("count").desc())

    top_ten_users.show(truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()