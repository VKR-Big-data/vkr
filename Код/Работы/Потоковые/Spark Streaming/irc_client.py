from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, expr, substring, current_timestamp, unix_millis
from kafka import KafkaProducer

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToHDFS") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Kafka configurations
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "irc_messages2"
kafka_metrics_topic = "spark_latency"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: str(v).encode('utf-8')
)

# Create DataFrame representing the stream of input lines from Kafka
lines = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Define the schema and transformations
messages = lines.selectExpr("CAST(value AS STRING) as value") \
    .select(split(col("value"), " ", 5).alias("parts")) \
    .selectExpr(
        "parts[0] as arrival_timestamp",
        "split(parts[1], '!')[0] as raw_username",
        "parts[3] as channel",
        "parts[4] as message"
    ) \
    .selectExpr(
        "CAST(arrival_timestamp AS LONG) as arrival_timestamp",
        "substring(raw_username, 2, length(raw_username) - 1) as user",
        "substring(channel, 2, length(channel) - 1) as streamer",
        "substring(message, 2, length(message) - 1) as msg"
    ) \
    .withColumn("processing_timestamp", (unix_millis(current_timestamp())).cast("long"))

# Define the function to send messages to Kafka
def send_to_kafka(df, epoch_id):
    for row in df.collect():
        latency = row['processing_timestamp'] - row['arrival_timestamp']
        producer.send(kafka_metrics_topic, latency)
    producer.flush()

# Write the streaming data to HDFS in CSV format and send metrics to Kafka
query = messages.writeStream \
    .outputMode("append") \
    .foreachBatch(send_to_kafka) \
    .start()

query.awaitTermination()