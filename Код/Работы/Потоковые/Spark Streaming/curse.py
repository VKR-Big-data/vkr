from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, expr, current_timestamp, unix_millis, regexp_replace, udf, trim
from pyspark.sql.types import StringType, LongType
from kafka import KafkaProducer

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FilterCurseWords") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Kafka configurations
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "irc_messages2"
filtered_topic = "filtered_messages"
latency_topic = "spark_latency"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: v if isinstance(v, bytes) else v.encode('utf-8')
)

# Load curse words from a text file
with open("curse_words.txt", "r", encoding='utf-8') as file:
    curse_words = file.read().splitlines()

# Create UDF for filtering curse words
def censor_text(text, curse_words):
    for word in curse_words:
        text = text.replace(word, '*' * len(word))
    return text

censor_udf = udf(lambda text: censor_text(text, curse_words), StringType())

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
        "parts[4] as message"
    ) \
    .selectExpr(
        "CAST(arrival_timestamp AS LONG) as arrival_timestamp",
        "substring(raw_username, 2, length(raw_username) - 1) as user",
        "substring(message, 2, length(message) - 1) as msg"
    ) \
    .withColumn("msg", regexp_replace(col("msg"), "\n|\r", "")) \
    .withColumn("msg", censor_udf(col("msg"))) \
    .withColumn("msg", trim(col("msg"))) \
    .withColumn("filtered_msg", censor_udf(col("msg"))) \
    .withColumn("processing_timestamp", (unix_millis(current_timestamp())).cast(LongType()))

def send_to_kafka(df, epoch_id):
    for row in df.collect():
        filtered_message = f"{row['user']}:{row['filtered_msg']}"
        latency = row['processing_timestamp'] - row['arrival_timestamp']
        if '*' in filtered_message:
            producer.send(filtered_topic, filtered_message.encode('utf-8'))
        producer.send(latency_topic, str(latency).encode('utf-8'))
    producer.flush()

# Write the streaming data and send metrics to Kafka
query = messages.writeStream \
    .outputMode("append") \
    .foreachBatch(send_to_kafka) \
    .start()

query.awaitTermination()