from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StringType, StructField
import re
import os

def censor_text(text, curse_words):
    if text is None:
        return text
    for word in curse_words:
        regex_pattern = re.compile(re.escape(word), re.IGNORECASE)
        text = regex_pattern.sub('*' * len(word), text)
    return text

def main():
    spark = SparkSession.builder.appName("Censor Curse Words").getOrCreate()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    curse_words_path = os.path.join(current_dir, "curse_words.txt")

    with open(curse_words_path, 'r') as file:
        curse_words = [line.strip() for line in file.readlines()]

    broadcast_curse_words = spark.sparkContext.broadcast(curse_words)

    censor_udf = udf(lambda text: censor_text(text, broadcast_curse_words.value), StringType())

    input_path = "hdfs://localhost:9000/user/hadoop/large"

    schema = StructType([
        StructField("arrival_timestamp", StringType(), True),
        StructField("user", StringType(), True),
        StructField("streamer", StringType(), True),
        StructField("msg", StringType(), True),
        StructField("processing_timestamp", StringType(), True)
    ])

    df = spark.read.format("csv").option("header", "false").option("delimiter", "$").schema(schema).load(input_path)

    censored_df = df.withColumn("msg", censor_udf(df.msg))

    censored_df.show(10000, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()