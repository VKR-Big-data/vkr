CREATE EXTERNAL TABLE IF NOT EXISTS large(
    arrival_timestamp BIGINT,
    `user` STRING,
    streamer STRING,
    msg STRING,
    processing_timestamp BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '$'
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hadoop/large';