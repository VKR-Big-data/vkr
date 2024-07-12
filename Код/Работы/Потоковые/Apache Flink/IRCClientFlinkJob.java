import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class IRCClientFlinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrapServers = "localhost:9092";
        String kafkaTopic = "irc_messages2";
        String latencyTopic = "flink_latency";

        // Create Kafka topic for latency
        createKafkaTopic(kafkaBootstrapServers, latencyTopic);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBootstrapServers);
        properties.setProperty("group.id", "flink-group3");
        properties.setProperty("auto.offset.reset", "latest");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), properties);
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(kafkaBootstrapServers, latencyTopic, new SimpleStringSchema());

        DataStream<String> text = env.addSource(kafkaConsumer);

        DataStream<Message> messages = text.map(new MapFunction<String, Message>() {
            @Override
            public Message map(String value) {
                try {
                    // Assuming the message format is "arrival_timestamp :user!user@user.tmi.twitch.tv PRIVMSG #streamer :msg"
                    String[] parts = value.split(" ");
                    if (parts.length < 5) {
                        throw new IllegalArgumentException("Unexpected message format: " + value);
                    }

                    long arrivalTimestamp = Long.parseLong(parts[0]);
                    String user = parts[1].split("!")[0].substring(1);
                    String streamer = parts[3].substring(1);
                    String msg = value.split(":", 3)[2].replace("\n", "").replace("\r", "");

                    long processingTimestamp = System.currentTimeMillis();

                    Message message = new Message(arrivalTimestamp, user, streamer, msg, processingTimestamp);
                    System.out.println("Processed message: " + message);
                    return message;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).filter(message -> message != null); // Filter out null messages

        // Add sink to HDFS
        messages.addSink(new HDFSSink());

        // Add sink to Kafka for latency
        messages.map(new MapFunction<Message, String>() {
            @Override
            public String map(Message message) {
                long latency = message.getProcessingTimestamp() - message.getArrivalTimestamp();
                return Long.toString(latency);
            }
        }).addSink(kafkaProducer);

        env.execute("IRC Client Flink Job");
    }

    public static class Message implements Serializable {
        private long arrivalTimestamp;
        private String user;
        private String streamer;
        private String msg;
        private long processingTimestamp;

        public Message() {
        }

        public Message(long arrivalTimestamp, String user, String streamer, String msg, long processingTimestamp) {
            this.arrivalTimestamp = arrivalTimestamp;
            this.user = user;
            this.streamer = streamer;
            this.msg = msg;
            this.processingTimestamp = processingTimestamp;
        }

        public long getArrivalTimestamp() {
            return arrivalTimestamp;
        }

        public void setArrivalTimestamp(long arrivalTimestamp) {
            this.arrivalTimestamp = arrivalTimestamp;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getStreamer() {
            return streamer;
        }

        public void setStreamer(String streamer) {
            this.streamer = streamer;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg.strip().trim();
        }

        public long getProcessingTimestamp() {
            return processingTimestamp;
        }

        public void setProcessingTimestamp(long processingTimestamp) {
            this.processingTimestamp = processingTimestamp;
        }

        @Override
        public String toString() {
            return arrivalTimestamp + "$" + user + "$" + streamer + "$" + msg + "$" + processingTimestamp;
        }
    }

    public static class HDFSSink extends RichSinkFunction<Message> {
        private transient FileSystem fs;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000");
            fs = FileSystem.get(hadoopConf);
        }

        @Override
        public void invoke(Message value, Context context) throws Exception {
            String filePath = "/user/hadoop/large/" + System.currentTimeMillis() + ".csv";
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filePath);

            try (FSDataOutputStream outputStream = fs.create(path, true);
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
                writer.write(value.toString());
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void createKafkaTopic(String bootstrapServers, String topicName) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("client.id", "admin-client");

        try (AdminClient adminClient = AdminClient.create(properties)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            KafkaFuture<Void> future = adminClient.createTopics(Collections.singletonList(newTopic)).all();
            future.get();  // Ensure topic creation is complete
            System.out.println("Topic " + topicName + " created successfully.");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}