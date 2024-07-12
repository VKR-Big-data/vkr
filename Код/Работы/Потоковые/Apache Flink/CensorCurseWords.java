import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CensorCurseWords {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrapServers = "localhost:9092";
        String kafkaTopic = "irc_messages2";
        String filteredTopic = "filtered_messages";
        String latencyTopic = "flink_latency";

        // Create Kafka topics
        createKafkaTopic(kafkaBootstrapServers, filteredTopic);
        createKafkaTopic(kafkaBootstrapServers, latencyTopic);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBootstrapServers);
        properties.setProperty("group.id", "flink-group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), properties);
        FlinkKafkaProducer<String> filteredKafkaProducer = new FlinkKafkaProducer<>(kafkaBootstrapServers, filteredTopic, new SimpleStringSchema());
        FlinkKafkaProducer<String> latencyKafkaProducer = new FlinkKafkaProducer<>(kafkaBootstrapServers, latencyTopic, new SimpleStringSchema());

        DataStream<String> text = env.addSource(kafkaConsumer);

        List<String> curseWords = loadCurseWords("curse_words.txt");

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
                    String msg = value.split(":", 3)[2].replace("\n", "").replace("\r", "").trim();

                    // Censor the message
                    msg = censorText(msg, curseWords);

                    long processingTimestamp = System.currentTimeMillis();

                    return new Message(arrivalTimestamp, user, msg, processingTimestamp);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).filter(message -> message != null); // Filter out null messages

        // Send filtered messages to Kafka
        messages.map(new MapFunction<Message, String>() {
            @Override
            public String map(Message message) {
                return message.getUser() + "::" + message.getMsg();
            }
        }).addSink(filteredKafkaProducer);

        // Calculate latency and send to Kafka
        messages.map(new MapFunction<Message, String>() {
            @Override
            public String map(Message message) {
                long latency = message.getProcessingTimestamp() - message.getArrivalTimestamp();
                return Long.toString(latency);
            }
        }).addSink(latencyKafkaProducer);

        env.execute("IRC Client Flink Job");
    }

    public static class Message implements Serializable {
        private long arrivalTimestamp;
        private String user;
        private String msg;
        private long processingTimestamp;

        public Message() {
        }

        public Message(long arrivalTimestamp, String user, String msg, long processingTimestamp) {
            this.arrivalTimestamp = arrivalTimestamp;
            this.user = user;
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
            return arrivalTimestamp + "$" + user + "$" + msg + "$" + processingTimestamp;
        }
    }

    private static String censorText(String text, List<String> curseWords) {
        for (String word : curseWords) {
            text = text.replaceAll("(?i)" + word, "*".repeat(word.length()));
        }
        return text;
    }

    private static List<String> loadCurseWords(String filePath) {
        try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(filePath))) {
            return br.lines().collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
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