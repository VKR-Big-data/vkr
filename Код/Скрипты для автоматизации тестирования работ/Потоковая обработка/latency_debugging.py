import time
import numpy as np
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
import os
import random


class LatencyAnalyzer:
    def __init__(self, kafka_bootstrap_servers, kafka_topic, output_path):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.output_path = output_path
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            auto_offset_reset='latest',
            value_deserializer=lambda x: int(x.decode('utf-8'))
        )
        self.latencies = []

        # Create the output directory if it doesn't exist
        os.makedirs(self.output_path, exist_ok=True)

    def start(self):
        print("Starting latency analyzer...")
        for message in self.consumer:
            latency = message.value
            self.latencies.append(latency)
            print(f"Received latency: {latency}")

    def stop(self):
        print("Stopping latency analyzer...")
        self.consumer.close()

        if not self.latencies:
            print("No latencies recorded.")
            return

        # Calculate statistics
        min_latency = np.min(self.latencies)
        max_latency = np.max(self.latencies)
        mean_latency = np.mean(self.latencies)
        std_latency = np.std(self.latencies)

        # Save statistics to a text file
        stats_file_path = os.path.join(
            self.output_path, 'latency_statistics.txt')
        with open(stats_file_path, 'w') as f:
            f.write(f"Min Latency: {min_latency}\n")
            f.write(f"Max Latency: {max_latency}\n")
            f.write(f"Mean Latency: {mean_latency}\n")
            f.write(f"Standard Deviation: {std_latency}\n")

        print(f"Statistics saved to {stats_file_path}")

        # Select every 100th latency value
        sampled_latencies = self.latencies[::15]

        # Plot the latencies
        plt.figure(figsize=(10, 5))
        plt.plot(sampled_latencies, marker='o', linestyle='-', color='b')
        plt.xlabel('Sample Index')
        plt.ylabel('Latency (ms)')
        plt.title('Latencies Over Time')
        plt.grid(True)
        plt.tight_layout()
        graph_file_path = os.path.join(self.output_path, 'latency_graph.png')
        plt.savefig(graph_file_path)
        plt.show()

        print(f"Graph saved to {graph_file_path}")


# Example usage
if __name__ == "__main__":
    analyzer = LatencyAnalyzer(
        kafka_bootstrap_servers="localhost:9092", kafka_topic="spark_latency_data", output_path="experiment_configs\stream\Data formatting and writing to hdfs\sex"
    )

    try:
        analyzer.start()
    except KeyboardInterrupt:
        analyzer.stop()
