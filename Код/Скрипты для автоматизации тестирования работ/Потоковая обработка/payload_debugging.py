import time
import numpy as np
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
import os


class PayloadDebug:
    def __init__(self, topic, server, output_path, interval=10):
        self.topic = topic
        self.server = server
        self.interval = interval
        self.output_path = output_path
        self.consumer = KafkaConsumer(
            self.topic, bootstrap_servers=self.server, auto_offset_reset='latest')
        self.message_counts = []
        self.running = False

        # Create the output directory if it doesn't exist
        os.makedirs(self.output_path, exist_ok=True)

    def start(self):
        self.running = True
        while self.running:
            interval_start_time = time.time()
            count = 0
            interval_end_time = interval_start_time + self.interval
            while time.time() < interval_end_time:
                msg_pack = self.consumer.poll(timeout_ms=500)
                count += sum(len(v) for v in msg_pack.values())
            self.message_counts.append(count)
            print(f"Messages in last {self.interval} seconds: {count}")

            elapsed_time = time.time() - interval_start_time
            remaining_time = self.interval - elapsed_time

            if remaining_time > 0:
                for _ in range(int(remaining_time)):
                    if not self.running:
                        break
                    time.sleep(1)
                if self.running:
                    time.sleep(remaining_time - int(remaining_time))

    def stop(self):
        print("Stopping payload debugger...")
        self.running = False
        if not self.message_counts:
            print("No messages recorded.")
            return

        # Calculate statistics
        max_val = max(self.message_counts)
        min_val = min(self.message_counts)
        mean_val = sum(self.message_counts) / len(self.message_counts)
        std_val = np.std(self.message_counts)

        # Save statistics to a text file
        stats_file_path = os.path.join(
            self.output_path, 'payload_statistics.txt')
        with open(stats_file_path, 'w') as f:
            f.write(f"Max messages: {max_val}\n")
            f.write(f"Min messages: {min_val}\n")
            f.write(f"Mean messages: {mean_val}\n")
            f.write(f"Standard Deviation: {std_val}\n")

        print(f"Statistics saved to {stats_file_path}")

        # Plot the results
        times = [(i+1) * self.interval for i in range(len(self.message_counts))]

        plt.figure(figsize=(10, 6))
        plt.plot(times, self.message_counts, marker='o')
        plt.title('Number of Messages Sent to Kafka Topic Over Time')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Number of Messages')
        plt.grid(True)
        graph_file_path = os.path.join(
            self.output_path, 'payload_message_counts.png')
        plt.savefig(graph_file_path)
        plt.show()

        print(f"Graph saved to {graph_file_path}")
