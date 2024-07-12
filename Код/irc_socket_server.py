from kafka import KafkaProducer
import socket
import threading
import time
from queue import Queue
from flask import Flask, request, jsonify

app = Flask(__name__)

class IRCClient:
    def __init__(self, server, port, nickname, token, kafka_server, kafka_topic):
        self.server = server
        self.port = port
        self.nickname = nickname
        self.token = token
        self.channels = []
        self.kafka_server = kafka_server
        self.kafka_topic = kafka_topic
        self.command_queue = Queue()
        self.producer = KafkaProducer(bootstrap_servers=[kafka_server])
        self.stop_event = threading.Event()
        self.threads = []

        threading.Thread(target=self.process_commands, daemon=True).start()

    def process_commands(self):
        while True:
            command, args = self.command_queue.get()
            if command == "add_channel":
                self._add_channel(args)
            elif command == "clear_channels":
                self._clear_channels()
            self.command_queue.task_done()

    def irc_connection(self, channel):
        irc_sock = socket.socket()
        irc_sock.connect((self.server, self.port))
        irc_sock.send(f"PASS {self.token}\n".encode('utf-8'))
        irc_sock.send(f"NICK {self.nickname}\n".encode('utf-8'))
        irc_sock.send(f"JOIN {channel}\n".encode('utf-8'))
        print(f"Connected to IRC channel {channel}")

        while not self.stop_event.is_set():
            try:
                resp = irc_sock.recv(2048).decode('utf-8')
                if resp.startswith('PING'):
                    irc_sock.send("PONG\n".encode('utf-8'))
                elif "PRIVMSG" in resp:
                    arrival_timestamp = str(int(float(time.time()) * 1000))
                    formatted_resp = f"{arrival_timestamp} {resp}"
                    print(f"Received message: {formatted_resp}")
                    self.producer.send(self.kafka_topic, formatted_resp.encode('utf-8'))
            except socket.error:
                break

        irc_sock.close()
        print(f"Disconnected from IRC channel {channel}")

    def _add_channel(self, channel):
        self.channels.append(channel)
        thread = threading.Thread(target=self.irc_connection, args=(channel,))
        thread.daemon = True
        thread.start()
        self.threads.append(thread)
        print(f"Added channel {channel}")

    def _clear_channels(self):
        self.stop_event.set()
        for thread in self.threads:
            thread.join()
        self.threads = []
        self.channels = []
        self.stop_event.clear()
        print("Cleared all channels and stopped threads")

    def add_channel(self, channel):
        self.command_queue.put(("add_channel", channel))
        return f"Adding channel {channel}"

    def clear_channels(self):
        self.command_queue.put(("clear_channels", None))
        return "Clearing all channels"

# Initialize the IRC client instance
irc_client = IRCClient(
    server="irc.chat.twitch.tv",
    port=6667,
    nickname="FrolovGeorgiy",
    token="###",
    kafka_server="localhost:9092",
    kafka_topic="irc_messages2"
)

@app.route('/add_channel', methods=['POST'])
def add_channel():
    data = request.json
    channel = data.get('channel')
    if channel:
        response = irc_client.add_channel(channel)
        return jsonify({"message": response}), 200
    return jsonify({"message": "Channel is required"}), 400

@app.route('/', methods=['GET'])
def root():
    return jsonify({}), 200

@app.route('/clear_channels', methods=['POST'])
def clear_channels():
    response = irc_client.clear_channels()
    return jsonify({"message": response}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)