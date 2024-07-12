from utils import try_connect, load_config, save_config
from connections_checker import check_connections
from logs import config_logger, logger, critical_log_with_error
import argparse
import os
import time
from datetime import datetime
from payload_debugging import PayloadDebug
from latency_debugging import LatencyAnalyzer
import threading
from channel_management import add_channel, clear_channels


def main():
    try:
        parser = argparse.ArgumentParser(
            description='Experiment directory')
        parser.add_argument('experiment_directory_path', type=str,
                            help='Path to the experiment directory')
        path = parser.parse_args().experiment_directory_path
        config = load_config(f"{path}/config.json")

        logger.success(
            "Successfully got configuration. Initializing current experiment directory!")
    except Exception as e:
        critical_log_with_error(
            "Unable to find path provided, or it doesn't contain config.json, please provide the correct path for the experiment directory!", e)
        exit()

    try:
        current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        new_directory_path = os.path.join(path, current_time)
        os.makedirs(new_directory_path, exist_ok=True)

        old_config_path = os.path.join(new_directory_path, 'config_old.json')
        save_config(config, old_config_path)

        config_logger(new_directory_path)

        logger.success(
            "Successfully initialized current try directory and saved old config!")
    except Exception as e:
        critical_log_with_error("Can't create current try directory!", e)
        exit()

    REQUIRED_SERVICES = [
        # {"title": "Spark Master", "port": 8080},
        # {"title": "Flink Web UI", "port": 8085},
        {"title": "IRC Socket Server", "port": 5000},
    ]

    try:
        check_connections(REQUIRED_SERVICES)
    except Exception as e:
        critical_log_with_error("Required services are not available!", e)
        exit()


    logger.success("Successfully submitted job!")

    for streamer in config['streamers']:
        add_channel(streamer)

    # Initialize debugging tools
    kafka_bootstrap_servers = "localhost:9092"
    payload_topic = "irc_messages2"
    latency_topic = "spark_latency"
    # duration of the experiment in seconds
    duration = config['experiment_duration']

    payload_debugger = PayloadDebug(
        topic=payload_topic, server=kafka_bootstrap_servers, output_path=new_directory_path)
    latency_analyzer = LatencyAnalyzer(
        kafka_bootstrap_servers=kafka_bootstrap_servers, kafka_topic=latency_topic, output_path=new_directory_path)

    try:
        # Start debugging tools
        payload_debugger_thread = threading.Thread(
            target=payload_debugger.start)
        latency_analyzer_thread = threading.Thread(
            target=latency_analyzer.start)

        payload_debugger_thread.start()
        latency_analyzer_thread.start()

        # Wait for the duration of the experiment
        time.sleep(duration)

        payload_debugger.stop()
        latency_analyzer.stop()

        # Ensure threads have finished
        payload_debugger_thread.join()
        latency_analyzer_thread.join()

        clear_channels()
        # flink_thread.join()
    except Exception as e:
        critical_log_with_error("Error during the experiment!", e)
        exit()

    logger.success("Experiment completed successfully!")


if __name__ == "__main__":
    main()
