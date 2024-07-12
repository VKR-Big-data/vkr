import requests
from logs import logger
import json


def add_channel(channel_name):
    url = "http://localhost:5000/add_channel"
    headers = {"Content-Type": "application/json"}
    body = {
        "channel": '#'+channel_name
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(body))
        if response.status_code == 200:
            logger.success(f"Successfully added channel: {channel_name}")
        else:
            logger.critical(f"Failed to add channel: {channel_name}")
    except Exception as e:
        print(e)
        logger.critical(f"Failed to add channel: {channel_name}")


def clear_channels():
    url = "http://localhost:5000/clear_channels"
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            logger.success(f"Successfully cleared channels!")
        else:
            logger.critical(f"Failed clear channels!")
    except Exception as e:
        print(e)
        logger.critical(f"Failed to add channel!")
