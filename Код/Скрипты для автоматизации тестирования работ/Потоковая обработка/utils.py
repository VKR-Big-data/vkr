import time
from logs import logger
import socket
import requests
import json


def submit_func(data):
    logger.success(f'''Successfully connected to {
                   data['title']} on port {data['port']}''')


def reject_func(data):
    logger.critical(f'''Unable to connect to {data['title']} on port {
                    data['port']}, make sure the service is running''')


def unsuccess_func(data):
    logger.error(f'''Can't connect to {data['title']} on port {
        data['port']}, trying again''')


def load_config(filename):
    with open(filename, 'r') as file:
        config = json.load(file)
    return config


def save_config(config, filename):
    with open(filename, 'w') as file:
        json.dump(config, file, indent=4)


def try_connect(n, i, func, args):
    for i in range(n):
        res = func(args)
        if (res):
            submit_func(args)
            return
        else:
            unsuccess_func(args)
        time.sleep(i)

    reject_func(args)
    raise Exception(f"Unable to connect to {args['name']}")


def is_http_service_running(args):
    url = f"http://localhost:{args['port']}"
    try:
        response = requests.get(url, timeout=2)
        if response.status_code == 200:
            return True
        else:
            return False
    except requests.RequestException:
        return False
