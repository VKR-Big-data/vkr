from utils import try_connect, is_http_service_running
from logs import logger


def check_connections(services):
    logger.info("Checking availibility of required services!")
    for service in services:
        try_connect(10, 10, is_http_service_running, service)
