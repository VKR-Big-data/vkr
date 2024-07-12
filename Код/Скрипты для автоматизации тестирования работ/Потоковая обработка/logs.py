from loguru import logger


def config_logger(path):
    logger.add(f"{path}/file.log",
               format="{time} {level} {message}", level="INFO")


def critical_log_with_error(message, error):
    logger.critical(
        f"{message} \n Error is: {error}")
