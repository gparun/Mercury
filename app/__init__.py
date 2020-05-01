"""
Contains core constants, datatypes etc. used application wise
"""
import logging
import os
from enum import Enum
from pythonjsonlogger import jsonlogger

import boto3

BASE_API_URL: str = 'https://cloud.iexapis.com/v1/'
API_TOKEN = os.getenv('API_TOKEN')
MAX_RETRIEVAL_THREADS = 16
MAX_PERSISTENCE_THREADS = 16

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
TABLE_NAME = os.getenv('AWS_TABLE_NAME')
AWS_TABLE_NAME = os.getenv('AWS_TABLE_NAME')

if os.getenv('TEST_ENVIRONMENT') == 'True':
    BASE_API_URL: str = 'https://sandbox.iexapis.com/stable/'
    API_TOKEN = os.getenv('API_TEST_TOKEN')


class ActionStatus(Enum):
    SUCCESS = 0
    ERROR = -1


class Results:
    def __init__(self):
        self.ActionStatus: ActionStatus = ActionStatus.ERROR
        self.Results = None


class AppException(Exception):
    def __init__(self, ex, message="See exception for detailed message."):
        self.Exception = ex
        self.Message = message


def get_logger(module_name: str, level: int = logging.DEBUG):
    log_format = '%(asctime)s - %(name)s - %(process)d - [%(levelname)s] - %(message)s'
    log_date_format = '%d-%b-%y %H:%M:%S'

    logger = logging.getLogger(module_name)
    logger.setLevel(level)

    logger_type = os.getenv('LOGGER_TYPE')
    if logger_type == "json":
        formatter = jsonlogger.JsonFormatter(log_format, datefmt=log_date_format)
    else:
        formatter = logging.Formatter(log_format, datefmt=log_date_format)

    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        filename = os.getenv('LOG_FILE')
        if filename:
            handler = logging.FileHandler(filename)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

    return logger


def get_dynamodb_resource():
    return boto3.resource(
        'dynamodb',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
