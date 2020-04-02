import time
import logging

from boto3.exceptions import RetriesExceededError
from botocore.exceptions import ClientError

import app


RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException',
                    'ThrottlingException')


class RetryConfig:
    def __init__(self, max_attempts: int = 1, init_delay: int = 1, max_delay: int = 60):
        """
        :param max_attempts: Maximum number of retry attempts.
            A value of 0 disables the retry mechanism
            A value less than 0 will result in an unlimited number of retries
        :param init_delay: Initial delay in seconds.
            A value equals or less than 0 will result in initial delay of 1 second
        :param max_delay: Maximum number of seconds to pause between retry attempts.
            A value less than 0 will result in unlimited pause time
        """
        self.max_attempts = max_attempts
        self.init_delay = init_delay if init_delay > 0 else 1
        self.max_delay = max_delay if max_delay >= 0 else float("inf")


class DynamoBatchWriter(object):
    def __init__(self, table, dynamo_client, flush_amount=25, retries=RetryConfig()):
        """
        :param table: The name of Dynamo table
        :param dynamo_client: A botocore client
        :param flush_amount: Maximum number of items to keep in buffer before flushing
        :param retries: Retry specific configurations
        """
        self.Logger = app.get_logger(__name__, level=logging.INFO)
        self._table_name = table
        self._client = dynamo_client
        self._items_buffer = []
        self._flush_amount = flush_amount
        self._retries = retries
        self._retry_attempt = 0

    def put_item(self, Item) -> None:
        put_request = {'PutRequest': {'Item': Item}}
        self._items_buffer.append(put_request)
        if len(self._items_buffer) >= self._flush_amount:
            self._flush()

    def _backoff_if_needed(self):
        if self._retry_attempt > 0 and self._retries.max_delay != 0:
            delay = min(self._retries.init_delay * 2 ** (self._retry_attempt - 1), self._retries.max_delay)
            self.Logger.debug(f"_backoff_if_needed: retry attempt - {self._retry_attempt },"
                              f" delay - {delay} second(s)")
            time.sleep(delay)

    def _flush(self):
        self._backoff_if_needed()
        items_to_send = self._items_buffer[:self._flush_amount]
        self._items_buffer = self._items_buffer[self._flush_amount:]

        try:
            self.Logger.debug(f"_flush: number of items to send - {len(items_to_send)}")
            response = self._client.batch_write_item(RequestItems={self._table_name: items_to_send})
            unprocessed_items = response['UnprocessedItems']

            if unprocessed_items and unprocessed_items[self._table_name]:
                self.Logger.debug(f"_flush: number of unprocessed items - {len(unprocessed_items[self._table_name])}")
                self._prepare_retry(unprocessed_items[self._table_name])
            else:
                self._items_buffer = []
                self._retry_attempt = 0
        except ClientError as err:
            if err.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                raise
            self._prepare_retry(items_to_send)
            self._flush()

    def _prepare_retry(self, unprocessed_items):
        self._retry_attempt = self._retry_attempt + 1
        if 0 <= self._retries.max_attempts < self._retry_attempt:
            raise RetriesExceededError(None,
                                       msg=f"Max Retries Exceeded: retry attempt - {self._retry_attempt},"
                                           f" max retries - {self._retries.max_attempts}")
        self._items_buffer.extend(unprocessed_items)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        while self._items_buffer:
            self._flush()
