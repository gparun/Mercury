import unittest
from typing import Any, List

from _pytest.monkeypatch import MonkeyPatch
from app import ActionStatus, AppException
from persistence.DynamoStore import DynamoStore
from botocore.exceptions import ClientError
from unittest.mock import MagicMock

TABLE_NAME = "test-table"


def get_input() -> List:
    symbol_to_save = {'symbol': 'AAE', 'date': '2017-10-12', 'document': {'docField': "fieldsValue"}}
    return [symbol_to_save]


class StoreDocumentsTest(unittest.TestCase):
    """
    Tests for store_documents() method of DynamoStore.py
    """
    def setUp(self):
        self.mock_client = MagicMock()
        self.monkeypatch = MonkeyPatch()
        self.monkeypatch.setattr("app.get_dynamodb_resource", MagicMock(return_value=self.mock_client))
        self.dynamoStore = DynamoStore(TABLE_NAME)

    def test_success(self):
        # Mock
        success_response = {'UnprocessedItems': {}}
        self.mock_client.batch_write_item.side_effect = [success_response]
        # Execute
        result = self.dynamoStore.store_documents(get_input())
        # Assert
        self.assertEqual(result, ActionStatus.SUCCESS, 'Should return SUCCESS status if AWS did not return any exception')
        self.mock_client.batch_write_item.assert_called_once()

    def test_client_error(self):
        # Mock
        client_err_response = {'Error': {'Code': '500', 'Message': 'Error insert'}}
        self.mock_client.batch_write_item.side_effect = ClientError(client_err_response, "operation")
        # Execute
        result = self.dynamoStore.store_documents(get_input())
        # Assert
        self.assertEqual(result, ActionStatus.ERROR, 'Should return ERROR status if AWS threw not a Throttling ClientException')
        # if AWS threw not a Throttling Exception then we shouldn't retry save data
        self.mock_client.batch_write_item.assert_called_once()

    def test_retry_error(self):
        # Mock
        client_err_response = {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Error insert'}}
        self.mock_client.batch_write_item.side_effect = ClientError(client_err_response, "operation")
        # Execute
        result = self.dynamoStore.store_documents(get_input())
        # Assert
        self.assertEqual(result, ActionStatus.ERROR, 'Should return ERROR status if AWS threw Throttling '
                                                     'ClientException and max retry count is exceeded')
        # Should retry until max retry count(10) is exceeded
        self.assertEqual(self.mock_client.batch_write_item.call_count, 11)

    def test_retry_success(self):
        # Mock
        client_err_response = {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Error insert'}}
        success_response = {'UnprocessedItems': {}}
        client_response_list: List[Any] = [ClientError(client_err_response, "operation")] * 4
        client_response_list.append(success_response)
        self.mock_client.batch_write_item.side_effect = client_response_list
        # Execute
        result = self.dynamoStore.store_documents(get_input())
        # Assert
        self.assertEqual(result, ActionStatus.SUCCESS, 'Should return ERROR status if AWS threw Throttling '
                                                       'ClientException but after retry data was saved')
        # Should retry until AWS return success response
        self.assertEqual(self.mock_client.batch_write_item.call_count, 5)

    def test_non_client_error(self):
        # Mock
        self.mock_client.batch_write_item.side_effect = AppException("Test exception")
        # Should raise AppException if AWS threw not a ClientException'
        with self.assertRaises(AppException):
            self.dynamoStore.store_documents(get_input())
        self.mock_client.batch_write_item.assert_called_once()

    def test_cleanup_symbol_documents(self):
        doc1 = {'symbol': 'AAE', 'date': '2017-10-12', 'document1': {'docField': "fieldsValue"}}
        test_input = [{'symbol': 'AAE', 'date': '2017-10-12', 'document1': {'docField': "fieldsValue"}},
                      {'NotASymbol': 'AAE', 'date': '2017-10-12', 'document2': {'docField': "fieldsValue"}},
                      {'symbol': 'AAE', 'NotADate': '2017-10-12', 'document3': {'docField': "fieldsValue"}},
                      {'NotASymbol': 'AAE', 'NotADate': '2017-10-12', 'document4': {'docField': "fieldsValue"}}]
        result = self.dynamoStore.cleanup_symbol_documents(test_input)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], doc1)
