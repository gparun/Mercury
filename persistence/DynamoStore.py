from datetime import datetime
from typing import Generator

from boto3.exceptions import RetriesExceededError
from botocore.exceptions import ClientError
import app
from persistence.DynamoBatchWriter import DynamoBatchWriter, RetryConfig
from datetime import date
from app import Results, ActionStatus, AppException
from boto3.dynamodb.conditions import Key


class DynamoStore:
    def __init__(self, table_name: str):
        self.Logger = app.get_logger(__name__)
        self.dynamoDb = app.get_dynamodb_resource()
        self.tableName = table_name
        self.table = self.dynamoDb.Table(self.tableName)

    def store_documents(self, documents: list) -> ActionStatus:
        """
        Persists list of dict() provided into the Dynamo table of the repo
        :param documents:
        :return: ActionStatus with SUCCESS when stored successfully, ERROR if failed, AppException if AWS Error: No access etc
                """
        try:
            cleaned_documents = self.cleanup_symbol_documents(documents)
            with DynamoBatchWriter(table=self.tableName, dynamo_client=self.dynamoDb, retries=RetryConfig(10)) as batch:
                for document in cleaned_documents:
                    batch.put_item(
                        Item={
                            'symbol': document['symbol'],
                            'date': document['date'],
                            'document': document
                        }
                    )
            return ActionStatus.SUCCESS
        except (ClientError, RetriesExceededError):
            return ActionStatus.ERROR
        except Exception as e:
            ex = AppException(ex=e, message='Failed to store documents to DynamoDB.')
            raise ex

    def get_filtered_documents(self, symbol_to_find: str = None, target_date: datetime.date = None):
        """
        Returns a list of documents matching given ticker and/or date
        :param symbol_to_find: ticker as a string
        :param target_date: desired date as a datetime.date, leave empty to get for all available dates
        :return: a list of dicts() each containing data available for a stock for a given period of time
        """
        output = Results()
        try:
            criteria = SymbolFilterCriteria(symbol_to_find, target_date)
            output.Results = criteria.query(self.table)
            output.ActionStatus = ActionStatus.SUCCESS
            return output
        except Exception as e:
            catastrophic_exception = AppException(ex=e, message='Catastrophic failure when trying to query symbols '
                                                                'for the Dynamo!')
            raise catastrophic_exception

    def clean_table(self, symbols_to_remove: list) -> Results:
        """
        Use this one to either clean specific stocks from the db or clean up the db if its small.
        :param symbols_to_remove: list of dicts each containing 'symbol' string
        :returns: number of the elements removed as int, 0 if not found, -1 if full table is cleaned,
         AppException if AWS Error: No access etc
        """
        output = Results()
        try:
            assert type(symbols_to_remove) is list
            if symbols_to_remove:
                self.Logger.info(f"Deleting {symbols_to_remove}")
                total_deleted_items = self._delete_symbols(symbols_to_remove)
                output.ActionStatus = ActionStatus.SUCCESS
                output.Results = total_deleted_items
            else:
                self.Logger.info('Nothing is specified to delete, so deleting the entire table')
                self._recreate_table()
                output.ActionStatus = ActionStatus.SUCCESS
                output.Results = -1
        except AssertionError:
            output.Results = "You have to pass a list of dict to the method!"
        except ClientError as e:
            raise AppException(e, "An error has occurred while interacting with Dynamo")
        return output

    def _delete_symbols(self, symbols_to_remove: list) -> int:
        """
        Use this method to delete symbols using the batch_writer.

        :param symbols_to_remove: what to delete.
        :return: total deleted items.
        """
        total_items_to_delete: int = 0
        with self.table.batch_writer() as batch:
            for symbol in symbols_to_remove:
                symbol_to_remove: str = symbol.get("symbol")
                if not symbol_to_remove:
                    continue
                for page in self._query_by_page(symbol_to_remove):
                    total_items_to_delete += page['Count']
                    for item in page["Items"]:
                        batch.delete_item(
                            Key={
                                "symbol": item["symbol"],
                                "date": item["date"]
                            }
                        )
        return total_items_to_delete

    def _query_by_page(self, symbol: str) -> Generator[dict, None, None]:
        """Use this method to query all symbol related records using pagination.

        :param symbol: what to search.
        :return: an iterable of pages. There is a python generator to perform lazy evaluated queries.
        """
        last_evaluated_key: dict = {}
        while True:
            query_params = {
                'KeyConditionExpression': Key('symbol').eq(symbol),
                'ProjectionExpression': '#symbol, #date',
                'ExpressionAttributeNames': {
                    "#symbol": "symbol",
                    "#date": "date",
                },
            }
            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key
            page: dict = self.table.query(**query_params)

            yield page

            last_evaluated_key = page.get('LastEvaluatedKey')
            if not last_evaluated_key:
                return

    def _recreate_table(self) -> None:
        """
        Use this method to clean the table quickly. It drops and creates a new table with the same schema.
        """
        self.Logger.info('Delete the table')
        self.table.delete()
        self.Logger.info('Wait until the table is deleted')
        self.table.meta.client.get_waiter('table_not_exists').wait(TableName=self.tableName)

        self.Logger.info('Create a new table.')
        self.table = self.dynamoDb.create_table(
            TableName=self.tableName,
            AttributeDefinitions=[
                {
                    'AttributeName': 'symbol',
                    'AttributeType': 'S',
                },
                {
                    'AttributeName': 'date',
                    'AttributeType': 'S',
                }
            ],
            KeySchema=[
                {
                    'AttributeName': 'symbol',
                    'KeyType': 'HASH',
                },
                {
                    'AttributeName': 'date',
                    'KeyType': 'RANGE',
                }
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'date-symbol-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'date',
                            'KeyType': 'HASH',
                        },
                        {
                            'AttributeName': 'symbol',
                            'KeyType': 'RANGE',
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL',
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 100,
                    }
                },
            ],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 100,
            },
        )
        self.Logger.info('Wait until the table exists.')
        self.table.meta.client.get_waiter('table_exists').wait(TableName=self.tableName)

    def remove_empty_strings(self, dict_to_clean: dict) -> Results:
        """
        Removes all the empty key+value pairs from the dict you give it; use to clean up dicts before persisting them to the DynamoDB
        :param dict_to_clean: as dict()
        :return: only non-empty key+value pairs from the source dict as dict() inside Results
        """
        try:
            # start with your guard clauses - thats a standard situation you have to handle in your error handling
            assert type(dict_to_clean) is dict

            # here comes processing...
            def recursive_clean(dict_to_process: dict) -> dict:
                for key in list(dict_to_process.keys()):
                    value = dict_to_process[key]
                    # clean if empty string or collection or None
                    if isinstance(value, (str, dict, list, tuple, type(None))) and not value:
                        del dict_to_process[key]
                    elif type(value) is dict:
                        processed_dict = recursive_clean(value)
                        if processed_dict:
                            dict_to_process[key] = processed_dict
                        else:
                            del dict_to_process[key]

                return dict_to_process

            cleaned_dict = recursive_clean(dict_to_clean)

            # now you are ready to ship back...
            output = Results()
            output.ActionStatus = ActionStatus.SUCCESS
            output.Results = cleaned_dict
            return output

        # here we have exceptions we KNOW we can encounter...
        except AssertionError:
            output = Results()
            output.Results = "You have to pass dict to this method!"
            return output

        # and this analog of *nix panic(*message)...
        except Exception as e:
            catastrophic_exception = AppException(ex=e, message='Catastrophic failure when trying to clean up dict '
                                                                'from the Dynamo!')
            raise catastrophic_exception

    def validate_symbol_document(self, document):
        return document is not None and ('symbol' in document) and ('date' in document)

    def cleanup_symbol_documents(self, documents):
        """
                1. Removes all the empty key+value pairs from documents
                2. Deletes documents without 'symbol' and 'date' key from the list.
                :param documents: list of symbol dicts
                :returns: cleaned up list of symbol dicts
                """
        non_empty_docs_result = [self.remove_empty_strings(doc).Results for doc in documents]
        cleaned_up = [doc for doc in non_empty_docs_result if self.validate_symbol_document(doc)]
        return cleaned_up


class SymbolFilterCriteria:
    def __init__(self, symbol_to_find: str = None, target_date: date = None):
        self.criteria_expression = self._build_criteria_expression(symbol_to_find, target_date)

    def _build_criteria_expression(self, symbol_to_find: str = None, target_date: date = None):
        criteria_expression = None
        if symbol_to_find:
            criteria_expression = Key("symbol").eq(symbol_to_find)
        if target_date:
            date_query = Key("date").eq(str(target_date))
            criteria_expression = criteria_expression & date_query if criteria_expression else date_query
        return criteria_expression

    def query(self, table):
        items = []
        last_evaluated_key = None
        while True:
            response = \
                table.query(KeyConditionExpression=self.criteria_expression, ExclusiveStartKey=last_evaluated_key) \
                if self.criteria_expression \
                else table.scan(ExclusiveStartKey=last_evaluated_key)
            items += response["Items"]
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break
        return items

