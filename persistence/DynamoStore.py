from datetime import datetime
from typing import Generator

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

import app
from app import Results, ActionStatus, AppException


class DynamoStore:
    def __init__(self):
        self.Logger = app.get_logger(__name__)
        self.dynamo_resource = boto3.resource('dynamodb', region_name=app.REGION)
        pass

    def store_documents(self, documents: list):
        """
        Persists list of dict() provided into the Dynamo table of the repo
        :param documents:
        :return: ActionStatus with SUCCESS when stored successfully, ERROR if failed, AppException if AWS Error: No access etc
                """
        pass

    def get_filtered_documents(self, symbol_to_find: str = None, target_date: datetime.date = None):
        """
        Returns a list of documents matching given ticker and/or date
        :param symbol_to_find: ticker as a string
        :param target_date: desired date as a datetime.date, leave empty to get for all available dates
        :return: a list of dicts() each containing data available for a stock for a given period of time
        """
        pass

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
            table = self.dynamo_resource.Table(app.TABLE)
            if symbols_to_remove:
                self.Logger.info(f"Deleting {symbols_to_remove}")
                total_deleted_items = self._delete_symbols(table, symbols_to_remove)
                output.ActionStatus = ActionStatus.SUCCESS
                output.Results = total_deleted_items
            else:
                self.Logger.info('Nothing is specified to delete, so deleting the entire table')
                self._recreate_table(table)
                output.ActionStatus = ActionStatus.SUCCESS
                output.Results = -1
        except AssertionError:
            output.Results = "You have to pass a list of dict to the method!"
        except ClientError as e:
            raise AppException(e, "An error has occurred while interacting with Dynamo")
        return output

    def _delete_symbols(self, table, symbols_to_remove: list) -> int:
        """
        Use this method to delete symbols using the batch_writer.
        :param table: where to delete.
        :param symbols_to_remove: what to delete.
        :return: total deleted items.
        """
        total_items_to_delete: int = 0
        with table.batch_writer() as batch:
            for symbol in symbols_to_remove:
                for page in self._query_by_page(table, symbol["symbol"]):
                    total_items_to_delete += page["Count"]
                    for item in page["Items"]:
                        batch.delete_item(
                            Key={
                                "symbol": item["symbol"],
                                "date": item["date"]
                            }
                        )
        return total_items_to_delete

    @staticmethod
    def _query_by_page(table, symbol: str) -> Generator[dict, None, None]:
        """Use this method to query all symbol related records using pagination.

        :param table: where to search.
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
            page: dict = table.query(**query_params)

            yield page

            last_evaluated_key = page.get('LastEvaluatedKey')
            if not last_evaluated_key:
                return

    def _recreate_table(self, table) -> None:
        """
        Use this method to clean the table quickly. It drops and creates a new table with the same schema.
        """
        self.Logger.info('Delete the table')
        table.delete()
        self.Logger.info('Wait until the table is deleted')
        table.meta.client.get_waiter('table_not_exists').wait(TableName=app.TABLE)

        self.Logger.info('Create a new table.')
        table = self.dynamo_resource.create_table(
            TableName=app.TABLE,
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
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL',
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 100,
                    }
                }
            ],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 100
            },
        )
        self.Logger.info('Wait until the table exists.')
        table.meta.client.get_waiter('table_exists').wait(TableName=app.TABLE)

    def remove_empty_strings(dict_to_clean: dict) -> Results:
        """
        Removes all the empty key+value pairs from the dict you give it; use to clean up dicts before persisting them to the DynamoDB
        :param dict_to_clean: as dict()
        :return: only non-empty key+value pairs from the source dict as dict() inside Results
        """
        try:
            # start with your guard clauses - thats a standard situation you have to handle in your error handling
            assert type(dict_to_clean) is dict

            # here comes processing...
            cleaned_dict = dict()

            # now you are ready to ship back...
            output = Results()
            output.ActionStatus = ActionStatus.SUCCESS
            output.Results = cleaned_dict
            return output

        # here we have exceptions we KNOW we can encounter...
        except AssertionError:
            output = Results()
            output.Results = "You have to pass dict to this method!"

        # and this analog of *nix panic(*message)...
        except Exception as e:
            catastrophic_exception = AppException(ex=e, message='Catastrophic failure when trying to clean up dict '
                                                                'for the Dynamo!')
            raise catastrophic_exception
