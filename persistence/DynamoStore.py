from typing import Optional, List

from boto3.exceptions import RetriesExceededError
from botocore.exceptions import ClientError
import app
from persistence.DynamoBatchWriter import DynamoBatchWriter, RetryConfig
from datetime import date

import boto3
from boto3.dynamodb.conditions import Key

from app import Results, ActionStatus, AppException


class DynamoStore:
    def __init__(self, dynamo_db, table_name):
        self.Dynamodb = dynamo_db
        self.TableName = table_name
        self.Table = self.Dynamodb.Table(self.TableName)

    def store_documents(self, documents: list) -> ActionStatus:
        """
        Persists list of dict() provided into the Dynamo table of the repo
        :param documents:
        :return: ActionStatus with SUCCESS when stored successfully, ERROR if failed, AppException if AWS Error: No access etc
                """
        try:
            cleaned_documents = self.cleanup_symbol_documents(documents)
            with DynamoBatchWriter(table=self.TableName, dynamo_client=self.Dynamodb, retries=RetryConfig(10)) as batch:
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

    def get_filtered_documents(self, symbol_to_find: str = None, target_date: date = None) -> Results:
        """
        Returns a list of documents matching given ticker and/or date
        :param symbol_to_find: ticker as a string
        :param target_date: desired date as a datetime.date, leave empty to get for all available dates
        :return: a list of dicts() each containing data available for a stock for a given period of time
        """
        output = Results()
        try:
            criteria = SymbolFilterCriteria(symbol_to_find, target_date)
            output.Results = criteria.query(self.Table)
            output.ActionStatus = ActionStatus.SUCCESS
            return output
        except Exception as e:
            catastrophic_exception = AppException(ex=e, message='Catastrophic failure when trying to query symbols '
                                                                'for the Dynamo!')
            raise catastrophic_exception

    def get_filtered_documents_v2(self, symbol_to_find: str = None, target_date: date = None) -> Results:
        """
        Returns a list of documents matching given ticker and/or date
        :param symbol_to_find: ticker as a string
        :param target_date: desired date as a datetime.date, leave empty to get for all available dates
        :return: a list of dicts() each containing data available for a stock for a given period of time
        """

        try:
            condition_expression = None

            if symbol_to_find:
                try:
                    assert type(symbol_to_find) is str
                except AssertionError:
                    output = Results()
                    output.Results = "Symbol to find must be string!"
                    return output

                condition_expression = Key("symbol").eq(symbol_to_find)

            if target_date:
                try:
                    assert type(target_date) is date
                except AssertionError:
                    output = Results()
                    output.Results = "Target date must be datetime.date!"
                    return output

                date_expression = Key("date").eq(str(target_date))
                condition_expression = date_expression if not condition_expression \
                    else condition_expression & date_expression

            query_params: dict = {
                "TableName": self.TableName
            }

            if condition_expression:
                query_params["KeyConditionExpression"] = condition_expression

            if not symbol_to_find:
                query_params["IndexName"] = "data-symbol-index"

            total_items: List[dict] = []
            last_evaluated_key: Optional[dict] = None
            while True:
                if last_evaluated_key:
                    query_params["ExclusiveStartKey"] = last_evaluated_key

                result: dict = self.Table.query(**query_params)
                items = result.get("Items")
                if items:
                    total_items.extend(items)

                last_evaluated_key = result.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break

            output = Results()
            output.ActionStatus = ActionStatus.SUCCESS
            output.Results = total_items
            return output

        except Exception as e:
            catastrophic_exception = AppException(
                ex=e, message='Catastrophic failure when trying to clean up dict for the Dynamo!')
            raise catastrophic_exception

    def clean_table(self, symbols_to_remove: list):
        """
        Use this one to either clean specific stocks from the db or clean up the db if its small.
        :param symbols_to_remove: list of dicts each containing 'symbol' string
        :returns: number of the elements removed as int, 0 if not found, AppException if AWS Error: No access etc
        """
        pass

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
