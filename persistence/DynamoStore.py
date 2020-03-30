from datetime import date

import boto3
from boto3.dynamodb.conditions import Key

from app import Results, ActionStatus, AppException, DYNAMODB_TABLE_NAME


class DynamoStore:
    def __init__(self):
        self.Dynamodb = boto3.resource("dynamodb")
        self.Table = self.Dynamodb.Table(DYNAMODB_TABLE_NAME)

    def store_documents(self, documents: list):
        """
        Persists list of dict() provided into the Dynamo table of the repo
        :param documents:
        :return: ActionStatus with SUCCESS when stored successfully, ERROR if failed, AppException if AWS Error: No access etc
                """
        pass

    def get_filtered_documents(self, symbol_to_find: str = None, target_date: date = None) -> Results:
        """
        Returns a list of documents matching given ticker and/or date
        :param symbol_to_find: ticker as a string
        :param target_date: desired date as a datetime.date, leave empty to get for all available dates
        :return: a list of dicts() each containing data available for a stock for a given period of time
        """
        try:
            condition_expression = None
            date_expression = None
            if target_date:
                try:
                    assert type(target_date) is date
                except AssertionError:
                    output = Results()
                    output.Results = "Target date must be datetime.date!"
                    return output

                date_expression = Key("date").eq(str(target_date))

            if symbol_to_find:
                try:
                    assert type(symbol_to_find) is str
                except AssertionError:
                    output = Results()
                    output.Results = "Symbol to find must be string!"
                    return output

                symbol_expression = Key("symbol").eq(symbol_to_find)
                condition_expression = symbol_expression if not date_expression \
                    else (symbol_expression & date_expression)

                response = self.Table.query(KeyConditionExpression=condition_expression)
            else:
                response = self.Table.scan()

            items = response["Items"]

            while True:
                last_evaluated_key = response.get("LastEvaluatedKey")
                if last_evaluated_key:
                    if condition_expression:
                        response = self.Table.query(
                            KeyConditionExpression=condition_expression,
                            ExclusiveStartKey=last_evaluated_key
                        )
                    else:
                        response = self.Table.scan(ExclusiveStartKey=last_evaluated_key)
                    items += response["Items"]
                else:
                    break

            output = Results()
            output.ActionStatus = ActionStatus.SUCCESS
            output.Results = items
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
            return output

        # and this analog of *nix panic(*message)...
        except Exception as e:
            catastrophic_exception = AppException(ex=e, message='Catastrophic failure when trying to clean up dict '
                                                                'for the Dynamo!')
            raise catastrophic_exception
