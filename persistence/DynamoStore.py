from datetime import datetime
from datetime import date
from app import Results, ActionStatus, AppException, TABLE
import boto3
from boto3.dynamodb.conditions import Key


class DynamoStore:
    def __init__(self):
        self.dynamoDb = boto3.resource("dynamodb")
        self.table = self.dynamoDb.Table(TABLE)

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
                                                                'from the Dynamo!')
            raise catastrophic_exception


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
