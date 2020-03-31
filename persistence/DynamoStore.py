from datetime import datetime

from boto3.exceptions import RetriesExceededError
from botocore.exceptions import ClientError
import app
from app import Results, ActionStatus, AppException
import boto3

from persistence.DynamoBatchWriter import DynamoBatchWriter, RetryConfig


class DynamoStore:
    def __init__(self):
        pass

    def store_documents(self, documents: list) -> ActionStatus:
        """
        Persists list of dict() provided into the Dynamo table of the repo
        :param documents:
        :return: ActionStatus with SUCCESS when stored successfully, ERROR if failed, AppException if AWS Error: No access etc
                """
        try:
            cleaned_documents = self.cleanup_symbol_documents(documents)
            client = self.get_dynamodb_resouce()
            with DynamoBatchWriter(table=app.AWS_TABLE_NAME, dynamo_client=client, retries=RetryConfig(10)) as batch:
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
        pass

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
            cleaned_dict = dict_to_clean

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

    def get_dynamodb_resouce(self):
        return boto3.resource(
            'dynamodb',
            aws_access_key_id=app.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=app.AWS_SECRET_ACCESS_KEY,
            region_name=app.AWS_TABLE_REGION
        )

    def validate_symbol_document(self, document):
        return document is not None and ('symbol' in document) and ('date' in document)

    def cleanup_symbol_documents(self, documents):
        """
                1. Removes all the empty key+value pairs from documents
                2. Deletes documents without 'symbol' and 'date' key from the list.
                :param documents: list of symbol dicts
                :returns: cleaned up list of symbol dicts
                """
        non_empty_docs_result = [DynamoStore.remove_empty_strings(doc).Results for doc in documents]
        cleaned_up = [doc for doc in non_empty_docs_result if self.validate_symbol_document(doc)]
        return cleaned_up
