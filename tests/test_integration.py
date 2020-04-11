from datetime import datetime
from unittest import TestCase

from app import TABLE_NAME
from datawell.iex import Iex
from persistence.DynamoStore import DynamoStore


class TestDynamoStore(TestCase):
    def test_store_documents_PassValidDocs_ExpectThemAppearInDB(self):
        # ARRANGE:
        datasource = Iex()
        datalake = DynamoStore(TABLE_NAME)

        # ACT:
        datalake.store_documents(documents=datasource.Symbols)

        # ASSERT:
        retrieved_docs = datalake.get_filtered_documents(target_date=datetime.now().date())
        self.assertDictEqual(datasource.Symbols, retrieved_docs, msg="Docs retrieved from the datalake does not match "
                                                                     "Symbols in Iex!")
