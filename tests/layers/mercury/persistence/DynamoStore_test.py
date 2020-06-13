import pytest

from app import Results, ActionStatus
from layers.mercury.persistence.DynamoStore import DynamoStore


@pytest.fixture()
def dynamo_db(mocker):
    return mocker.patch('boto3.resource')


def test_clean_table_empty_list(dynamo_db):
    # Mock
    dynamo_store: DynamoStore = DynamoStore()

    # Execute
    results: Results = dynamo_store.clean_table([])

    # Assert
    assert results.ActionStatus == ActionStatus.SUCCESS
    assert results.Results == -1
