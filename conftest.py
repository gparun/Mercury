import boto3
import pytest
from moto import mock_dynamodb2


@pytest.fixture()
def dynamo_db(mocker):
    resource = boto3.resource('dynamodb', region_name='us-east-1')
    mocker.patch('boto3.resource', return_value=resource)
    return resource


@pytest.fixture(scope='module')
def dynamo():
    with mock_dynamodb2():
        yield boto3.resource('dynamodb', region_name='us-east-1')

