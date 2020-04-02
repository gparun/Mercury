import pytest

from persistence.DynamoStore import DynamoStore

DATABASE_TABLE='test_table'

@pytest.fixture(autouse=True)
def dynamo_store(dynamo, table):
    dynamo_store = DynamoStore()
    dynamo_store.dynamoDb = dynamo
    dynamo_store.table = table
    return dynamo_store


@pytest.fixture()
def table(dynamo):
    initial_create_resource(dynamo, DATABASE_TABLE)
    table = dynamo.Table(DATABASE_TABLE)
    data = [{"symbol":"EPM", "date":"2020-04-02"},
            {"symbol":"TRC", "date":"2020-03-02"}]
    for data_item in data:
        table.put_item(Item=dict(data_item))
    yield table


def test_get_filtered_doc(table, dynamo_store):
    # result: dict = table.scan()
    result = dynamo_store.get_filtered_documents(symbol_to_find='TRC')
    print(result.Results)


def initial_create_resource(dynamo, database_table_name: str):
    dynamo.create_table(
        AttributeDefinitions=[{"AttributeName": "symbol", "AttributeType": "S"},
                              {"AttributeName": "date", "AttributeType": "S"}],
        TableName=database_table_name,
        KeySchema=[{"AttributeName": "symbol", "KeyType": "HASH"},
                   {"AttributeName": "date", "KeyType": "RANGE"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    return dynamo

