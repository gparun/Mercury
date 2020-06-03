import logging
import os
from datetime import datetime

import app
from datawell.decorators import publish_running_time_metric, log_execution_time
from datawell.iex import Iex
from persistence.DynamoStore import DynamoStore


def lambda_handler(event=None, context=None):
    logger = app.get_logger(module_name=__name__, level=logging.INFO)
    try:
        start_time = datetime.now()
        datasource = _load_iex_data()
        _store_iex_data(datasource)

        # Ok, lets time our run...
        end_time = datetime.now()
        run_time = end_time - start_time
        logger.info('Timing: It took ' + str(run_time) + ' to finish this run')
    except app.AppException as e:
        logger.error(e.Message, exc_info=True)
        os._exit(-1)  # please note: python has no encapsulation - you can call private methods! doesnt mean you should


@log_execution_time()
@publish_running_time_metric('iex', 'load')
def _load_iex_data():
    datapoints: list = [
        "book", "company", "financials"
    ]
    return Iex(datapoints)


@log_execution_time()
@publish_running_time_metric('iex', 'store')
def _store_iex_data(datasource):
    datalake = DynamoStore(app.AWS_TABLE_NAME)
    datalake.store_documents(documents=datasource.Symbols)

