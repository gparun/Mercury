"""
Contains Iex class which retrieves information from IEX API
"""

import json
from decimal import Decimal
from typing import List

import requests
import app
from layers.mercury.datawell.decorators import retry, log_execution_time


class Iex(object):
    SYMBOL_BATCH_SIZE = 100
    DATAPOINT_BATCH_SIZE = 10

    def __init__(self, datapoints: List[str] = None):
        self.Logger = app.get_logger(__name__)
        self.Symbols = self.get_stocks()
        self.datapoints = self._check_datapoints(datapoints)
        self.load_symbols_datapoints()

    @log_execution_time()
    def _check_datapoints(self, datapoints_to_check: List[str]) -> List[str]:
        """
        This method is used to check whether the desired datapoints are valid and accessible.
        It makes a test API call for each datapoint in the list.
         If there is no any valid datapoint, then list with "company" only is returned.

        :param datapoints_to_check:
        :return: list of valid datapoints
        """
        valid_blocks: List[str] = []
        if datapoints_to_check and isinstance(datapoints_to_check, list):
            for block in datapoints_to_check:
                result: app.Results = self.load_from_iex("stock/aapl/batch", {"types": block})
                if result.ActionStatus == app.ActionStatus.SUCCESS:
                    valid_blocks.append(block)
        if not valid_blocks:
            valid_blocks.append("company")
        return valid_blocks

    @log_execution_time()
    def get_stocks(self):
        """
        Will return all the stocks being traded on IEX.
        :return: list of stock tickers and basic facts as list(), raises AppException if encountered an error
        """
        try:
            # basically we create a market snapshot
            results: app.Results = self.load_from_iex("ref-data/Iex/symbols/")
            return results.Results
        except Exception as e:
            message = 'Failed while retrieving stock list!'
            ex = app.AppException(e, message)
            raise ex

    @log_execution_time()
    @retry(delay=5, max_delay=30)
    def load_from_iex(self, uri: str, params: dict = None) -> app.Results:
        """
        Connects to the IEX endpoint and gets the data you requested
        :param uri: service path
        :param params: extra parameters to include to url
        :return: Dict() with the answer from the endpoint
        """
        self.Logger.info(f"Now retrieving from {app.BASE_API_URL}{uri}")

        request_params = {"token": app.API_TOKEN}
        if params is not None:
            request_params.update(params)

        response = requests.get(f"{app.BASE_API_URL}{uri}", params=request_params)
        results = app.Results()

        if response.status_code == 200:
            iex_data = json.loads(response.content.decode("utf-8"), parse_float=Decimal)

            results.ActionStatus = app.ActionStatus.SUCCESS
            results.Results = iex_data
        else:
            error = response.status_code
            self.Logger.error(
                f"Encountered an error: {error} ({response.text}) while retrieving {app.BASE_API_URL}{uri}")
            if params is not None:
                self.Logger.error(f"Failed parameters: {params}")
            results.Results = error

        return results

    @log_execution_time()
    def load_symbols_datapoints(self):
        for datapoints_batch in self._batchify(self.datapoints, self.DATAPOINT_BATCH_SIZE):
            self._load_symbols_datapoints_info(datapoints_batch)

    def _load_symbols_datapoints_info(self, datapoints: List[str]):
        """
        Splits Symbols into the 100 item bathes and get IEX data for each batch and defined datapoints.
        Updates Symbols with retrieved datapoint data.
        :param datapoints: list of datapoint names. Note that datapoints count must not exceed 10
        """
        symbols_dict = {symbol["symbol"]: symbol for symbol in self.Symbols}
        for symbols_batch in self._batchify(list(symbols_dict.keys()), self.SYMBOL_BATCH_SIZE):
            result = self.load_symbols_from_iex("stock/market/batch", symbols_batch, datapoints)
            if result.ActionStatus == app.ActionStatus.SUCCESS:
                self.update_symbols(symbols_dict, result.Results)
        self.Symbols = list(symbols_dict.values())

    def _batchify(self, lst, batch_size: int):
        for i in range(0, len(lst), batch_size):
            yield lst[i:i + batch_size]

    def update_symbols(self, symbols_dict: dict, data_points_data: dict):
        for symbol_name in data_points_data.keys():
            symbol = symbols_dict[symbol_name]
            point_symbol = data_points_data[symbol_name]
            for point_name in point_symbol.keys():
                symbol[point_name] = point_symbol[point_name]

    def load_symbols_from_iex(self, url_path: str, symbols: List[str], datapoints: List[str]):
        assert len(symbols) <= 100, 'Load from IEX error: symbols count must not exceed 100 per request'
        assert len(datapoints) <= 10, 'Load from IEX error: datapoints count must not exceed 10 per request'

        request_params = {}
        if len(symbols) > 0:
            request_params['symbols'] = ','.join(symbols)
        if len(datapoints) > 0:
            request_params['types'] = ','.join(datapoints)
        result = self.load_from_iex(url_path, request_params)
        return result
