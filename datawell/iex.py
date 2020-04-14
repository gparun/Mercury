"""
Contains Iex class which retrieves information from IEX API
"""

import json
from decimal import Decimal
import requests
import app
from datawell.decorators import retry
from typing import List

class Iex(object):

    SYMBOL_BATCH_SIZE = 100

    def __init__(self, datapoints: List[str] = None):
        self.Logger = app.get_logger(__name__)
        self.datapoints = datapoints
        self.Symbols = self.get_stocks()

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

    @retry(delay=5, max_delay=30)
    def load_from_iex(self, url_path: str, symbols: List[str] = None, datapoints: List[str] = None) -> app.Results:
        """
        Connects to the IEX endpoint and gets the data you requested
        :param symbols: list of Symbols you need to get data for. Note that symbols count must not exceed 100 per request
        :param datapoints: list of datapoints you need to get data for. Note that datapoints count must not exceed 10 per request
        :return: Dict() with the answer from the endpoint
        """
        assert symbols is None or len(symbols) <= 100, 'Load from IEX error: symbols count must not exceed 100 per request'
        assert datapoints is None or len(datapoints) <= 10, 'Load from IEX error: datapoints count must not exceed 10 per request'

        self.Logger.info(f"Now retrieving from {app.BASE_API_URL}{url_path}")

        request_params = {"token": app.API_TOKEN}
        if datapoints is not None:
            request_params['symbols'] = f"{','.join(symbols)}"
        if datapoints is not None:
            request_params['types'] = f"{','.join(datapoints)}"

        response = requests.get(f"{app.BASE_API_URL}{url_path}", params=request_params)
        results = app.Results()

        if response.status_code == 200:
            iex_data = json.loads(response.content.decode("utf-8"), parse_float=Decimal)

            results.ActionStatus = app.ActionStatus.SUCCESS
            results.Results = iex_data
        else:
            error = response.status_code
            self.Logger.error(
                f"Encountered an error: {error} ({response.text}) while retrieving {app.BASE_API_URL}{url_path}")
            if symbols is not None:
                self.Logger.info(f"Failed Symbols: {','.join(symbols)}")
            if datapoints is not None:
                self.Logger.info(f"Failed Datapoints: {','.join(datapoints)}")

            results.Results = error

        return results

    def load_symbols_datapoint_info(self, datapoints: List[str]):
        """
        Splits Symbols into the 100 item bathes and get IEX data for each batch and defined datapoints.
        Updates Symbols with retrieved datapoint data.
        :param datapoints: list of datapoint names. Note that datapoints count must not exceed 10
        """
        symbols_dict = {symbol["symbol"]: symbol for symbol in self.Symbols}
        symbol_names = list(symbols_dict.keys())
        for i in range(0, len(symbol_names), self.SYMBOL_BATCH_SIZE):
            result = self.load_from_iex("stock/market/batch", symbol_names[i: i + self.SYMBOL_BATCH_SIZE], datapoints)
            if result.ActionStatus == app.ActionStatus.SUCCESS:
                self.update_symbols(symbols_dict, result.Results)
        self.Symbols = list(symbols_dict.values())

    def update_symbols(self, symbols_dict: dict, data_points_data: dict):
        for symbol_name in data_points_data.keys():
            symbol = symbols_dict[symbol_name]
            point_symbol = data_points_data[symbol_name]
            for point_name in point_symbol.keys():
                symbol[point_name] = point_symbol[point_name]
