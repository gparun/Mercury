"""
Contains Iex class which retrieves information from IEX API
"""

import json
from decimal import Decimal
import requests
import app


class Iex(object):

    def __init__(self):
        self.stock_list = []
        self.Logger = app.get_logger(__name__)
        self.Symbols = self.get_stocks()
        self.cash_flow_list = {}

    def get_stocks(self):
        """
        Will return all the stocks being traded on IEX.
        :return: list of stock tickers and basic facts as list(), raises AppException if encountered an error
        """
        try:
            # basically we create a market snapshot
            uri = app.BASE_API_URL + 'ref-data/Iex/symbols/' + app.API_TOKEN
            self.stock_list = self.load_from_iex(uri)
            return self.stock_list

        except Exception as e:
            message = 'Failed while retrieving stock list!'
            ex = app.AppException(e, message)
            raise ex

    def get_cash_flow(self, symbol: str, params=None):
        """
        Will return cash flow data for specific symbol on IEX.
        :param params: dict of uri params for filtering
        :param symbol: str with symbol from stock obj
        :return: cash_flow as json obj
        """
        params = params if params else {}
        try:
            uri = app.BASE_API_URL + 'stock/' + symbol + '/cash-flow' + app.API_TOKEN
            if params:
                for key, value in params.items():
                    uri = uri + '&' + key + '=' + value
            cash_flow = self.load_from_iex(uri)
            self.cash_flow_list[symbol] = cash_flow
            return cash_flow
        except Exception as ex:
            message = 'Failed while retrieving cash flow list!'
            app_ex = app.AppException(ex, message)
            raise app_ex

    def get_all_cash_flows(self, params=None):
        """
        Gets cash_flow for all current stocks
        :return: List of cash_flow
        """
        params = params if params else {}
        try:
            return list(map(lambda stock: self.get_cash_flow(stock['symbol'], params), self.get_stocks()))
        except Exception as e:
            message = 'Failed while retrieving cash_flow list!'
            ex = app.AppException(e, message)
            raise ex

    def load_from_iex(self, uri: str):
        """
        Connects to the specified IEX endpoint and gets the data you requested.
        :type uri: str with the endpoint to query
        :return Dict() with the answer from the endpoint, Exception otherwise
        """
        self.Logger.info('Now retrieveing from ' + uri)
        response = requests.get(uri)
        if response.status_code == 200:
            company_info = json.loads(response.content.decode("utf-8"), parse_float=Decimal)
            self.Logger.debug('Got response: ' + str(company_info))
            return company_info
        else:
            error = response.status_code
            self.Logger.error(
                'Encountered an error: ' + str(error) + "(" + str(response.text) + ") while retrieving " + str(uri))
