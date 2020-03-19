"""
Contains Iex class which retrieves information from IEX API
"""

import json
import os
from decimal import Decimal
import requests
import app
from urllib.parse import urlencode


class Iex(object):

    def __init__(self):
        self.stock_list = []
        self.Logger = app.get_logger(__name__)
        self.Symbols = self.get_stocks()

    def get_stocks(self):
        """
        Will return all the stocks being traded on IEX.
        :return: list of stock tickers and basic facts as list(), raises AppException if encountered an error
        """
        try:
            # basically we create a market snapshot
            uri = app.BASE_API_URL + 'ref-data/Iex/symbols/' + app.API_TOKEN
            self.stock_list = self.load_from_iex(uri)
            for stock in self.stock_list:
                stock['cash-flow'] = self.get_cash_flow(stock['symbol'])
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
        params['token'] = os.getenv('API_TOKEN')
        uri = app.BASE_API_URL + 'stock/' + symbol + '/cash-flow?'
        uri += urlencode(params)
        cash_flow = self.load_from_iex(uri)
        return cash_flow

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
