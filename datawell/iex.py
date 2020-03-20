"""
Contains Iex class which retrieves information from IEX API
"""

import json
import os
from decimal import Decimal
import requests
import app
from datawell.decorators import retry
from urllib.parse import urlencode


class Iex(object):

    def __init__(self):
        self.Logger = app.get_logger(__name__)
        self.Symbols = self.get_stocks()
        self.get_companies()
        self.populate_financials()
        for stock in self.stock_list:
            stock['cash-flow'] = self.get_cash_flow(stock['symbol'])

    def get_stocks(self):
        """
        Will return all the stocks being traded on IEX.
        :return: list of stock tickers and basic facts as list(), raises AppException if encountered an error
        """
        try:
            # basically we create a market snapshot
            uri = app.BASE_API_URL + 'ref-data/Iex/symbols/' + app.API_TOKEN
            self.stock_list = self.load_from_iex(uri)
            self.pull_all_books()
            return self.stock_list

        except Exception as e:
            message = 'Failed while retrieving stock list!'
            ex = app.AppException(e, message)
            raise ex

    def pull_all_books(self):
        """
        Pulls all books to Symbols dict
        """
        for symbol_dict in self.stock_list:
            symbol_name = symbol_dict['symbol']
            get_book_uri = f'{app.BASE_API_URL}stock/{symbol_name}/book{app.API_TOKEN}'
            symbol_dict['book'] = self.load_from_iex(get_book_uri)

    def get_companies(self):
        if self.Symbols:
            try:
                for company_symbol in self.Symbols:
                    uri = app.BASE_API_URL + 'stock/{}/company'.format(company_symbol['symbol']) + app.API_TOKEN
                    company_symbol['company_info'] = self.load_from_iex(uri)
            except Exception as e:
                message = 'Failed while retrieving company list!'
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

    @retry(delay=5, max_delay=30)
    def load_from_iex(self, uri: str):
        """
        Connects to the specified IEX endpoint and gets the data you requested.
        :type uri: str with the endpoint to query
        :return Dict() with the answer from the endpoint, Exception otherwise
        """
        self.Logger.info('Now retrieving from ' + uri)
        response = requests.get(uri)
        if response.status_code == 200:
            company_info = json.loads(response.content.decode("utf-8"), parse_float=Decimal)
            self.Logger.debug('Got response: ' + str(company_info))
            return company_info
        else:
            error = response.status_code
            self.Logger.error(
                'Encountered an error: ' + str(error) + "(" + str(response.text) + ") while retrieving " + str(uri))

            return error

    def populate_financials(self, ticker: dict = None) -> None:
        if ticker:
            self.populate_ticker_financials(ticker)
        else:
            for ticker in self.stock_list:
                self.populate_ticker_financials(ticker)

    def populate_ticker_financials(self, ticker: dict) -> None:
        url = f'{app.BASE_API_URL}stock/{ticker["symbol"]}/financials/{app.API_TOKEN}'
        ticker['financials'] = self.load_from_iex(url).get('financials')
