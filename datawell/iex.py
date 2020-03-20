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
        self.populate_dividends()
        for stock in self.stock_list:
            stock['cash-flow'] = self.get_cash_flow(stock['symbol'])
        self.populate_advanced_stats()

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

    def populate_dividends(self, ticker: dict = None, range: str = '1y'):
        """
        Adds dividends item to the dict specified by ticker argument.
        If the ticker is not specified, then walks though all company dicts in the stock list to add dividends.
        :param ticker: dictionary to add dividends to
        :param range: dividends time range
        """
        try:
            if ticker:
                ticker['dividends'] = self.load_dividends(ticker['symbol'], range)
            else:
                for ticker in self.stock_list:
                    self.populate_dividends(ticker, range)
        except Exception as e:
            raise app.AppException(e, 'Exception while populating dividends')

    def load_dividends(self, company_symbol: str, range: str = '1y'):
        """
        Makes API call to retrieve dividends for the company and time range
        :param company_symbol: company identifier
        :param range: time range to retrieve dividends for
        """
        url = f'{app.BASE_API_URL}stock/{company_symbol}/dividends/{range}/{app.API_TOKEN}'
        return self.load_from_iex(url)

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

    def populate_advanced_stats(self) -> app.ActionStatus:
        try:
            for stock in self.stock_list:
                self.populate_advanced_stats_for_stock(stock)
            return app.ActionStatus.SUCCESS
        except Exception as e:
            message = 'Failed while retrieving advanced-stats data'
            ex = app.AppException(e, message)
            raise ex

    def populate_advanced_stats_for_stock(self, stock: dict) -> app.ActionStatus:
        try:
            url = self.generate_symbol_info_service_url(stock['symbol'], 'advanced-stats')
            stock['advanced-stats'] = self.load_from_iex(url)
            return app.ActionStatus.SUCCESS
        except Exception as e:
            self.Logger.exception(f'Unable to load advanced-stats data for stock: {stock["symbol"]}')
            return app.ActionStatus.ERROR

    def generate_symbol_info_service_url(self, symbol: str, service_name: str) -> str:
        assert symbol and symbol.strip(), 'IEX symbol must be a non empty string'
        assert service_name and service_name.strip(), 'IEX service name must be a non empty string'
        return f'{app.BASE_API_URL}stock/{symbol}/{service_name}/{app.API_TOKEN}'
