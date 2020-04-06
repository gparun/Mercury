"""
Contains Iex class which retrieves information from IEX API
"""

import json
from decimal import Decimal
import requests
import app
from datawell.decorators import retry
from urllib.parse import urlencode


class Iex(object):

    def __init__(self):
        self.Logger = app.get_logger(__name__)
        self.Symbols = self.get_stocks()
        self.pull_all_books()
        self.get_companies()
        self.populate_financials()
        self.populate_dividends()
        for stock in self.Symbols:
            stock['cash-flow'] = self.get_cash_flow(stock['symbol'])
        self.populate_advanced_stats()

    def get_stocks(self):
        """
        Will return all the stocks being traded on IEX.
        :return: list of stock tickers and basic facts as list(), raises AppException if encountered an error
        """
        try:
            # basically we create a market snapshot
            uri = f'{app.BASE_API_URL}ref-data/Iex/symbols/{app.API_TOKEN}'
            results: app.Results = self.load_from_iex(uri)
            return results.Results
        except Exception as e:
            message = 'Failed while retrieving stock list!'
            ex = app.AppException(e, message)
            raise ex

    def pull_all_books(self):
        """
        Pulls all books to Symbols dict
        """
        for symbol in self.Symbols:
            uri = f"{app.BASE_API_URL}stock/{symbol['symbol']}/book{app.API_TOKEN}"
            results = self.load_from_iex(uri)
            if results.ActionStatus == app.ActionStatus.SUCCESS:
                symbol['book'] = results.Results

    def get_companies(self):
        try:
            for symbol in self.Symbols:
                uri = f"{app.BASE_API_URL}stock/{symbol['symbol']}/company{app.API_TOKEN}"
                results = self.load_from_iex(uri)
                if results.ActionStatus == app.ActionStatus.SUCCESS:
                    symbol['company'] = results.Results
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
        params['token'] = app.API_TOKEN
        uri = app.BASE_API_URL + 'stock/' + symbol + '/cash-flow?'
        uri += urlencode(params)
        cash_flow = ""
        results = self.load_from_iex(uri)
        if results.ActionStatus == app.ActionStatus.SUCCESS:
            cash_flow = results.Results
        return cash_flow

    @retry(delay=5, max_delay=30)
    def load_from_iex(self, uri: str) -> app.Results:
        """
        Connects to the specified IEX endpoint and gets the data you requested.
        :type uri: str with the endpoint to query
        :return Dict() with the answer from the endpoint, Exception otherwise
        """
        self.Logger.info(f'Now retrieving from {uri}')
        response = requests.get(uri)
        results = app.Results()

        if response.status_code == 200:
            iex_data = json.loads(response.content.decode("utf-8"), parse_float=Decimal)

            results.ActionStatus = app.ActionStatus.SUCCESS
            results.Results = iex_data
        else:
            error = response.status_code
            self.Logger.error(
                f'Encountered an error: {error} ({response.text}) while retrieving {uri}')
            results.Results = error

        return results

    def populate_financials(self, ticker: dict = None) -> None:
        if ticker:
            self.populate_ticker_financials(ticker)
        else:
            for ticker in self.Symbols:
                self.populate_ticker_financials(ticker)

    def populate_ticker_financials(self, ticker: dict) -> None:
        url = f'{app.BASE_API_URL}stock/{ticker["symbol"]}/financials/{app.API_TOKEN}'
        results = self.load_from_iex(url)
        if results.ActionStatus == app.ActionStatus.SUCCESS:
            ticker['financials'] = results.Results

    def populate_advanced_stats(self) -> app.ActionStatus:
        try:
            for stock in self.Symbols:
                self.populate_advanced_stats_for_stock(stock)
            return app.ActionStatus.SUCCESS
        except Exception as e:
            message = 'Failed while retrieving advanced-stats data'
            ex = app.AppException(e, message)
            raise ex

    def populate_advanced_stats_for_stock(self, stock: dict) -> app.ActionStatus:
        try:
            url = self.generate_symbol_info_service_url(stock['symbol'], 'advanced-stats')
            results = self.load_from_iex(url)
            if results.ActionStatus == app.ActionStatus.SUCCESS:
                stock['advanced-stats'] = results.Results
            return app.ActionStatus.SUCCESS
        except Exception as e:
            self.Logger.exception(f'Unable to load advanced-stats data for stock: {stock["symbol"]}')
            return app.ActionStatus.ERROR

    def generate_symbol_info_service_url(self, symbol: str, service_name: str) -> str:
        assert symbol and symbol.strip(), 'IEX symbol must be a non empty string'
        assert service_name and service_name.strip(), 'IEX service name must be a non empty string'
        return f'{app.BASE_API_URL}stock/{symbol}/{service_name}/{app.API_TOKEN}'

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
                for ticker in self.Symbols:
                    self.populate_dividends(ticker, range)
        except Exception as e:
            raise app.AppException(e, 'Exception while populating dividends')

    def load_dividends(self, company_symbol: str, range_: str = '1y'):
        """
        Makes API call to retrieve dividends for the company and time range
        :param company_symbol: company identifier
        :param range_: time range to retrieve dividends for
        """
        url = f'{app.BASE_API_URL}stock/{company_symbol}/dividends/{range_}/{app.API_TOKEN}'
        results = self.load_from_iex(url)
        if results.ActionStatus == app.ActionStatus.SUCCESS:
            return results.Results
        else:
            return ""

