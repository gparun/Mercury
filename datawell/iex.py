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
        self.company_symbols = []
        self.Logger = app.get_logger(__name__)
        self.Symbols = self.get_stocks()
        datapoints = ['logo', 'company']
        self.Datapoints = dict(zip(datapoints, datapoints))
        self.Companies = {}

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

    def get_company_data(self, company_symbol=None):
        """
        method for IEX which will populate COMPANY data
        either for all stocks (if no ticker given) or a particular stock (if ticker given).
        :return: a list of companies or a particular company
        """
        try:
            return self.get_all_companies() if company_symbol is None else self.get_company_by_symbol(company_symbol)
        except Exception as e:
            message = 'Failed while retrieving company data!'
            ex = app.AppException(e, message)
            raise ex

    def get_all_companies(self):
        for company_symbol in self.get_company_symbols():
            self.Companies[company_symbol] = self.get_company_by_symbol(company_symbol)
        return self.Companies

    def get_company_by_symbol(self, company_symbol: str):
        uri = app.BASE_API_URL + '/stock/{}/company'.format(company_symbol) + app.API_TOKEN
        return self.load_from_iex(uri)

    def get_company_symbols(self):
        """
        Get all company symbols from stocks list
        :return: company symbols
        """
        self.company_symbols = [stock['symbol'] for stock in self.stock_list]
        return self.company_symbols

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
