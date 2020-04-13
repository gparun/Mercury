from typing import List

import pytest

import app
from datawell.iex import Iex


def test_load_valid_blocks(mocker):
    # ARRANGE:
    datapoint: str = "logo"
    mock_get_stocks = mocker.patch.object(Iex, "get_stocks")
    mock_pull_all_books = mocker.patch.object(Iex, "pull_all_books")
    mock_get_companies = mocker.patch.object(Iex, "get_companies")
    mock_populate_financials = mocker.patch.object(Iex, "populate_financials")
    mock_populate_dividends = mocker.patch.object(Iex, "populate_dividends")
    mock_populate_advanced_stats = mocker.patch.object(Iex, "populate_advanced_stats")
    results: app.Results = app.Results()
    results.ActionStatus = app.ActionStatus.SUCCESS
    mock_load_from_iex = mocker.patch.object(Iex, "load_from_iex", return_value=results)

    # ACT:
    datapoints: List[str] = Iex([datapoint]).datapoints

    # ASSERT:
    assert datapoints == [datapoint]
    mock_get_stocks.assert_called_once()
    mock_pull_all_books.assert_called_once()
    mock_get_companies.assert_called_once()
    mock_populate_financials.assert_called_once()
    mock_populate_dividends.assert_called_once()
    mock_populate_advanced_stats.assert_called_once()
    mock_load_from_iex.assert_called_once()


@pytest.mark.parametrize("datapoints", [
    None, [], {}, 1
])
def test_load_no_blocks(mocker, datapoints):
    # ARRANGE:
    mock_get_stocks = mocker.patch.object(Iex, "get_stocks")
    mock_pull_all_books = mocker.patch.object(Iex, "pull_all_books")
    mock_get_companies = mocker.patch.object(Iex, "get_companies")
    mock_populate_financials = mocker.patch.object(Iex, "populate_financials")
    mock_populate_dividends = mocker.patch.object(Iex, "populate_dividends")
    mock_populate_advanced_stats = mocker.patch.object(Iex, "populate_advanced_stats")

    # ACT:
    datapoints: List[str] = Iex(datapoints).datapoints

    # ASSERT:
    assert datapoints == ["company"]
    mock_get_stocks.assert_called_once()
    mock_pull_all_books.assert_called_once()
    mock_get_companies.assert_called_once()
    mock_populate_financials.assert_called_once()
    mock_populate_dividends.assert_called_once()
    mock_populate_advanced_stats.assert_called_once()


def test_load_invalid_blocks(mocker):
    # ARRANGE:
    datapoint: str = "invalid"
    mock_get_stocks = mocker.patch.object(Iex, "get_stocks")
    mock_pull_all_books = mocker.patch.object(Iex, "pull_all_books")
    mock_get_companies = mocker.patch.object(Iex, "get_companies")
    mock_populate_financials = mocker.patch.object(Iex, "populate_financials")
    mock_populate_dividends = mocker.patch.object(Iex, "populate_dividends")
    mock_populate_advanced_stats = mocker.patch.object(Iex, "populate_advanced_stats")
    results: app.Results = app.Results()
    results.ActionStatus = app.ActionStatus.ERROR
    mock_load_from_iex = mocker.patch.object(Iex, "load_from_iex", return_value=results)

    # ACT:
    datapoints: List[str] = Iex([datapoint]).datapoints

    # ASSERT:
    assert datapoints == ["company"]
    mock_get_stocks.assert_called_once()
    mock_pull_all_books.assert_called_once()
    mock_get_companies.assert_called_once()
    mock_populate_financials.assert_called_once()
    mock_populate_dividends.assert_called_once()
    mock_populate_advanced_stats.assert_called_once()
    mock_load_from_iex.assert_called_once()
