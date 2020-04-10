import pytest

from datawell.iex import Iex


@pytest.mark.parametrize("blocks", [
    None, [], {}, 1, 1.0,
])
def test_load_no_blocks(mocker, blocks):
    # ARRANGE:
    iex: Iex = Iex(blocks)
    mock_get_stocks = mocker.patch.object(iex, "get_stocks")
    mock_load_blocks = mocker.patch.object(iex, "load_blocks")
    mock_get_companies = mocker.patch.object(iex, "get_companies")

    # ACT:
    iex.load()

    # ASSERT:
    mock_get_stocks.assert_called_once()
    mock_load_blocks.assert_not_called()
    mock_get_companies.assert_called_once()


@pytest.mark.parametrize("blocks, method", [
    (["books"], "pull_all_books"),
    (["financials"], "populate_financials"),
    (["dividends"], "populate_dividends"),
    (["cash_flow"], "get_cash_flows"),
    (["advanced_stats"], "populate_advanced_stats"),
])
def test_load_blocks(mocker, blocks, method):
    # ARRANGE:
    iex: Iex = Iex(blocks)
    mock_load_block = mocker.patch.object(iex, method)

    # ACT:
    iex.load_blocks()

    # ASSERT:
    mock_load_block.assert_called_once()
