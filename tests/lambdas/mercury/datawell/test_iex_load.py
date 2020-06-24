from unittest.mock import call

import pytest
from requests.exceptions import SSLError, RequestException


@pytest.mark.parametrize("exception", [
    SSLError, RequestException
])
def test_request_error(mocker, monkeypatch, exception):
    # GIVEN
    monkeypatch.setattr('datawell.decorators.retry.__init__.__defaults__', (2, 0, 0, ()))
    mock_delay = mocker.patch('datawell.decorators.retry._exponential_delay', return_value=0)
    mocker.patch('requests.get', side_effect=exception)

    from datawell.iex import Iex
    mocker.patch.object(Iex, 'get_stocks', side_effect=None)
    mocker.patch.object(Iex, '_check_datapoints', side_effect=None)

    # WHEN
    Iex().load_from_iex('/uri')

    # THEN
    mock_delay.assert_has_calls([
        call(1, 5, 30),
        call(2, 5, 30)
    ])