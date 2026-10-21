"""Tests unitaires du MarketplaceAPIHook — requests et la Connection sont mockés (aucune API requise)."""

from unittest import mock

import pytest
import requests

from airflow.sdk import Connection
from hooks.marketplace_api_hook import MarketplaceAPIHook

TOKEN = "token-de-test"


def _response(payload, status_code=200):
    response = mock.MagicMock(spec=requests.Response)
    response.status_code = status_code
    response.json.return_value = payload
    if status_code >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(f"{status_code} Error", response=response)
    return response


@pytest.fixture
def hook():
    connection = Connection(conn_id="marketplace_api", conn_type="http", host="http://api-test:5000/", password=TOKEN)
    with mock.patch.object(MarketplaceAPIHook, "get_connection", return_value=connection):
        yield MarketplaceAPIHook()


def test_get_orders_sends_bearer_token_and_date(hook):
    orders = [{"order_id": "ORD-1"}, {"order_id": "ORD-2"}]
    with mock.patch.object(requests.Session, "get", return_value=_response({"count": 2, "data": orders})) as get:
        result = hook.get_orders("2026-04-07")

    assert result == orders
    get.assert_called_once_with("http://api-test:5000/orders", params={"date": "2026-04-07"}, timeout=hook.timeout)
    assert hook.get_conn().headers["Authorization"] == f"Bearer {TOKEN}"


def test_get_sellers_passes_limit_and_accepts_raw_list(hook):
    with mock.patch.object(requests.Session, "get", return_value=_response([{"seller_id": "SEL-1"}])) as get:
        assert hook.get_sellers(limit=5) == [{"seller_id": "SEL-1"}]
    assert get.call_args.kwargs["params"] == {"limit": 5}


def test_no_limit_param_when_none(hook):
    with mock.patch.object(requests.Session, "get", return_value=_response({"data": []})) as get:
        hook.get_products()
    assert get.call_args.kwargs["params"] == {}


@pytest.mark.parametrize("bad_date", ["07/04/2026", "2026-13-01", "", None])
def test_invalid_date_rejected_before_any_http_call(hook, bad_date):
    with mock.patch.object(requests.Session, "get") as get, pytest.raises(ValueError):
        hook.get_orders(bad_date)
    get.assert_not_called()


def test_http_error_is_raised(hook):
    with mock.patch.object(requests.Session, "get", return_value=_response({"error": "Unauthorized"}, 401)):
        with pytest.raises(requests.HTTPError):
            hook.get_orders("2026-04-07")


def test_retry_policy_on_5xx_only(hook):
    retry = hook.get_conn().get_adapter("http://api-test:5000").max_retries

    assert retry.total == 3
    assert {500, 502, 503, 504} <= set(retry.status_forcelist)
    assert 401 not in retry.status_forcelist
    assert "GET" in retry.allowed_methods


def test_missing_token_raises():
    connection = Connection(conn_id="marketplace_api", conn_type="http", host="http://api-test:5000")
    with mock.patch.object(MarketplaceAPIHook, "get_connection", return_value=connection):
        with pytest.raises(ValueError, match="token"):
            MarketplaceAPIHook().get_conn()
