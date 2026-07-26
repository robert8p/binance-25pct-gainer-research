from datetime import datetime, timezone

from app.alpaca_client import AlpacaClient, AlpacaError, extract_invalid_symbols, is_likely_stock_symbol
from app.config import Settings

UTC = timezone.utc


def test_extract_invalid_symbol_from_alpaca_json_error():
    message = 'Alpaca HTTP 400: {"message":"invalid symbol: 0029900E0"}'
    assert extract_invalid_symbols(message) == {"0029900E0"}


def test_likely_stock_symbol_rejects_cusip_like_identifier():
    assert is_likely_stock_symbol("AAPL")
    assert is_likely_stock_symbol("BRK.B")
    assert not is_likely_stock_symbol("0029900E0")
    assert not is_likely_stock_symbol("")


def test_multi_symbol_bars_prunes_rejected_symbol_and_retries():
    client = AlpacaClient(Settings(request_pause_seconds=0, max_retries=1))
    calls = []

    def fake_request(method, url, *, params=None):
        calls.append(dict(params or {}))
        if "0029900E0" in (params or {}).get("symbols", ""):
            raise AlpacaError('Alpaca HTTP 400: {"message":"invalid symbol: 0029900E0"}')
        return {"bars": {"AAPL": [{"t": "2026-07-01T04:00:00Z", "c": 200}]}, "next_page_token": None}

    client._request = fake_request  # type: ignore[method-assign]
    try:
        result = client.get_bars(
            ["AAPL", "0029900E0"],
            timeframe="1Day",
            start=datetime(2026, 7, 1, tzinfo=UTC),
            end=datetime(2026, 7, 2, tzinfo=UTC),
            feed="sip",
        )
    finally:
        client.close()

    assert len(calls) == 2
    assert calls[1]["symbols"] == "AAPL"
    assert result["AAPL"][0]["c"] == 200
    assert result["0029900E0"] == []
