"""Shared data contracts for ingestion, indicators, and dataset publication."""

from datetime import datetime, timezone

from airflow.sdk import Asset
import requests


PRICE_ASSET = Asset("snowflake://bitcoin_data/data/btc_hourly_data")
INDICATORS_ASSET = Asset("snowflake://bitcoin_data/data/hourly_ta")
HOUR = 3600


def closed_hour_cutoff():
    """Exclusive UTC cutoff: the candle currently trading is not complete."""
    return int(datetime.now(timezone.utc).timestamp()) // HOUR * HOUR


def asset_update_range(context):
    """Combine queued upstream changes, including explicit historical repairs."""
    changes = [
        event.extra
        for events in context.get("triggering_asset_events", {}).values()
        for event in events
        if event.extra and "start_timestamp" in event.extra and "through_timestamp" in event.extra
    ]
    if not changes:
        return None
    return {
        "start_timestamp": min(int(change["start_timestamp"]) for change in changes),
        "through_timestamp": max(int(change["through_timestamp"]) for change in changes),
    }


def fetch_hourly_klines(start_timestamp, end_timestamp, get=None):
    """Fetch every closed candle in [start, end), paging beyond Binance's limit."""
    get = get or requests.get
    cursor = start_timestamp
    records = []
    while cursor < end_timestamp:
        response = get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1h", "startTime": cursor * 1000,
                    "endTime": end_timestamp * 1000 - 1, "limit": 1000},
            timeout=(10, 60),
        )
        response.raise_for_status()
        page = response.json()
        if not isinstance(page, list) or not page:
            raise ValueError(f"Binance returned no candles while data is missing at {cursor}")
        for candle in page:
            timestamp = int(candle[0]) // 1000
            if timestamp != cursor or timestamp >= end_timestamp or int(candle[6]) >= end_timestamp * 1000:
                raise ValueError(f"Incomplete or non-contiguous Binance candles: expected {cursor}, got {timestamp}")
            records.append(candle)
            cursor += HOUR
    return records


def validate_indicator_alignment(hook, cutoff):
    """Prevent publication until all eligible source rows have matching indicators."""
    row = hook.get_first(f"""
        SELECT
          (SELECT MAX(UNIX_TIMESTAMP) FROM BITCOIN_DATA.DATA.BTC_HOURLY_DATA WHERE UNIX_TIMESTAMP < {int(cutoff)}),
          (SELECT MAX(UNIX_TIMESTAMP) FROM BITCOIN_DATA.DATA.HOURLY_TA WHERE UNIX_TIMESTAMP < {int(cutoff)}),
          (SELECT COUNT(*) FROM BITCOIN_DATA.DATA.BTC_HOURLY_DATA s
           LEFT JOIN BITCOIN_DATA.DATA.HOURLY_TA t ON s.UNIX_TIMESTAMP = t.UNIX_TIMESTAMP
           WHERE s.UNIX_TIMESTAMP >= (SELECT MIN(UNIX_TIMESTAMP) FROM BITCOIN_DATA.DATA.HOURLY_TA)
             AND s.UNIX_TIMESTAMP < {int(cutoff)} AND t.UNIX_TIMESTAMP IS NULL)
    """)
    if not row or row[0] is None or row[1] != row[0] or row[2]:
        raise ValueError(f"OHLCV and indicators are not ready for publication: {row}")
    return int(row[0])
