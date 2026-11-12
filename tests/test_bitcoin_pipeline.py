"""Regression tests for outage catchup, gap repairs, and publication ordering."""

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import bitcoin_pipeline_helpers as helpers
import bitcoin_ohlcv_dataset as price
import technical_indicators_dag as technical
import btc_dataset_github_sync_dag as publisher
import pandas as pd


def candle(timestamp):
    return [timestamp * 1000, "1", "2", "0.5", "1.5", "10", (timestamp + 3600) * 1000 - 1, "15"]


class PaginationTests(unittest.TestCase):
    def test_outage_larger_than_api_limit_fetches_every_page(self):
        start = 1794009600
        end = start + 1105 * helpers.HOUR
        calls = []

        def get(url, params, timeout):
            calls.append(params)
            first = params['startTime'] // 1000
            last = min(first + 1000 * helpers.HOUR, end)
            response = MagicMock()
            response.json.return_value = [candle(ts) for ts in range(first, last, helpers.HOUR)]
            return response

        rows = helpers.fetch_hourly_klines(start, end, get)
        self.assertEqual(len(rows), 1105)
        self.assertEqual(rows[-1][0] // 1000, end - helpers.HOUR)
        self.assertEqual(calls[1]['startTime'], (start + 1000 * helpers.HOUR) * 1000)

    def test_empty_response_with_unfilled_hours_fails(self):
        response = MagicMock()
        response.json.return_value = []
        with self.assertRaisesRegex(ValueError, 'missing'):
            helpers.fetch_hourly_klines(1794009600, 1794013200, MagicMock(return_value=response))

    def test_missing_hour_is_not_silently_accepted(self):
        response = MagicMock()
        response.json.return_value = [candle(1794009600), candle(1794016800)]
        with self.assertRaisesRegex(ValueError, 'non-contiguous'):
            helpers.fetch_hourly_klines(1794009600, 1794020400, MagicMock(return_value=response))

    def test_open_candle_is_rejected(self):
        response = MagicMock()
        row = candle(1794009600)
        row[6] = 1794013200000
        response.json.return_value = [row]
        with self.assertRaisesRegex(ValueError, 'Incomplete'):
            helpers.fetch_hourly_klines(1794009600, 1794013200, MagicMock(return_value=response))


class PriceRepairTests(unittest.TestCase):
    @patch.object(price, 'fetch_hourly_klines', return_value=[candle(1794009600)])
    @patch.object(price, 'closed_hour_cutoff', return_value=1794445200)
    @patch.object(price, 'SnowflakeHook')
    def test_repair_can_start_before_the_current_maximum(self, hook, cutoff, fetch):
        hook.return_value.get_first.return_value = (1794441600,)
        ti = MagicMock()
        price.fetch_btc_data(task_instance=ti, dag_run=SimpleNamespace(conf={'repair_from_timestamp': 1794009600}))
        fetch.assert_called_once_with(1794009600, 1794445200)

    @patch.object(price, 'SnowflakeHook')
    def test_incomplete_merge_does_not_publish_an_asset(self, hook):
        hook.return_value.get_first.return_value = (4, 4, 1794092400)
        ti = MagicMock()
        ti.xcom_pull.return_value = {'start_timestamp': 1794009600, 'through_timestamp': 1794092400}
        event = SimpleNamespace(extra=None)
        outlets = MagicMock()
        outlets.__getitem__.return_value = event
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            price.validate_price_data(task_instance=ti, outlet_events=outlets)
        self.assertIsNone(event.extra)


class IndicatorRepairTests(unittest.TestCase):
    @patch.object(technical, 'SnowflakeHook')
    def test_interior_gap_is_repaired_even_when_watermarks_match(self, hook):
        hook.return_value.get_first.side_effect = [(1000,), (1720,), (1794441600,), (1794441600,), (1794013200,)]
        ti = MagicMock()
        branch = technical.check_table_status(ti=ti)
        self.assertEqual(branch, 'process_delta_data')
        ti.xcom_push.assert_any_call(key='calculation_start', value=1794013200)

    @patch.object(technical, 'merge_records', side_effect=lambda hook, df: len(df))
    @patch.object(technical, 'add_technical_indicators_talib', side_effect=lambda df: df)
    @patch.object(technical, 'closed_hour_cutoff', return_value=1798441600)
    @patch.object(technical, 'SnowflakeHook')
    def test_recalculation_covers_long_backlog_and_existing_repaired_rows(self, hook, cutoff, calculate, merge):
        start = 1794009600
        rows = [(ts, 1., 2., 1.5, .5, 10.) for ts in range(start - 1000 * helpers.HOUR, start + 1105 * helpers.HOUR, helpers.HOUR)]
        hook.return_value.get_records.return_value = rows
        ti = MagicMock()
        ti.xcom_pull.side_effect = lambda key, task_ids: start + 1000 * helpers.HOUR if key == 'latest_ta_timestamp' else start
        count = technical.process_delta_data(ti=ti)
        self.assertEqual(count, 1105)
        merged = pd.concat([call.args[1] for call in merge.call_args_list])
        self.assertEqual(int(merged['UNIX_TIMESTAMP'].min()), start)
        self.assertEqual(int(merged['UNIX_TIMESTAMP'].max()), start + 1104 * helpers.HOUR)


class PublicationTests(unittest.TestCase):
    def test_dags_follow_successful_upstream_assets(self):
        self.assertEqual([asset.uri for asset in technical.dag.schedule], [helpers.PRICE_ASSET.uri])
        self.assertEqual([asset.uri for asset in publisher.dag.schedule], [helpers.INDICATORS_ASSET.uri])
        self.assertIn(helpers.PRICE_ASSET, price.dag.get_task('validate_price_data').outlets)
        self.assertIn(helpers.INDICATORS_ASSET, technical.dag.get_task('validate_indicators').outlets)

    def test_stale_or_missing_indicators_block_export(self):
        for summary in [(1794441600, 1794009600, 0), (1794441600, 1794441600, 20), (None, None, 0)]:
            with self.subTest(summary=summary), self.assertRaises(ValueError):
                hook = MagicMock()
                hook.get_first.return_value = summary
                helpers.validate_indicator_alignment(hook, 1794445200)

    def test_matching_complete_tables_are_ready(self):
        hook = MagicMock()
        hook.get_first.return_value = (1794441600, 1794441600, 0)
        self.assertEqual(helpers.validate_indicator_alignment(hook, 1794445200), 1794441600)

    def test_asset_repair_ranges_are_preserved(self):
        events = [SimpleNamespace(extra={'start_timestamp': 1794009600, 'through_timestamp': 1794441600}),
                  SimpleNamespace(extra={'start_timestamp': 1794355200, 'through_timestamp': 1794445200})]
        result = helpers.asset_update_range({'triggering_asset_events': {helpers.PRICE_ASSET.uri: events}})
        self.assertEqual(result, {'start_timestamp': 1794009600, 'through_timestamp': 1794445200})


if __name__ == '__main__':
    unittest.main()
