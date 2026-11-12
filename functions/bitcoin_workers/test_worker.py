"""Tests run in the actual Lambda image before CodeBuild publishes it."""

from datetime import datetime
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import worker


class CalculationTests(unittest.TestCase):
    @patch.object(worker, 'aligned_watermark', return_value=1794441600)
    @patch.object(worker, 'merge_records', side_effect=lambda session, frame: len(frame))
    @patch.object(worker, 'add_technical_indicators_talib', side_effect=lambda frame: frame)
    def test_repairs_include_existing_rows_after_the_gap(self, calculate, merge, validate):
        session = MagicMock()
        session.get_first.return_value = (1794441600,)
        start, through = 1794009600, 1794441600
        session.get_records.return_value = [(ts, 1., 2., 1.5, .5, 15.)
            for ts in range(start - 1000 * worker.HOUR, through + worker.HOUR, worker.HOUR)]
        result = worker.calculate(session, {'start_timestamp': start, 'through_timestamp': through})
        self.assertEqual(result['processed_records'], 121)
        self.assertEqual(result['through_timestamp'], through)
        self.assertEqual(int(merge.call_args.args[1]['UNIX_TIMESTAMP'].min()), start)

    def test_incomplete_indicators_block_publication(self):
        session = MagicMock()
        session.get_first.side_effect = [(1794441600,), (1794009600,), (20,)]
        with self.assertRaises(ValueError):
            worker.aligned_watermark(session, 1794445200)

    def test_unsupported_actions_do_not_read_secrets(self):
        with patch.object(worker, 'secret') as secret, self.assertRaises(ValueError):
            worker.handler({'action': 'unknown'}, None)
        secret.assert_not_called()


class PublicationTests(unittest.TestCase):
    @patch.object(worker, 'aligned_watermark', return_value=1794009600)
    @patch.object(worker, 'secret')
    def test_stale_snapshot_cannot_satisfy_a_newer_asset(self, secret, watermark):
        with self.assertRaisesRegex(ValueError, 'upstream asset watermark'):
            worker.publish(MagicMock(), {'through_timestamp': 1794441600})
        secret.assert_not_called()

    @patch.object(worker, 'aligned_watermark', return_value=1794441600)
    @patch.object(worker, 'secret', return_value={'username': 'example', 'repository': 'dataset', 'token': 'test-only', 'email': 'test@example.com'})
    def test_lfs_objects_are_uploaded_before_the_git_ref(self, secret, watermark):
        session = MagicMock()
        session.dataframe.side_effect = [
            pd.DataFrame({'UNIX_TIMESTAMP': [1794441600], 'DATETIME': [datetime(2026, 11, 12)], 'CLOSE': [1.]}),
            pd.DataFrame({'UNIX_TIMESTAMP': [1794441600], 'DATETIME': ['2026-11-12 00:00:00'], 'CLOSE': [1.]})]
        calls = []
        def git(args, cwd, env):
            calls.append(args)
            self.assertNotIn('test-only', ' '.join(args))
            if args[0] == 'clone':
                worker.Path(args[-1]).mkdir()
            if args[:2] == ['status', '--porcelain']:
                return ' M bitcoin-hourly-ohlcv.csv'
            return 'abc123' if args[:2] == ['rev-parse', 'HEAD'] else ''
        with patch.object(worker, 'git', side_effect=git), patch.dict(worker.os.environ, {'GITHUB_SECRET_ARN': 'test-secret'}):
            result = worker.publish(session, {})
        self.assertTrue(result['success'])
        self.assertEqual(result['commit_sha'], 'abc123')
        self.assertLess(calls.index(['lfs', 'push', 'origin', 'HEAD']), calls.index(['push', 'origin', 'HEAD:main']))


if __name__ == '__main__':
    unittest.main()
