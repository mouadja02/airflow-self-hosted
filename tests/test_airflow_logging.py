"""Regression coverage for hostless task logs, shared logs, and executor evidence."""

import json
import io
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

# Boot Airflow before importing its configured logging module directly, matching
# normal CLI/API startup and avoiding a partially initialized config module.
import airflow  # noqa: F401
from airflow_local_settings import ReliableFileTaskHandler


class TaskLogTests(unittest.TestCase):
    def setUp(self):
        self.handler = ReliableFileTaskHandler(base_log_folder="/tmp")
        self.ti = SimpleNamespace(
            hostname="", triggerer_job=None, state="failed", start_date=None,
            dag_id="example", task_id="task", run_id="manual__example",
        )

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    def test_failed_before_start_does_not_request_hostless_url(self, fetch):
        sources, logs = self.handler._read_from_logs_server(self.ti, "attempt=1.log")
        fetch.assert_not_called()
        entry = json.loads(next(logs[0]))
        self.assertIn("never started", entry["event"])
        self.assertIn("airflow-scheduler", entry["event"])

    def test_upstream_failure_explains_why_no_log_exists(self):
        self.ti.state = "upstream_failed"
        _, logs = self.handler._read_from_logs_server(self.ti, "attempt=1.log")
        self.assertIn("upstream task failed", json.loads(next(logs[0]))["event"])

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    def test_missing_log_returns_a_finished_readable_ui_response(self, fetch):
        with TemporaryDirectory() as folder:
            self.handler.local_base = folder
            self.handler._render_filename = lambda ti, attempt: "missing.log"
            self.handler._read_remote_logs = lambda *args: ([], [])
            self.ti.try_number = 1
            logs, metadata = self.handler.read(self.ti, 1)
            fetch.assert_not_called()
            self.assertTrue(any("never started" in str(log) for log in logs))
            self.assertTrue(metadata["end_of_log"])

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    def test_recorded_hostname_uses_normal_log_server(self, fetch):
        self.ti.hostname = "airflow-scheduler"
        content = b'{"event":"worker output"}\n'
        fetch.return_value.status_code = 200
        fetch.return_value.headers = {"Content-Length": str(len(content))}
        fetch.return_value.raw = io.BytesIO(content)
        _, logs = self.handler._read_from_logs_server(self.ti, "attempt=1.log")
        self.assertTrue(fetch.call_args.args[0].startswith("http://airflow-scheduler:8793/"))
        self.assertIn("worker output", "\n".join(logs[0]))

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    def test_completed_log_is_read_from_shared_volume_without_worker(self, fetch):
        with TemporaryDirectory() as folder:
            path = Path(folder) / "attempt=1.log"
            path.write_text('{"timestamp":"2026-11-12T00:00:00Z","level":"info","event":"durable output"}\n')
            self.handler.local_base = folder
            self.handler._render_filename = lambda ti, attempt: "attempt=1.log"
            self.handler._read_remote_logs = lambda *args: ([], [])
            self.ti.try_number = 1
            logs, metadata = self.handler.read(self.ti, 1)
            fetch.assert_not_called()
            self.assertTrue(any("durable output" in str(log) for log in logs))
            self.assertTrue(metadata["end_of_log"])


if __name__ == "__main__":
    unittest.main()
