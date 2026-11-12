"""Exercise resource exhaustion detection and safe recovery of executor evidence."""

import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest


ROOT = Path(__file__).resolve().parents[1]


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


guard = load_script("check_file_descriptors")
recovery = load_script("recover_executor_logs")


class FileLimitTests(unittest.TestCase):
    def test_detects_exhausted_idle_worker(self):
        with TemporaryDirectory() as folder:
            process = Path(folder) / "123456789"
            (process / "fd").mkdir(parents=True)
            (process / "limits").write_text("Max open files            10                   10                   files\n")
            for fd in range(10):
                (process / "fd" / str(fd)).touch()
            self.assertEqual(len(guard.check_processes(Path(folder))), 1)
            for fd in range(5):
                (process / "fd" / str(fd)).unlink()
            self.assertEqual(guard.check_processes(Path(folder)), [])

    def test_process_exiting_during_health_check_is_harmless(self):
        with TemporaryDirectory() as folder:
            (Path(folder) / "123456789").mkdir()
            self.assertEqual(guard.check_processes(Path(folder)), [])


class RecoveryTests(unittest.TestCase):
    def failure(self, run_id="scheduled__2026-11-11T01:00:00+00:00", event_time="01:20:21"):
        return [
            "airflow-scheduler-1 | [2026-11-11T01:20:21.009+0000] {local_executor.py:96} ERROR - uhoh\n",
            "airflow-scheduler-1 | Traceback (most recent call last):\n",
            "airflow-scheduler-1 | OSError: [Errno 24] Too many open files\n",
            f"airflow-scheduler-1 | [2026-11-11T{event_time}.019+0000] {{scheduler_job_runner.py:810}} INFO - Received executor event with state failed for task instance TaskInstanceKey(dag_id='bitcoin_news', task_id='fetch_finnhub_news', run_id='{run_id}', try_number=3, map_index=-1)\n",
        ]

    def test_recovers_original_executor_error_for_correct_attempt(self):
        entries = list(recovery.recoveries(self.failure()))
        self.assertEqual(len(entries), 1)
        path, entry = entries[0]
        self.assertIn("task_id=fetch_finnhub_news/attempt=3.log", str(path))
        self.assertIn("Recovered from", entry["event"])
        self.assertIn("OSError: [Errno 24]", entry["event"])

    def test_does_not_attribute_an_old_exception_to_a_later_task(self):
        self.assertEqual(list(recovery.recoveries(self.failure(event_time="01:21:22"))), [])

    def test_rejects_paths_outside_log_directory(self):
        self.assertEqual(list(recovery.recoveries(self.failure(run_id="../../outside"))), [])


if __name__ == "__main__":
    unittest.main()
