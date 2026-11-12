"""Guard against the task-log descriptor leak that exhausted LocalExecutor workers."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

from airflow.sdk.execution_time import supervisor


class SupervisorFileHandleTests(unittest.TestCase):
    def open_probe_logs(self, directory):
        remaining = []
        for descriptor in Path("/proc/self/fd").iterdir():
            try:
                if str(directory) in str(descriptor.readlink()):
                    remaining.append(str(descriptor))
            except FileNotFoundError:
                pass
        return remaining

    def exercise(self, directory, start):
        with patch.object(supervisor.ActivitySubprocess, "start", start), patch.object(
            supervisor, "ensure_secrets_backend_loaded"
        ), patch("airflow.sdk.execution_time.secrets_masker.reset_secrets_masker"):
            for number in range(12):
                try:
                    supervisor.supervise(
                        ti=MagicMock(),
                        bundle_info=MagicMock(),
                        dag_rel_path="unused.py",
                        token="unused",
                        client=MagicMock(),
                        log_path=str(Path(directory) / f"attempt-{number}.log"),
                    )
                except RuntimeError as exc:
                    if str(exc) != "simulated process startup failure":
                        raise

    def test_completed_tasks_release_log_file_descriptors(self):
        process = MagicMock()
        process.wait.return_value = 0
        process.final_state = "success"
        with TemporaryDirectory() as directory:
            self.exercise(directory, MagicMock(return_value=process))
            self.assertEqual(self.open_probe_logs(directory), [])

    def test_startup_failure_also_releases_log_file_descriptors(self):
        with TemporaryDirectory() as directory:
            self.exercise(directory, MagicMock(side_effect=RuntimeError("simulated process startup failure")))
            self.assertEqual(self.open_probe_logs(directory), [])

    def test_logging_setup_failure_releases_its_open_file(self):
        with TemporaryDirectory() as directory, patch(
            "airflow.sdk.log.logging_processors", side_effect=RuntimeError("simulated process startup failure")
        ):
            self.exercise(directory, MagicMock())
            self.assertEqual(self.open_probe_logs(directory), [])

    def test_task_wait_failure_releases_log_file_descriptors(self):
        process = MagicMock()
        process.wait.side_effect = RuntimeError("simulated process startup failure")
        with TemporaryDirectory() as directory:
            self.exercise(directory, MagicMock(return_value=process))
            self.assertEqual(self.open_probe_logs(directory), [])


if __name__ == "__main__":
    unittest.main()
