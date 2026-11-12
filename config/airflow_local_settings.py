"""Task logging for the shared-volume LocalExecutor deployment."""

from copy import deepcopy
from datetime import datetime, timezone
import json

from airflow.config_templates.airflow_local_settings import (
    DEFAULT_LOGGING_CONFIG,
    DEFAULT_REMOTE_CONN_ID,
    REMOTE_TASK_LOG,
)
from airflow.utils.log.file_task_handler import FileTaskHandler


class ReliableFileTaskHandler(FileTaskHandler):
    """Explain missing worker logs without requesting an invalid, hostless URL."""

    @staticmethod
    def _read_from_local(worker_log_path):
        paths = sorted(worker_log_path.parent.glob(worker_log_path.name + "*"))

        def read_lines(path):
            with path.open(encoding="utf-8") as stream:
                yield from stream

        return [str(path) for path in paths], [read_lines(path) for path in paths]

    def _read_from_logs_server(self, ti, worker_log_rel_path):
        if ti.hostname or ti.triggerer_job:
            return super()._read_from_logs_server(ti, worker_log_rel_path)

        if ti.state == "upstream_failed":
            message = "This task did not run because an upstream task failed. Open the failed upstream task's logs."
        elif ti.state == "skipped":
            message = "This task was skipped; no worker log was created."
        elif ti.start_date is None:
            message = (
                "This task never started, so no worker log or hostname was recorded. "
                "Check the airflow-scheduler container logs for the executor startup error "
                f"for dag_id={ti.dag_id}, task_id={ti.task_id}, run_id={ti.run_id}."
            )
        else:
            message = (
                "No worker hostname was recorded and no task log was found in the shared log volume. "
                "Check the airflow-scheduler container logs for this task attempt."
            )

        entry = json.dumps(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "error" if ti.state in ("failed", "upstream_failed") else "info",
                "event": message,
            }
        )
        # Airflow 3.0.6 consumes one iterator of lines per log source.
        return ["No worker hostname recorded; worker log server was not contacted."], [iter((entry,))]


LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
LOGGING_CONFIG["handlers"]["task"]["class"] = "airflow_local_settings.ReliableFileTaskHandler"
