"""Make task-log cleanup exception-safe in the pinned Airflow Task SDK 1.0.6.

The upstream normal-completion fix in Airflow 3.0.3 does not cover failures in
logging setup, secrets loading, process startup, or process.wait(). Refuse to
patch a different SDK or source layout so future upgrades require review.
"""

from importlib.metadata import distribution
from pathlib import Path
from textwrap import indent


def patch_supervisor():
    package = distribution("apache-airflow-task-sdk")
    if package.version != "1.0.6":
        raise RuntimeError(f"Review the logging cleanup patch for Task SDK {package.version}")
    source = Path(package.locate_file("airflow/sdk/execution_time/supervisor.py"))
    text = source.read_text()

    configure_before = '''    with _remote_logging_conn(client):
        processors = logging_processors(enable_pretty_log=pretty_logs)[0]
    logger = structlog.wrap_logger(underlying_logger, processors=processors, logger_name="task").bind()

    return logger, log_file_descriptor
'''
    configure_after = '''    try:
        with _remote_logging_conn(client):
            processors = logging_processors(enable_pretty_log=pretty_logs)[0]
        logger = structlog.wrap_logger(underlying_logger, processors=processors, logger_name="task").bind()
        return logger, log_file_descriptor
    except BaseException:
        log_file_descriptor.close()
        raise
'''
    supervise_before = '''    backends = ensure_secrets_backend_loaded()
    log.info(
        "Secrets backends loaded for worker",
        count=len(backends),
        backend_classes=[type(b).__name__ for b in backends],
    )

    reset_secrets_masker()

    process = ActivitySubprocess.start(
        dag_rel_path=dag_rel_path,
        what=ti,
        client=client,
        logger=logger,
        bundle_info=bundle_info,
        subprocess_logs_to_stdout=subprocess_logs_to_stdout,
    )

    exit_code = process.wait()
    end = time.monotonic()
    log.info("Task finished", exit_code=exit_code, duration=end - start, final_state=process.final_state)
    if log_path and log_file_descriptor:
        log_file_descriptor.close()
    return exit_code
'''
    body = supervise_before.replace(
        "    if log_path and log_file_descriptor:\n        log_file_descriptor.close()\n", ""
    )
    supervise_after = (
        "    try:\n" + indent(body, "    ")
        + "    finally:\n        if log_file_descriptor is not None:\n            log_file_descriptor.close()\n"
    )
    for before, after in ((configure_before, configure_after), (supervise_before, supervise_after)):
        if text.count(before) != 1:
            raise RuntimeError("Task SDK source changed; review the logging cleanup patch before building")
        text = text.replace(before, after, 1)
    compile(text, str(source), "exec")
    source.write_text(text)
    print("Applied exception-safe task-log cleanup to Airflow Task SDK 1.0.6")


if __name__ == "__main__":
    patch_supervisor()
