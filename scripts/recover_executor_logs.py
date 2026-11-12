"""Recover pre-start executor failures into missing task logs for the Airflow UI.

Usage: python scripts/recover_executor_logs.py scheduler.log --log-dir PATH [--write]
Existing nonempty task logs are never replaced. No task metadata is changed.
"""

import argparse
import ast
from collections import deque
import json
from pathlib import Path
import re


FAILURE = re.compile(r"Received executor event with state failed for task instance (TaskInstanceKey\(.*\))")
TIMESTAMP = re.compile(r"\[(\d{4}-\d{2}-\d{2}T[^\]]+)\]")


def recoveries(lines):
    history = deque(maxlen=80)
    for line in lines:
        history.append(line.rstrip())
        match = FAILURE.search(line)
        if not match:
            continue
        timestamp = TIMESTAMP.search(line)
        if timestamp is None:
            continue
        preceding = list(history)[:-1]
        starts = [i for i, entry in enumerate(preceding) if "{local_executor.py:" in entry and "ERROR - uhoh" in entry]
        if not starts:
            continue
        traceback = preceding[starts[-1]:]
        if not any("OSError: [Errno 24] Too many open files" in entry for entry in traceback):
            continue
        # Only associate an immediately preceding executor failure (within the same second).
        error_time = TIMESTAMP.search(traceback[0])
        if error_time is None or error_time[1][:19] != timestamp[1][:19]:
            continue
        call = ast.parse(match[1], mode="eval").body
        if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name) or call.func.id != "TaskInstanceKey":
            continue
        key = {kw.arg: ast.literal_eval(kw.value) for kw in call.keywords}
        if not all(isinstance(key.get(field), str) and not any(c in key[field] for c in ("/", "\\", "\0")) for field in ("dag_id", "task_id", "run_id")):
            continue
        attempt, map_index = key.get("try_number"), key.get("map_index", -1)
        if not isinstance(attempt, int) or attempt < 1 or not isinstance(map_index, int):
            continue
        path = Path(f"dag_id={key['dag_id']}") / f"run_id={key['run_id']}" / f"task_id={key['task_id']}"
        if map_index >= 0:
            path /= f"map_index={map_index}"
        path /= f"attempt={attempt}.log"
        content = {
            "timestamp": timestamp[1],
            "level": "error",
            "event": (
                "Recovered from the airflow-scheduler container log. This task attempt failed "
                "before starting; no original worker log or hostname was created. "
                "Cause: OSError: [Errno 24] Too many open files. "
                "The LocalExecutor worker exhausted its open-file limit.\n\n"
                + "\n".join(entry.split(" | ", 1)[-1] for entry in traceback)
            ),
            "logger": "airflow.executor.recovered",
        }
        yield path, content


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scheduler_log", type=Path)
    parser.add_argument("--log-dir", type=Path, required=True)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    recovered = 0
    with args.scheduler_log.open() as source:
        for relative, content in recoveries(source):
            target = args.log_dir / relative
            if target.exists() and target.stat().st_size:
                continue
            if args.write:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(json.dumps(content) + "\n")
            print(("Recovered " if args.write else "Would recover ") + str(relative))
            recovered += 1
    print(f"{recovered} task attempt logs {'recovered' if args.write else 'eligible'}.")


if __name__ == "__main__":
    main()
