"""Fail container health checks before a process exhausts its open-file limit."""

from pathlib import Path
import os
import sys


def check_processes(proc_root=Path("/proc"), threshold=0.8):
    problems = []
    for process in proc_root.iterdir():
        if not process.name.isdigit() or process.name == str(os.getpid()):
            continue
        try:
            # Other users' processes can be unreadable inside a container.
            if process.stat().st_uid != os.getuid():
                continue
            limit_line = next(
                line for line in (process / "limits").read_text().splitlines()
                if line.startswith("Max open files")
            )
            soft_limit = limit_line.split()[3]
            if soft_limit == "unlimited":
                continue
            limit = int(soft_limit)
            count = len(list((process / "fd").iterdir()))
            if count >= limit * threshold:
                problems.append(f"PID {process.name}: {count}/{limit} file descriptors open")
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            # Processes may exit between listing /proc and reading their files.
            continue
    return problems


if __name__ == "__main__":
    problems = check_processes()
    if problems:
        print("File descriptor pressure: " + "; ".join(problems), file=sys.stderr)
        sys.exit(1)
