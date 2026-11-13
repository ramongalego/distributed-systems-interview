#!/usr/bin/env python3
"""Keeps runs going while something else breaks things, then waits for quiet.

    uv run python scripts/load.py --for 60 --every 4 --cancel 5

Starts a run every few seconds, alternating between the workflows, and cancels
every Nth one shortly after it starts. A request the executor cannot take is
skipped: it may be down, that is the point. Follow with scripts/truth.py.
"""

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from typing import Any

EXEC = os.getenv("EXEC", "http://localhost:5002")


def call(method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    request = urllib.request.Request(
        f"{EXEC}{path}",
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.load(response)
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--for", dest="seconds", type=float, default=60)
    parser.add_argument("--every", type=float, default=4)
    parser.add_argument("--cancel", type=int, default=0, help="cancel every Nth run")
    parser.add_argument("--settle", type=float, default=180)
    args = parser.parse_args()

    workflows = [w["name"] for w in call("GET", "/workflows") or []]
    started = cancelled = 0
    deadline = time.monotonic() + args.seconds
    while time.monotonic() < deadline:
        run = call("POST", "/runs", {"workflow_name": workflows[started % len(workflows)]})
        if run and call("POST", f"/runs/{run['id']}/start"):
            started += 1
            if args.cancel and started % args.cancel == 0:
                time.sleep(1.5)
                cancelled += call("POST", f"/runs/{run['id']}/cancel") is not None
        time.sleep(args.every)
    print(f"started {started} runs, cancelled {cancelled}")

    settle_by = time.monotonic() + args.settle
    while time.monotonic() < settle_by:
        runs = call("GET", "/runs")
        if runs is not None and not any(run["status"] == "running" for run in runs):
            print("quiet")
            return
        time.sleep(1)
    print(f"still not quiet after {args.settle:g}s")


if __name__ == "__main__":
    main()
