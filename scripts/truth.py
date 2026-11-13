#!/usr/bin/env python3
"""Is the record true? Holds what the executor reports against what the instruments did.

    uv run python scripts/truth.py              # every run there is
    uv run python scripts/truth.py --settled    # and nothing may still be going on

What the instruments did comes from their own status (ports 7101-7103) and the
stored data from object storage, never from the executor. The record is read
twice: steps over HTTP, as the acceptance check reads them, and every attempt
at every step over GraphQL. Exits non-zero if anything in it is not true.
"""

import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

EXEC = os.getenv("EXEC", "http://localhost:5002")
S3 = os.getenv("S3", "http://localhost:8343")
INSTRUMENTS = os.getenv(
    "INSTRUMENTS", "http://localhost:7101 http://localhost:7102 http://localhost:7103"
).split()

MAX_ATTEMPTS = 3
CLOCK_TOLERANCE = 0.002
ENDED = ("completed", "failed", "cancelled")
LOST = "the instrument completed, but its measurement was lost before it could be stored"

ATTEMPTS = """query ($id: ID!) { run(id: $id) { steps { attempts {
    jobId status startedAt finishedAt error data { sha256 } } } } }"""
# What an attempt may be called, for each way its execution can stand.
TOLD_AS = {
    "running": ("RUNNING",),
    "completed": ("COMPLETED",),
    "failed": ("FAILED",),
    "hung": ("FAILED",),
}

RULES = {
    "completed": "a step the record calls completed, completed on its instrument",
    "failed": "a step the record calls failed, failed or hung on its instrument",
    "untouched": "a step the record calls pending or cancelled never reached an instrument",
    "running": "a step the record calls running is running on its instrument",
    "times": "the record gives the times the instrument gives",
    "attempts": "every execution an instrument made is an attempt on record, and the same",
    "once": "nothing ran again unless its instrument failed and called that safe",
    "order": "nothing started before what it depends on had completed",
    "data": "stored data is what the instrument measured",
    "ended": "a run that has ended has nothing left open, and says when it ended",
    "settled": "nothing is still going on",
    "device": "an instrument did one thing at a time, and only for runs on record",
}


def fetch(url: str, body: dict[str, Any] | None = None) -> Any:
    request = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def attempts_of(run_id: str) -> dict[str, dict[str, Any]]:
    """Every attempt at every step of a run, by the job id its instrument knows."""
    reply = fetch(f"{EXEC}/graphql", {"query": ATTEMPTS, "variables": {"id": run_id}})
    steps = reply["data"]["run"]["steps"]
    return {attempt["jobId"]: attempt for step in steps for attempt in step["attempts"]}


def epoch(moment: str | None) -> float | None:
    return None if moment is None else datetime.fromisoformat(moment).timestamp()


def stored_sha256(data_ref: str) -> str | None:
    try:
        with urllib.request.urlopen(f"{S3}/{data_ref.removeprefix('s3://')}", timeout=10) as body:
            return hashlib.sha256(body.read()).hexdigest()
    except urllib.error.URLError:
        return None


def main() -> int:
    settled = "--settled" in sys.argv[1:]
    runs = fetch(f"{EXEC}/runs")
    instruments = [fetch(f"{url}/status") for url in INSTRUMENTS]
    retryable = {i["device_id"]: i["failures_retryable"] for i in instruments}

    executions: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for instrument in instruments:
        for execution in sorted(instrument["executions"], key=lambda e: e["started_at"]):
            executions[execution["run_id"], execution["step_name"]].append(execution)

    wrong: dict[str, list[str]] = {rule: [] for rule in RULES}
    attempts: dict[str, dict[str, Any]] = {}

    for run in runs:
        attempts.update(attempts_of(run["id"]))
        steps = {step["name"]: step for step in run["steps"]}
        over = run["status"] in ENDED
        if over != (run["finished_at"] is not None):
            wrong["ended"].append(f"{run['id']} is {run['status']}, ended at {run['finished_at']}")
        if settled and run["status"] == "running":
            wrong["settled"].append(f"{run['id']} is still running")

        for step in steps.values():
            where = f"{run['id']} {step['name']}"
            ran = executions[run["id"], step["name"]]
            last = ran[-1] if ran else None
            outcome = last["outcome"] if last else "nothing"
            status = step["status"]

            if over and status in ("pending", "running"):
                wrong["ended"].append(f"{where} is {status} in a run that is {run['status']}")
            if status == "completed" and outcome != "completed":
                wrong["completed"].append(f"{where}: the instrument says {outcome}")
            if status == "failed" and outcome not in ("failed", "hung"):
                wrong["failed"].append(f"{where}: the instrument says {outcome} ({step['error']})")
            if status == "cancelled" and ran:
                wrong["untouched"].append(f"{where} is cancelled, the instrument says {outcome}")
            if status == "pending" and outcome in ("running", "completed") and settled:
                wrong["untouched"].append(f"{where} is pending, the instrument says {outcome}")
            if status == "running" and outcome != "running" and settled:
                wrong["running"].append(f"{where}: the instrument says {outcome}")

            if last and status in ("running", "completed", "failed"):
                for mine, theirs in (("started_at", "started_at"), ("finished_at", "finished_at")):
                    told, real = epoch(step[mine]), last[theirs]
                    if status == "running" and mine == "finished_at":
                        continue
                    if told is None or real is None or abs(told - real) > CLOCK_TOLERANCE:
                        wrong["times"].append(f"{where} {mine}: record {told}, instrument {real}")

            if len(ran) > MAX_ATTEMPTS:
                wrong["once"].append(f"{where} ran {len(ran)} times")
            for earlier in ran[:-1]:
                # The one case where a completed job is run again: its
                # measurement never reached storage, and the record says so.
                lost = attempts.get(earlier["job_id"], {}).get("error") == LOST
                failed = earlier["outcome"] in ("failed", "hung") or lost
                if not failed or not retryable[step["device_id"]]:
                    wrong["once"].append(
                        f"{where} ran again after {earlier['outcome']} on {step['device_id']}"
                    )

            for execution in ran:
                for dependency in step["depends_on"]:
                    done = [
                        e["finished_at"]
                        for e in executions[run["id"], dependency]
                        if e["outcome"] == "completed"
                    ]
                    if not done or min(done) > execution["started_at"]:
                        wrong["order"].append(f"{where} started before {dependency} completed")

            measured = last["data_sha256"] if last else None
            if status == "completed" and measured:
                ref = step["data_ref"]
                found = stored_sha256(ref) if ref else None
                if found != measured:
                    wrong["data"].append(f"{where}: {ref} holds {found}, measured {measured}")
            elif step["data_ref"]:
                wrong["data"].append(f"{where} is {status} and points at {step['data_ref']}")

    made = {e["job_id"]: e for ran in executions.values() for e in ran}
    for job_id, execution in made.items():
        attempt = attempts.get(job_id)
        if attempt is None:
            wrong["attempts"].append(f"{job_id} ran, and is not on record")
            continue
        lost = execution["outcome"] == "completed" and attempt["error"] == LOST
        if attempt["status"] not in TOLD_AS[execution["outcome"]] and not lost:
            behind = attempt["status"] in ("PENDING", "RUNNING") and not settled
            if not behind:
                wrong["attempts"].append(
                    f"{job_id}: record {attempt['status']}, instrument {execution['outcome']}"
                )
            continue
        for mine, theirs in (("startedAt", "started_at"), ("finishedAt", "finished_at")):
            told, real = epoch(attempt[mine]), execution[theirs]
            if (told is None) != (real is None) or (told and abs(told - real) > CLOCK_TOLERANCE):
                wrong["attempts"].append(f"{job_id} {mine}: record {told}, instrument {real}")
        measured = execution["data_sha256"] if attempt["status"] == "COMPLETED" else None
        if (attempt["data"] or {}).get("sha256") != measured:
            wrong["attempts"].append(f"{job_id}: data on record is not what was measured")
    for job_id, attempt in attempts.items():
        if job_id not in made and attempt["status"] not in ("PENDING", "CANCELLED"):
            wrong["attempts"].append(f"{job_id} is {attempt['status']}, and never ran")

    known = {run["id"] for run in runs}
    for instrument in instruments:
        timeline = sorted(instrument["executions"], key=lambda e: e["started_at"])
        for execution in timeline:
            if execution["run_id"] not in known:
                wrong["device"].append(f"{instrument['device_id']} ran {execution['job_id']}")
        for before, after in zip(timeline, timeline[1:], strict=False):
            if before["finished_at"] is None or before["finished_at"] > after["started_at"]:
                wrong["device"].append(
                    f"{instrument['device_id']} ran {after['job_id']} during {before['job_id']}"
                )
        if settled and instrument["busy"]:
            wrong["settled"].append(f"{instrument['device_id']} is still busy")

    statuses = Counter(run["status"] for run in runs)
    total = sum(len(ran) for ran in executions.values())
    print(f"{len(runs)} runs ({', '.join(f'{n} {s}' for s, n in sorted(statuses.items()))})")
    print(f"{total} executions on {len(instruments)} instruments\n")
    for rule, description in RULES.items():
        if rule == "settled" and not settled:
            continue
        print(f"  {'FAIL' if wrong[rule] else 'PASS'}  {description}")
        for detail in wrong[rule]:
            print(f"          {detail}")

    failed = sum(1 for rule in RULES if wrong[rule])
    print(f"\n{'The record is true.' if not failed else f'{failed} rule(s) broken.'}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
