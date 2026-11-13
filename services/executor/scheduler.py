"""Decides what should start next. Given, and working.

A pure function: it does no I/O and keeps no state. You call it with the steps
of one run as they are now, and it tells you which to start. When and how often
you call it, and what you do with the answer, is up to you.
"""

from collections.abc import Sequence
from dataclasses import dataclass

from .models import STEP_COMPLETED, STEP_PENDING, STEP_RUNNING


@dataclass(frozen=True)
class StepState:
    name: str
    device_id: str
    depends_on: list[str]
    status: str


def next_steps(steps: Sequence[StepState]) -> list[str]:
    """The names of the steps that should start now, in workflow order.

    A step should start when it is pending, everything it depends on has
    completed, and its device is not already running a step of this run. A
    device does one thing at a time, so at most one step per device is
    returned.
    """
    completed = {s.name for s in steps if s.status == STEP_COMPLETED}
    occupied = {s.device_id for s in steps if s.status == STEP_RUNNING}

    chosen: list[str] = []
    for step in steps:
        if step.status != STEP_PENDING or step.device_id in occupied:
            continue
        if all(dep in completed for dep in step.depends_on):
            chosen.append(step.name)
            occupied.add(step.device_id)
    return chosen
