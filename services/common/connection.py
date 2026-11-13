"""How both sides connect to NATS."""

import logging
from typing import Any

log = logging.getLogger(__name__)


async def _on_error(error: Exception) -> None:
    """One line, where the client would log a stack trace per reconnect attempt."""
    log.warning("nats: %r", error)


# Reconnect for ever. And ping often: a partition leaves a connection that
# looks open and carries nothing, and by default that goes unnoticed for
# minutes. This way it is dropped, and reconnected, within seconds.
NATS: dict[str, Any] = {
    "max_reconnect_attempts": -1,
    "reconnect_time_wait": 1,
    "ping_interval": 3,
    "max_outstanding_pings": 2,
    "error_cb": _on_error,
}
