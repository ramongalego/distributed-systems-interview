"""Core NATS: one side answers requests, the other asks.

    uv run python examples/core_request_reply.py

Runs both sides in one process against the stack's NATS server. Nothing here
is part of the system; it only shows the client API.
"""

import asyncio
import os

import nats
import nats.errors
from nats.aio.msg import Msg

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4242")
SUBJECT = "examples.greet"


async def main() -> None:
    nc = await nats.connect(NATS_URL)

    # The answering side: every message on SUBJECT gets a reply.
    async def on_request(msg: Msg) -> None:
        name = msg.data.decode()
        await msg.respond(f"hello, {name}".encode())

    sub = await nc.subscribe(SUBJECT, cb=on_request)

    # The asking side: send a request and wait for one reply.
    reply = await nc.request(SUBJECT, b"plate reader", timeout=2)
    print("reply:", reply.data.decode())

    # With nobody subscribed, a request fails straight away.
    await sub.unsubscribe()
    try:
        await nc.request(SUBJECT, b"anyone?", timeout=2)
    except nats.errors.NoRespondersError:
        print("no responders")

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
