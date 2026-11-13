"""JetStream: publish to a stream, read it back through a durable consumer.

    uv run python examples/jetstream_pub_sub.py

Runs both sides in one process against the stack's NATS server. Nothing here
is part of the system; it only shows the client API.
"""

import asyncio
import os

import nats
import nats.errors

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4242")
STREAM = "EXAMPLES"
SUBJECT = "examples.readings"


async def main() -> None:
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()

    # A stream stores every message published to its subjects.
    await js.add_stream(name=STREAM, subjects=["examples.>"])

    # Publishing to a stream returns once the server has stored the message.
    for value in ("0.41", "0.52", "0.38"):
        ack = await js.publish(SUBJECT, value.encode())
        print(f"stored {value} as message {ack.seq}")

    # A durable consumer remembers what it has been given, across restarts.
    sub = await js.pull_subscribe(SUBJECT, durable="example-reader", stream=STREAM)
    try:
        msgs = await sub.fetch(10, timeout=2)
    except nats.errors.TimeoutError:
        msgs = []
    for msg in msgs:
        print("read:", msg.data.decode())
        await msg.ack()

    # Clean up, so running it again starts from nothing.
    await js.delete_stream(STREAM)
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
