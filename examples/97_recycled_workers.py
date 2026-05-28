"""Recycled workers — each item runs in a fresh Claude session.

With `recycle=True`, every work item is processed in its own short-lived
Claude subprocess: claim → do the work → submit → exit. A supervisor task
spawns a replacement immediately so the pool stays at `count` workers.
The context window never grows past one item's worth, so compaction is
impossible.

Trade-off: each item pays ~5–10s of Claude startup. Useful for
long-running batches where context cleanliness matters more than
per-item latency.

Prereqs:
    python3 -m venv .venv && .venv/bin/pip install -e .
    docker run -d --name openbridge-redis -p 127.0.0.1:6379:6379 redis:7-alpine

Run:
    .venv/bin/python examples/97_recycled_workers.py
"""
from __future__ import annotations

import asyncio
import os

from openbridge import Bridge
from openbridge.spawn import spawn_workers

POOL = "recycle-smoketest"
WORDS = ["aurora", "lament", "swift", "harbor"]
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")

os.environ["OPENBRIDGE_NO_DOCKER"] = "1"

bridge = Bridge(name="recycle-producer", pool=POOL, redis_url=REDIS_URL)


async def main() -> None:
    async with spawn_workers(bridge, count=2, recycle=True,
                             max_jobs_per_session=2):
        async def one(word: str):
            result = await bridge.ask(
                item_id=word,
                prompt=(
                    f"Classify the part of speech of {word!r}.\n"
                    "Edit the submission.json named in the footer: set "
                    "`part_of_speech` to one of: noun, verb, adjective, "
                    "adverb, other. Then submit."
                ),
                template={"word": word, "part_of_speech": ""},
            )
            print(f"  {word}: {result.data.get('part_of_speech')}")

        await asyncio.gather(*(one(w) for w in WORDS))
        print(f"done — {len(WORDS)} items via recycled claude workers")


if __name__ == "__main__":
    bridge.serve(main())
