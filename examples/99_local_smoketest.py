"""Local smoketest — exercises the full producer/worker round-trip in-process.

A real deployment splits the producer (Python `Bridge`) from the worker
(a Claude session driving the `openbridge` CLI). For a quick sanity check
on one machine, this script does both: it spins up a background task that
plays the worker role by talking to Redis directly — claiming each work
item, filling in the submission JSON, and pushing a "submit" signal — so
the producer's `await bridge.ask()` can complete end-to-end.

Prereqs:
    python3 -m venv .venv && .venv/bin/pip install -e .
    docker run -d --name openbridge-redis -p 127.0.0.1:6379:6379 redis:7-alpine

Run:
    .venv/bin/python examples/99_local_smoketest.py
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import redis.asyncio as aioredis

from openbridge import Bridge

POOL = "smoketest"
WORDS = ["aurora", "lament", "swift", "harbor"]
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")

# Skip the Docker auto-bootstrap in redis_runtime — we manage Redis ourselves.
os.environ["OPENBRIDGE_NO_DOCKER"] = "1"

bridge = Bridge(name="smoketest-producer", pool=POOL, redis_url=REDIS_URL)


async def auto_worker(stop: asyncio.Event) -> None:
    """Stand-in for a Claude session: claim work, fill the template, submit."""
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    queue_key = f"openbridge:pool:{POOL}:queue"

    while not stop.is_set():
        # BRPOP with a short timeout so we can notice the stop signal.
        popped = await r.brpop([queue_key], timeout=1)
        if popped is None:
            continue
        _, work_id = popped

        work_key = f"openbridge:pool:{POOL}:work:{work_id}"
        raw = await r.hgetall(work_key)
        if not raw:
            continue
        work = {k: json.loads(v) for k, v in raw.items()}

        # Claim it (matches what `openbridge get` does).
        await r.set(f"openbridge:pool:{POOL}:claim:{work_id}",
                    "auto-worker", ex=300)

        sub_path = Path(work["submission_path"])
        data = json.loads(sub_path.read_text())
        # "Classify" by length — toy logic so we have a deterministic answer.
        data["part_of_speech"] = "noun" if len(work["item_id"]) % 2 == 0 else "verb"
        sub_path.write_text(json.dumps(data, indent=2) + "\n")

        # Signal the producer (matches what `openbridge submit` does).
        await r.lpush(
            f"openbridge:pool:{POOL}:result:{work_id}",
            json.dumps({"work_id": work_id, "kind": "submit",
                        "worker_id": "auto-worker"}),
        )

    await r.aclose()


async def producer() -> None:
    for word in WORDS:
        result = await bridge.ask(
            item_id=word,
            prompt=f"Classify {word!r}.",
            template={"word": word, "part_of_speech": ""},
        )
        print(f"  {word}: {result.data.get('part_of_speech')}")
    print(f"done — {len(WORDS)} items round-tripped")


async def main() -> None:
    stop = asyncio.Event()
    worker_task = asyncio.create_task(auto_worker(stop))
    try:
        await producer()
    finally:
        stop.set()
        await worker_task


if __name__ == "__main__":
    bridge.serve(main())
