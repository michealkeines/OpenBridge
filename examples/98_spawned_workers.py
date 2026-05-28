"""End-to-end test of `spawn_workers` — real claude subprocesses doing work.

Producer publishes a handful of word-classification tasks. Two claude
subprocesses are spawned automatically (each loads the bundled openbridge
skill) and consume the pool in parallel. The producer awaits each result.

Prereqs:
    python3 -m venv .venv && .venv/bin/pip install -e .
    docker run -d --name openbridge-redis -p 127.0.0.1:6379:6379 redis:7-alpine
    claude CLI on PATH (or $OPENBRIDGE_CLAUDE_BIN set)

Run:
    .venv/bin/python examples/98_spawned_workers.py
"""
from __future__ import annotations

import asyncio
import os

from openbridge import Bridge
from openbridge.spawn import spawn_workers

POOL = "spawn-smoketest"
WORDS = ["aurora", "lament", "swift", "harbor"]
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")

os.environ["OPENBRIDGE_NO_DOCKER"] = "1"

bridge = Bridge(name="spawn-producer", pool=POOL, redis_url=REDIS_URL)


async def main() -> None:
    async with spawn_workers(bridge, count=2):
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
        print(f"done — {len(WORDS)} items via spawned claude workers")


if __name__ == "__main__":
    bridge.serve(main())
