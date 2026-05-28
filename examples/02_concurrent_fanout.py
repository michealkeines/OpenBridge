"""Pattern 2 — Concurrent fan-out.

The producer issues N concurrent ask() calls via asyncio.gather. A
semaphore caps in-flight work items so the pool doesn't grow unbounded.
N claude worker sessions are spawned automatically and cooperatively
drain the queue.

Run:
    python examples/02_concurrent_fanout.py
"""
import asyncio
from openbridge import Bridge
from openbridge.spawn import spawn_workers

WORDS = ["aurora", "lament", "swift", "harbor", "candid", "ponder",
         "bramble", "torpid", "verdant", "nadir"]
CONCURRENCY = 5

bridge = Bridge(name="fanout", pool="fanout")


async def main() -> None:
    sem = asyncio.Semaphore(CONCURRENCY)
    results: dict[str, str] = {}
    lock = asyncio.Lock()

    async def process(word: str) -> None:
        async with sem:
            r = await bridge.ask(
                item_id=word,
                prompt=f"Classify {word!r} as a part of speech.",
                template={"word": word, "part_of_speech": ""},
            )
            async with lock:
                results[word] = (r.data.get("part_of_speech")
                                 if not r.skipped else "(skipped)")

    async with spawn_workers(bridge, count=CONCURRENCY):
        # Gather collects all tasks; return_exceptions keeps one failure
        # from cancelling siblings mid-flight.
        await asyncio.gather(
            *(process(w) for w in WORDS),
            return_exceptions=True,
        )

    print(f"done — {len(results)} items processed across "
          f"up to {CONCURRENCY} concurrent workers")
    for w in WORDS:
        print(f"  {w}: {results.get(w, '(missing)')}")


if __name__ == "__main__":
    bridge.serve(main())
