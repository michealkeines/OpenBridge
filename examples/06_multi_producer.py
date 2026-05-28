"""Pattern 6 — Multi-producer, one pool.

Two separate producer processes, each with a unique `name`, share a pool
named `shared`. Workers see one combined queue.

Run two producers in different terminals, each with a different shard:

    python examples/06_multi_producer.py shard-a
    python examples/06_multi_producer.py shard-b

Drive the shared pool from worker(s):

    openbridge list                       # see both producers, one queue
    openbridge get --pool shared

Items from both shards interleave in the workers' view. Each producer's
`await` resumes when its specific item is submitted — they never see
each other's results.
"""
import sys
from openbridge import Bridge

SHARD_ITEMS = {
    "shard-a": ["apple", "ant", "arrow", "azure", "axel"],
    "shard-b": ["banana", "bramble", "byte", "boreal", "buoy"],
}


def run(shard: str) -> None:
    if shard not in SHARD_ITEMS:
        print(f"unknown shard: {shard}; pick one of {list(SHARD_ITEMS)}")
        sys.exit(2)

    # Each producer has a UNIQUE name; both write to the SAME pool.
    # Each producer also gets its own workdir (default: ./.<name>/) so
    # state.json / history.jsonl / scratch files don't collide.
    bridge = Bridge(name=shard, pool="shared")

    async def main():
        for word in SHARD_ITEMS[shard]:
            r = await bridge.ask(
                item_id=f"{shard}/{word}",
                prompt=(
                    f"Producer {shard!r} asks: classify {word!r} as a "
                    "part of speech."
                ),
                template={"word": word, "shard": shard, "part_of_speech": ""},
            )
            label = r.data.get("part_of_speech") if not r.skipped else "skip"
            print(f"[{shard}] {word}: {label}")

    bridge.serve(main())


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"usage: python 06_multi_producer.py {'|'.join(SHARD_ITEMS)}")
        sys.exit(2)
    run(sys.argv[1])
