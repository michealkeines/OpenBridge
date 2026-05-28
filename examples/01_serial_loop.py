"""Pattern 1 — Serial loop.

The simplest possible OpenBridge producer: a for-loop with one
`await bridge.ask()` per item. One auto-spawned worker, one in-flight
item at a time.

Run:
    pip install -e .                              # from repo root
    python examples/01_serial_loop.py             # producer + worker
"""
from openbridge import Bridge
from openbridge.spawn import spawn_workers

WORDS = ["aurora", "lament", "swift", "harbor", "candid", "ponder"]

bridge = Bridge(name="word-classifier", pool="word-classifier")


async def main() -> None:
    async with spawn_workers(bridge, count=1):
        for word in WORDS:
            result = await bridge.ask(
                item_id=word,
                prompt=(
                    f"Classify the part of speech of {word!r}.\n"
                    f"Edit submission.json: set `part_of_speech` to "
                    f"noun / verb / adjective / adverb / other."
                ),
                template={"word": word, "part_of_speech": ""},
            )
            if result.skipped:
                print(f"  {word}: skipped ({result.skip_reason})")
            else:
                print(f"  {word}: {result.data.get('part_of_speech')}")

        print(f"done — {len(WORDS)} items processed sequentially")


if __name__ == "__main__":
    bridge.serve(main())
