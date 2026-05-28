"""Pattern 7 — Resume from disk.

The deliverable file is the source of truth for "what's done." On every
producer start the loop re-walks all items and skips ones already in the
deliverable. Crash, kill -9, machine reboot — none of it matters. Workers
are spawned automatically.

Run:
    python examples/07_resume.py

^C the producer mid-run, then re-run: already-done items are skipped,
only pending ones are re-published.

The library also exposes `bridge.checkpoint(key)` / `bridge.save(key,
data)` for the same pattern — used here so the example is self-contained
(no external file path required).
"""
from openbridge import Bridge
from openbridge.spawn import spawn_workers

ITEMS = [f"item-{n:02d}" for n in range(10)]

bridge = Bridge(name="resumable", pool="resumable")


async def main() -> None:
    # `checkpoint` reads <workdir>/checkpoint_progress.json (or returns the
    # default). On a fresh run the file doesn't exist yet → default kicks in.
    progress = bridge.checkpoint("progress", default={"done": {}, "skipped": []})
    skipped_ids = {s["id"] for s in progress["skipped"]}

    todo = [i for i in ITEMS
            if i not in progress["done"] and i not in skipped_ids]

    print(f"resuming: {len(progress['done'])} done, "
          f"{len(skipped_ids)} skipped, {len(todo)} pending")

    async with spawn_workers(bridge, count=2):
        for item in todo:
            r = await bridge.ask(
                item_id=item,
                prompt=f"Process {item}. Set submission.result to anything.",
                template={"item": item, "result": ""},
            )
            if r.skipped:
                progress["skipped"].append({"id": item, "reason": r.skip_reason})
            else:
                progress["done"][item] = r.data
            # Save after EVERY item so a crash never loses more than the
            # one in-flight item (which the reaper will requeue anyway).
            bridge.save("progress", progress)

    print(f"final — done={len(progress['done'])} "
          f"skipped={len(progress['skipped'])}")


if __name__ == "__main__":
    bridge.serve(main())
