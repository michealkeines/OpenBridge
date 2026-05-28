---
name: openbridge-build
description: Library reference for authoring new pool-based skills on top of openbridge. Use this when you're writing a producer that publishes work items into a named pool via `Bridge.ask()`. The pool can be served by any number of Claude-session workers in parallel; each ask() is an independent work item with its own scratch file, claimed atomically by one worker via Redis BRPOP. Not for operators driving an existing pool — for that, load `openbridge` instead.
---

# openbridge-build — author a new pool producer

This is the library reference for authors. If you're driving an existing
pool as a worker, load the **openbridge** skill, not this one.

## What you write

A normal `async def main():` that calls `await bridge.ask(...)` to
publish work into a pool. Each ask() is one work item, served by one
worker. Concurrent ask()s fan out across workers in parallel.

```python
import asyncio
# Install OpenBridge first:  `pip install -e .` from the OpenBridge repo root
from openbridge import Bridge

bridge = Bridge(name="my-producer", pool="my-pool")
# name = this producer's unique ID (for `openbridge list`, history)
# pool = the work-distribution group workers consume from

async def process(item):
    result = await bridge.ask(
        item_id=item["id"],
        prompt=render_prompt(item),
        template={"summary": "", "scope": ""},
        validate=lambda d: "summary required" if not d.get("summary") else None,
    )
    if result.skipped:
        ...
    else:
        apply(item, result.data)

async def main():
    items = discover_items()
    progress = bridge.checkpoint("progress", default={"done": []})
    pending = [i for i in items if i["id"] not in progress["done"]]
    # Fan out across the worker pool — N items processed in parallel
    sem = asyncio.Semaphore(5)
    async def gated(item):
        async with sem:
            await process(item)
            progress["done"].append(item["id"])
            bridge.save("progress", progress)
    await asyncio.gather(*(gated(i) for i in pending))

if __name__ == "__main__":
    bridge.serve(main())
```

The runtime handles: scratch submission files per work_id, Redis queue + claim + result lists, validation re-prompt loop, producer/worker registration in pool sets, history.jsonl, state.json.

## API

### `Bridge(name, pool, *, redis_url=None, workdir=None)`

- `name` — this producer's unique identity. Used in `openbridge list`, history, state.json. Multiple producers with different names can publish into the same pool concurrently.
- `pool` — the work pool to publish into. Workers consume from this pool. Pool names namespace everything in Redis — two projects with different pool names cannot collide.
- `redis_url` — defaults to `$REDIS_URL` or `redis://localhost:6379/0`. Auto-bootstraps a Docker container if no Redis is up (opt out with `OPENBRIDGE_NO_DOCKER=1`).
- `workdir` — defaults to `<cwd>/.<name>/`. Each producer has its own workdir for state.json / history.jsonl / scratch submission files.

### `await bridge.ask(*, item_id, prompt, template=None, validate=None, max_validation_retries=5)`

Publish a work item into the pool, block until a worker submits or skips. Returns `AskResult(data, skipped, skip_reason)`.

- `item_id` — stable string for this item (shown in status output + history).
- `prompt` — the text the worker sees on `openbridge get`.
- `template` — pre-filled JSON the worker edits in the per-work scratch file. Defaults to `{}`.
- `validate(data) -> str | None` — return `None` to accept, a string error to reject. On rejection the work item is re-published with the error appended and the previous draft kept as the new template, so the worker (same or different one) can fix in place.
- `max_validation_retries` — after this many rejections raise `RuntimeError`. Default 5.

Each `ask()` generates a fresh UUID `work_id` and a scratch file at `<workdir>/work/<work_id>.json`. Concurrent ask()s are independent — each blocks on its own Redis result list, so they fan out across workers without interfering.

### `bridge.checkpoint(key, default=None)` / `bridge.save(key, data)`

Read/write JSON files in the workdir for crash recovery. Library never auto-saves — call `save()` whenever you reach a durable point (typically after each `ask()` returns). Files land at `<workdir>/checkpoint_<key>.json`.

### `bridge.serve(coro)`

Wrap your `main()`. Acquires the singleton lock (30s heartbeat-refreshed TTL), clears stale Redis queues, registers the workdir pointer, runs the coroutine, releases everything on exit.

## Architecture

Per-work-item routing through pool-keyed Redis lists.

```
┌─ Producer A (pool=foo) ─┐    ┌─ Producer B (pool=foo) ─┐
│  await ask(item_X)      │    │  await ask(item_Y)      │
│    ► writes scratch     │    │    ► writes scratch     │
│    ► LPUSH queue        │    │    ► LPUSH queue        │
│    ► BRPOP result:X     │    │    ► BRPOP result:Y     │
└─────────┬───────────────┘    └─────────┬───────────────┘
          │                              │
          ▼                              ▼
   ┌─ pool "foo" queue ────────────────────────────────────┐
   │  [work_id_X, work_id_Y]                               │
   └────────┬──────────────────┬───────────────────────────┘
            │ BRPOP+claim      │ BRPOP+claim
            ▼                  ▼
       ┌─ Worker 1 ─┐     ┌─ Worker 2 ─┐
       │ bridge get │     │ bridge get │
       │ edit scratch     │ edit scratch
       │ bridge submit ───┘ bridge submit
       └──────┬─────┘     └──────┬─────┘
              │                  │
              ▼                  ▼
       LPUSH result:X     LPUSH result:Y
              │                  │
              └──────────────────┴── wakes the right producer's await
```

Workers are stateless; producers are the durable side.

## Workdir layout (per producer)

```
<workdir>/
├── state.json          producer session: name, pool, status, owner, started_at,
│                       last_published_work_id, last_published_item_id
├── history.jsonl       append-only: submitted / skipped / validation_failed
├── work/<work_id>.json scratch submission — one per ask() call;
│                       worker edits in place; producer reads on result
└── checkpoint_*.json   author-driven via bridge.save()
```

Scratch files are not auto-cleaned. Old ones accumulate but are tiny;
GC by `rm -rf <workdir>/work/` between runs if you care.

## Redis keys (per pool)

```
openbridge:pool:<pool>:queue              LIST work_ids — FIFO backlog
openbridge:pool:<pool>:work:<work_id>     HASH payload (prompt, item_id, submission_path, producer_id, attempt, created_at)
openbridge:pool:<pool>:claim:<work_id>    STR worker_id, TTL 300s
openbridge:pool:<pool>:result:<work_id>   LIST BLPOP target for the producer's await
openbridge:pool:<pool>:producers          SET of active producer IDs
openbridge:pool:<pool>:workers            SET of active worker IDs (currently unused; per-claim tracking is the source of truth)
```

## Multi-step per item

Want classify-then-write? Just call `ask()` twice:

```python
for item in items:
    label = (await bridge.ask(
        item_id=f"{item.id}#classify",
        prompt=f"Classify {item.name}",
        template={"label": ""},
        validate=lambda d: None if d.get("label") in {"critical","normal","skip"} else "bad label",
    )).data["label"]

    if label == "skip":
        continue

    body = (await bridge.ask(
        item_id=f"{item.id}#write",
        prompt=f"You classified {item.name} as {label}. Now write a "
               f"{'detailed' if label=='critical' else 'short'} summary.",
        template={"summary": ""},
    )).data["summary"]

    save(item, label, body)
```

No state machine, no extra subcommands. Just Python.

## Multi-pool on one machine

Each pool is namespaced. Run as many pools as you want concurrently; producers and workers are kept apart by pool name.

```bash
openbridge list                        # all pools + summary
openbridge status --pool foo           # producers, workers, queue depth, in-flight
openbridge get --pool foo              # claim next work from pool foo
```

**Multiple workers per pool is the supported model.** Two or three Claude sessions can `bridge get --pool foo` in parallel; each gets a different work item via atomic BRPOP-claim. No collisions, no driver lock needed.

**Multiple producers per pool is also supported.** Two producer processes both publishing into `pool="foo"` share the same queue. Workers don't care which producer published a given item — they just process it and submit. The producer's `await` wakes on its own `result:<work_id>` list regardless of which worker handled it.

## Useful patterns

### Resume via the deliverable file

The cleanest resume mechanism is making the deliverable file double as the queue source. At startup, your `main()` walks all candidate items, filters out ones already present in the deliverable, and only `ask()`s the remainder. A daemon restart picks up exactly where it left off without any extra state tracking.

```python
async def main():
    items = discover_items()
    output = load_deliverable_if_exists()
    pending = [i for i in items if i["id"] not in output]
    for item in pending:
        result = await bridge.ask(...)
        output[item["id"]] = build_entry(item, result)
        save_deliverable(output)   # save after every item
```

### Progress prints

`bridge.ask()` blocks silently. Print one line to stderr per item transition so the operator's terminal isn't empty for hours:

```python
import time, sys
for idx, item in enumerate(pending):
    print(f"[my-skill] [{idx+1}/{len(pending)}] {item['id']} ⏳",
          file=sys.stderr, flush=True)
    t0 = time.monotonic()
    result = await bridge.ask(...)
    dt = time.monotonic() - t0
    print(f"[my-skill] [{idx+1}/{len(pending)}] {item['id']} "
          f"{'⊘ skipped' if result.skipped else '✓ done'} ({dt:.0f}s)",
          file=sys.stderr, flush=True)
```

### Validation that helps Claude fix in place

When validation rejects, the previous submission is kept as the template, so write rules that point at specific fields:

```python
def validate(data: dict) -> str | None:
    if not (data.get("summary") or "").strip():
        return "submission.summary is empty"
    if len(data["summary"]) < 80:
        return f"summary is {len(data['summary'])} chars; need ≥ 80"
    if data.get("scope") not in {"regional", "global", "per-account", "per-resource"}:
        return f"scope must be one of regional|global|per-account|per-resource, got {data.get('scope')!r}"
    return None
```

## Auto-bootstrap

Both the library and the operator CLI call `redis_runtime.ensure_redis()` before any Redis op. If port 6379 isn't open, it runs `docker run -d --name openbridge-redis -p 127.0.0.1:6379:6379 redis:7-alpine` (idempotent — reuses existing container if present). Opt out via `OPENBRIDGE_NO_DOCKER=1`.

## Dependencies

- `redis>=4.2` (installed via `pip install -e .` from the OpenBridge repo, or `pip install openbridge` once published).
- `docker` on `$PATH` for the Redis container (skip with `OPENBRIDGE_NO_DOCKER=1`; bring your own Redis via `REDIS_URL`).
- Python 3.10+ (uses `str | None` unions).

## Files in the OpenBridge repository

```
openbridge/
├── __init__.py              exposes Bridge, AskResult
├── __main__.py              python -m openbridge entry point
├── bridge.py                Bridge class + ask/checkpoint/save/serve
├── cli.py                   operator CLI (get/submit/skip/list/...)
└── redis_runtime.py         Docker bootstrap (ensure_redis / stop_redis)

skills/
├── openbridge/SKILL.md      operator skill (drive a pool)
└── openbridge-build/SKILL.md  this file (build a producer)

examples/word_classifier.py  ~30-line working example
```

## Gotchas

1. **Concurrent ask()s are supported and encouraged.** Use `asyncio.gather` (with a semaphore for backpressure) to fan out items across the worker pool. Each ask() is independent.
2. **Crash recovery uses checkpoints + deliverable.** Producer's in-memory state is lost on restart. Persist via `bridge.save()` whenever you reach a durable point, or make the deliverable file double as the resume source.
3. **You own the resume logic.** Watch your data shapes — if `skipped` is `[{"id":...}]`, you can't do `if id in progress["skipped"]`. Extract the IDs first.
4. **Stale claims are not auto-recycled.** A worker that crashes mid-edit leaves a claim with a TTL (300s). After expiry, the work item is orphaned in `work:<work_id>` but not back on the queue. Operator action: `bridge reclaim --pool X --work-id WID --force`. A future watcher process could automate this.
5. **Scratch files accumulate.** Each ask() writes `<workdir>/work/<work_id>.json`. The library never deletes them (postmortem value). If your producer runs for a long time, sweep periodically.
6. **No producer singleton lock.** The old "one daemon per skill name" lock is gone — multiple producers can run with the same name (though confusing). Use unique names per producer if you run them in parallel.
