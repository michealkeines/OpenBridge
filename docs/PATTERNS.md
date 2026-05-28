# Patterns

Eight patterns, each with a runnable example under `examples/`. Pick the
one closest to your workload, copy it, adapt.

| # | Pattern | When to use | Example |
|---|---|---|---|
| 1 | Serial loop | Simplest — one item at a time, one worker | [`01_serial_loop.py`](../examples/01_serial_loop.py) |
| 2 | Concurrent fan-out | Many items, want N-way parallelism via N worker sessions | [`02_concurrent_fanout.py`](../examples/02_concurrent_fanout.py) |
| 3 | Multi-step per item | One item needs two or more Claude turns (classify → write) | [`03_multi_step.py`](../examples/03_multi_step.py) |
| 4 | Validation reprompt | Worker submissions need automated quality enforcement | [`04_validation.py`](../examples/04_validation.py) |
| 5 | Stage-to-stage pipeline | Stage A's output is the queue for stage B | [`05_pipeline.py`](../examples/05_pipeline.py) |
| 6 | Multi-producer one pool | Several Python scripts feeding a shared worker pool | [`06_multi_producer.py`](../examples/06_multi_producer.py) |
| 7 | Resume from disk | Long runs that must survive producer restarts | [`07_resume.py`](../examples/07_resume.py) |
| 8 | Recycled workers (fresh session per item) | Long batches where session context bloat would force compaction | [`97_recycled_workers.py`](../examples/97_recycled_workers.py) |

Patterns combine — a real skill might fan out, validate, recycle, AND resume.

## Workers come from where?

Every pattern below ships a producer that drives a pool. Workers can be
supplied in two ways:

1. **Auto-spawned via `spawn_workers`** — the producer launches `claude`
   subprocesses internally. This is what every numbered example does
   now; no second terminal required. Uses your Claude subscription
   (never API billing) and runs `--dangerously-skip-permissions` so
   sessions are fully autonomous.
2. **Manually** — you run `openbridge get / submit / skip` from your
   own interactive Claude Code sessions or shell scripts. Same pool, no
   coordination required; both modes can coexist.

The snippets below show the `spawn_workers` form. To run manually
instead, just remove the `async with spawn_workers(...)` wrapper and
drive the pool yourself.

---

## Pattern 1 — Serial loop

**Shape:** one for-loop, one `await ask()` per item, one worker.

**When to use:** prototype; small batches; tasks where strict ordering
matters more than throughput.

```python
from openbridge import Bridge
from openbridge.spawn import spawn_workers
bridge = Bridge(name="serial", pool="serial")

async def main():
    async with spawn_workers(bridge, count=1):
        for word in ["aurora", "lament", "ponder"]:
            r = await bridge.ask(
                item_id=word,
                prompt=f"Classify {word!r} as noun/verb/adjective.",
                template={"word": word, "part_of_speech": ""},
            )
            print(f"{word} → {r.data['part_of_speech']}")

bridge.serve(main())
```

The single spawned worker processes items one at a time; the producer
exits cleanly when the loop finishes.

**See:** [`examples/01_serial_loop.py`](../examples/01_serial_loop.py)

---

## Pattern 2 — Concurrent fan-out

**Shape:** `asyncio.gather` with a semaphore, multiple workers
consuming in parallel.

**When to use:** large batches where each item is independent and you
have multiple Claude sessions available as workers.

```python
import asyncio
from openbridge import Bridge
from openbridge.spawn import spawn_workers
bridge = Bridge(name="fanout", pool="fanout")

async def main():
    items = [f"item-{i}" for i in range(20)]
    sem = asyncio.Semaphore(5)               # max 5 in flight

    async def one(item):
        async with sem:
            return await bridge.ask(
                item_id=item, prompt=f"do {item}",
                template={"item": item, "result": ""},
            )

    async with spawn_workers(bridge, count=5):   # 5 claude sessions in parallel
        results = await asyncio.gather(*(one(i) for i in items))
        print(f"got {len(results)} results")

bridge.serve(main())
```

The five spawned workers each claim a different item via atomic BRPOP;
the semaphore caps in-flight items so the producer doesn't outrun the
workers.

**Tradeoff:** the semaphore caps in-flight items. Set it ≥ your worker
count or workers will sit idle waiting for new publishes.

**See:** [`examples/02_concurrent_fanout.py`](../examples/02_concurrent_fanout.py)

---

## Pattern 3 — Multi-step per item

**Shape:** two or more `await ask()` calls per logical item. Output of
the first feeds the second's prompt.

**When to use:** the item needs Claude to *think then write* — classify
then summarise, plan then execute, draft then revise.

```python
from openbridge import Bridge
bridge = Bridge(name="multistep", pool="multistep")

async def process(topic):
    # Step 1: classify
    classification = (await bridge.ask(
        item_id=f"{topic}#classify",
        prompt=f"Classify {topic!r}: simple/complex/skip?",
        template={"label": ""},
    )).data["label"]

    if classification == "skip":
        return

    # Step 2: write — prompt depends on step 1
    detail = "detailed" if classification == "complex" else "short"
    body = (await bridge.ask(
        item_id=f"{topic}#write",
        prompt=f"You classified {topic!r} as {classification}. "
               f"Write a {detail} summary.",
        template={"summary": ""},
    )).data["summary"]

    return body
```

**Why this works:** each ask() is an independent work item with its own
work_id. Workers don't know one item produced both — they just process
them as separate requests.

**Tradeoff:** if the per-item flow has 5 steps × 100 items = 500 work
items. Visible in `openbridge status` as longer queues.

**See:** [`examples/03_multi_step.py`](../examples/03_multi_step.py)

---

## Pattern 4 — Validation reprompt

**Shape:** pass `validate=...` to `bridge.ask`. Bad submissions are
automatically republished with the error appended.

**When to use:** the worker's submission has measurable correctness
criteria (length, schema, allowed values, regex match).

```python
from openbridge import Bridge
bridge = Bridge(name="validated", pool="validated")

VALID_POS = {"noun", "verb", "adjective", "adverb"}

def validate(data):
    pos = (data.get("part_of_speech") or "").strip().lower()
    if not pos:
        return "part_of_speech is required"
    if pos not in VALID_POS:
        return f"part_of_speech must be one of {sorted(VALID_POS)}, got {pos!r}"
    if len(data.get("notes", "")) < 20:
        return "notes must be at least 20 characters"
    return None  # accept

async def main():
    for word in ["aurora", "lament"]:
        result = await bridge.ask(
            item_id=word,
            prompt=f"Classify {word!r}",
            template={"word": word, "part_of_speech": "", "notes": ""},
            validate=validate,
            max_validation_retries=3,   # raises RuntimeError after this
        )
        print(word, result.data)
```

**On rejection**, the worker sees the prompt again with a
`=== PREVIOUS SUBMISSION REJECTED ===` block and their previous draft
as the new template — they can fix in place. The library catches any
exception inside `validate()` and treats it as a rejection too.

**Tradeoff:** retries cost one full worker round-trip each. Pick a
realistic `max_validation_retries` (default 5) so a stuck item doesn't
loop forever.

**See:** [`examples/04_validation.py`](../examples/04_validation.py)

---

## Pattern 5 — Stage-to-stage pipeline

**Shape:** two (or more) producers, two pools. Stage 1's submissions
become stage 2's input.

**When to use:** complex pipelines where each stage has a distinct
prompt + validation. Common: structured-extraction → prose-summary →
apply-to-source.

```python
# stage1.py — extracts structured facts
bridge_s1 = Bridge(name="extractor", pool="stage1")
async def main_s1():
    for doc in DOCUMENTS:
        r = await bridge_s1.ask(
            item_id=doc.id,
            prompt=f"Extract structured facts from {doc!r}",
            template={"facts": {}},
        )
        save_facts(doc.id, r.data["facts"])    # writes to a shared file/db

# stage2.py — composes prose from facts
bridge_s2 = Bridge(name="composer", pool="stage2")
async def main_s2():
    for doc_id, facts in load_completed_facts():
        r = await bridge_s2.ask(
            item_id=doc_id,
            prompt=f"Compose a customer-facing summary from {facts!r}",
            template={"summary": ""},
        )
        save_summary(doc_id, r.data["summary"])
```

Run them in different terminals; drive each pool with its own set of
worker sessions. Stage 2 can start as soon as stage 1 has produced any
completed facts — they can run concurrently.

**Why use separate pools (not just multi-step):** stages may need
different worker skills (e.g. extraction wants a developer mindset,
prose summary wants a customer-empathy mindset). Different pool name =
different operator skill loaded in the worker session.

**See:** [`examples/05_pipeline.py`](../examples/05_pipeline.py)

---

## Pattern 6 — Multi-producer one pool

**Shape:** N producer processes, each with a unique `name`, all
publishing into the same `pool`. Workers see them as one queue.

**When to use:** sharding (shard A handles items 1-1000, shard B handles
items 1001-2000) where each shard is a separate process — e.g. one per
machine, one per dataset partition.

```python
# producer for shard A
bridge = Bridge(name="shard-a", pool="shared")
async def main():
    for item in items_for_shard_a():
        await bridge.ask(item_id=item.id, prompt=..., template=...)
bridge.serve(main())

# producer for shard B (separate process)
bridge = Bridge(name="shard-b", pool="shared")   # same pool!
async def main():
    for item in items_for_shard_b():
        await bridge.ask(item_id=item.id, prompt=..., template=...)
bridge.serve(main())
```

`openbridge status --pool shared` will show both producers; workers
draining the pool serve both transparently.

**Tradeoff:** every producer needs a unique `name` AND a unique workdir
(default `<cwd>/.<name>/`). Same `name` + same workdir = two producers
writing to the same `state.json` / `history.jsonl` (race).

**See:** [`examples/06_multi_producer.py`](../examples/06_multi_producer.py)

---

## Pattern 7 — Resume from disk

**Shape:** the producer checks a deliverable file before each ask() and
skips items already complete.

**When to use:** **always for any non-trivial run**. Long jobs crash for
all sorts of reasons (process killed, machine reboot, Redis container
restart). The deliverable file is your durable source of truth.

```python
import json
from pathlib import Path
from openbridge import Bridge
bridge = Bridge(name="resumable", pool="resumable")

DELIVERABLE = Path("results.json")

async def main():
    done = json.loads(DELIVERABLE.read_text()) if DELIVERABLE.exists() else {}

    for item in ALL_ITEMS:
        if item in done:
            continue   # already processed; skip
        r = await bridge.ask(
            item_id=item, prompt=f"process {item}", template={"x": ""},
        )
        done[item] = r.data
        DELIVERABLE.write_text(json.dumps(done, indent=2))   # save after every item

bridge.serve(main())
```

**Why save after every item?** A crash mid-loop leaves you with at most
one item half-done (the in-flight one). The reaper will requeue it on
next run; everything else resumes immediately.

**Combined with built-in `bridge.checkpoint` / `bridge.save`**:

```python
async def main():
    progress = bridge.checkpoint("progress", default={"done": []})
    for item in ALL_ITEMS:
        if item in progress["done"]:
            continue
        r = await bridge.ask(...)
        progress["done"].append(item)
        bridge.save("progress", progress)
```

`checkpoint` reads `<workdir>/checkpoint_progress.json`; `save` writes
it. Same idea, just collocated with the rest of the workdir.

**See:** [`examples/07_resume.py`](../examples/07_resume.py)

---

## Pattern 8 — Recycled workers (bounded-context sessions)

**Shape:** pass `recycle=True` to `spawn_workers`. Each worker session
processes a bounded number of items (`max_jobs_per_session`, default 1)
then exits; a supervisor spawns a replacement.

**When to use:** long-running batches (hundreds to thousands of items)
where a single Claude session would eventually accumulate enough
context to trigger compaction. With recycling, every session starts
with a clean context window, so compaction is impossible by
construction.

```python
from openbridge import Bridge
from openbridge.spawn import spawn_workers
bridge = Bridge(name="big-batch", pool="big-batch")

async def main():
    # Default: every item gets a fresh session (max isolation, max overhead).
    async with spawn_workers(bridge, count=4, recycle=True):
        for item in ALL_ITEMS:
            await bridge.ask(item_id=item, prompt=..., template=...)

bridge.serve(main())
```

**Amortizing startup cost:** raise `max_jobs_per_session` to handle
more items per session. The supervisor only spawns a fresh process
every N items, so the per-item startup tax drops by a factor of N:

```python
# Process up to 5 items per session, then recycle.
async with spawn_workers(bridge, count=4, recycle=True,
                         max_jobs_per_session=5):
    ...
```

**Picking `max_jobs_per_session`:**

| Per-item context | Reasonable value |
|---|---|
| Large reads, many tool calls per item | `1` |
| Typical text extraction / classification | `3–5` |
| Small / single-turn items | `10+` |

**Trade-off:** each spawn pays ~5–10s of Claude startup latency
(authentication, skill load, model warm-up). Use `recycle=False`
(default) when batches are short or items are very small — there the
startup tax dominates the work.

**How it works internally:**

- The user-level prompt sent to each subprocess is overridden to
  "process EXACTLY ONE item, then exit." The SKILL honors this
  override (see the *How this skill is loaded* section of
  `skills/openbridge/SKILL.md`).
- A supervisor coroutine per worker slot watches the subprocess; when
  it exits (zero or non-zero), the supervisor spawns a replacement
  with an incremented generation counter (`worker-0.gen0`, `gen1`, …).
  Worker logs accumulate across generations at
  `<workdir>/workers/worker-N.log`.
- A crash-loop guard backs off (1s → 2s → … → 30s) if a worker exits
  within 5 seconds of spawn, so a misconfigured environment doesn't
  thrash the pool.

**See:** [`examples/97_recycled_workers.py`](../examples/97_recycled_workers.py)

---

## Combining patterns

Real skills usually compose several:

```python
# Pattern 7 (resume) + Pattern 2 (fan-out) + Pattern 4 (validate)
async def main():
    progress = bridge.checkpoint("progress", default={"done": []})
    pending = [i for i in ALL_ITEMS if i not in progress["done"]]

    sem = asyncio.Semaphore(5)
    lock = asyncio.Lock()

    async def one(item):
        async with sem:
            try:
                r = await bridge.ask(
                    item_id=item, prompt=..., template=...,
                    validate=lambda d: None if d.get("ok") else "missing ok",
                )
            except RuntimeError as e:
                # Validation exhausted — mark as failed but keep going
                async with lock:
                    progress.setdefault("failed", []).append({"id": item, "err": str(e)})
                    bridge.save("progress", progress)
                return
            async with lock:
                progress["done"].append(item)
                bridge.save("progress", progress)

    await asyncio.gather(*(one(i) for i in pending), return_exceptions=True)
```

That's a real-world producer in ~20 lines. Workers don't change — they
just run `openbridge get --pool <name>` in a loop. The producer handles
all the coordination.

---

## What OpenBridge doesn't do

These are out of scope by design:

- **Priority queues / item dependencies.** All work in a pool is FIFO.
  If item B needs item A's output, use a stage-to-stage pipeline
  (pattern 5) instead.
- **Dead-letter queue / failed-item routing.** If validation exhausts
  retries, `ask()` raises `RuntimeError` — your code decides what to do
  with the failed item.
- **Rate limiting.** Use a semaphore to cap concurrency; OpenBridge
  doesn't slow you down.
- **Distributed worker discovery.** Workers connect by knowing the pool
  name and Redis URL. There's no service-discovery layer.
- **Cross-machine scratch files.** Workers must be able to read the
  scratch file at the path the prompt names. Same machine + same
  filesystem, OR a shared mount.

For these, layer the missing piece on top — OpenBridge is intentionally
small.
