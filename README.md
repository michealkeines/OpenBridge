# OpenBridge

> **Orchestrate multiple Claude Code sessions programmatically — and stop paying Claude API rates for batch work.**

You already pay a flat monthly subscription for Claude Code. Why are you also burning per-token API budget to run agents on top of it? OpenBridge lets your Python code drive any number of Claude Code sessions in parallel — turning the subscription you already have into the agent-orchestration backend, with **zero additional API spend**.

## The cost problem this solves

Building an agent that processes hundreds or thousands of items via the Anthropic API gets expensive quickly:

| Workload | Anthropic API | OpenBridge + Claude Code |
|---|---|---|
| 1,000 items × ~30k tokens each (read source + structured submission) | **$50–$200+** in API charges, depending on model | **$0** — covered by your existing Claude Code subscription |
| 10,000-item batch refactor or code-walk | **$500–$2,000+** | **$0** |
| Long-running agent that runs nightly for months | Linear cost growth | Flat cost, forever |

OpenBridge ships zero AI capability of its own — it's a **coordination layer**. The intelligence comes from the Claude Code sessions you already have. The library just lets your Python code feed prompts to them programmatically and collect the results.

## How it works

OpenBridge is a small library + CLI for distributing prompts between a Python program (the **producer**) and one or more Claude Code sessions (the **workers**). The producer issues `await bridge.ask(...)` calls; each call publishes a work item into a named pool. Workers pull from the pool, do the work, and submit. The producer's `await` resumes with the result.

It's the missing piece when:

- You want **fan-out**: one Python program issuing N concurrent prompts, served by N parallel Claude Code sessions — all paid by your existing subscription.
- You want **agentic batch work** without an API bill: walk a codebase, classify findings, summarise, refactor, audit — each item gets full Claude attention.
- You want **stateless workers**: a Claude Code session can join, do some work, and leave. The pool keeps going.

State of the art: stable, well-tested, production-grade for unattended runs of 100s–1000s of items at zero per-item AI cost.

---

## Architecture in one diagram

```
┌─ Producer (Python) ────────────────────────┐
│                                            │
│   async def main():                        │
│     async with spawn_workers(bridge,       │
│                              count=N):     │   ┌─ Redis (signal+queue) ──┐
│       await bridge.ask(...)                │   │  pool:foo:queue         │
│         ► writes scratch.json              │   │  pool:foo:work:<wid>    │
│         ► LPUSH queue   ───────────────────┼──►│  pool:foo:result:<wid>  │
│         ► BLPOP result:<wid> ◄─────────────┼───│  pool:foo:claim:<wid>   │
│         ► reads scratch.json               │   └─────────────────────────┘
│                                            │              ▲ │
│   spawn_workers manages N claude           │              │ │ BRPOP atomic
│   subprocesses (subscription, not API)     │              │ ▼
└───────────────┬────────────────────────────┘   ┌──────────┴───────────┐
                │ spawn + supervise              │                      │
                ▼                                ▼                      ▼
       ┌─ claude worker 1 ─┐   ┌─ claude worker 2 ─┐   ...
       │ skill loaded via  │   │ skill loaded via  │
       │ append-system-    │   │ append-system-    │
       │ prompt-file       │   │ prompt-file       │
       │ $ openbridge get  │   │ $ openbridge get  │
       │   edit scratch    │   │   edit scratch    │
       │ $ openbridge      │   │ $ openbridge      │
       │   submit          │   │   submit          │
       └───────────────────┘   └───────────────────┘
```

The Python program is the source of truth (durable, on-disk state).
Workers are stateless — they can come and go. With `spawn_workers` the
producer launches its own Claude subprocesses automatically; you can
also still run workers in separate terminals if you prefer.

---

## Quickstart

### Install

```bash
git clone https://github.com/<your-org>/openbridge.git
cd openbridge
pip install -e .
```

Dependencies: Python 3.10+, `redis>=4.2`, `docker` on `$PATH` (for the auto-bootstrap Redis container — opt out with `OPENBRIDGE_NO_DOCKER=1`), and **the `claude` CLI on `$PATH`** if you use `spawn_workers` (override location with `OPENBRIDGE_CLAUDE_BIN`).

**Full setup walkthrough:** [`docs/SETUP.md`](docs/SETUP.md). Covers prerequisites, install, bring-your-own-Redis, troubleshooting.

### Pick a pattern

OpenBridge ships seven worked examples under [`examples/`](examples/),
each demonstrating one composable pattern. The full tour is in
[`docs/PATTERNS.md`](docs/PATTERNS.md).

| # | Pattern | Example |
|---|---|---|
| 1 | Serial loop | [`examples/01_serial_loop.py`](examples/01_serial_loop.py) |
| 2 | Concurrent fan-out (N workers in parallel) | [`examples/02_concurrent_fanout.py`](examples/02_concurrent_fanout.py) |
| 3 | Multi-step per item (classify → write) | [`examples/03_multi_step.py`](examples/03_multi_step.py) |
| 4 | Validation with auto-reprompt | [`examples/04_validation.py`](examples/04_validation.py) |
| 5 | Stage-to-stage pipeline | [`examples/05_pipeline.py`](examples/05_pipeline.py) |
| 6 | Multi-producer, one pool | [`examples/06_multi_producer.py`](examples/06_multi_producer.py) |
| 7 | Resume from disk after restart | [`examples/07_resume.py`](examples/07_resume.py) |

### Write a producer (self-contained — workers spawn automatically)

```python
# my_skill.py
import asyncio
from openbridge import Bridge
from openbridge.spawn import spawn_workers

bridge = Bridge(name="my-skill", pool="demo")

async def main():
    items = ["aurora", "candor", "lament", "ponder"]
    progress = bridge.checkpoint("progress", default={"done": []})

    async with spawn_workers(bridge, count=3):   # 3 claude sessions auto-spawned
        sem = asyncio.Semaphore(3)
        async def process(item):
            async with sem:
                if item in progress["done"]:
                    return
                result = await bridge.ask(
                    item_id=item,
                    prompt=f"Classify {item!r} as noun/verb/adjective. Set submission.json.part_of_speech.",
                    template={"word": item, "part_of_speech": ""},
                    validate=lambda d: None if d.get("part_of_speech") in
                        {"noun","verb","adjective"} else "must be noun/verb/adjective",
                )
                print(f"{item} → {result.data['part_of_speech']}")
                progress["done"].append(item)
                bridge.save("progress", progress)

        await asyncio.gather(*(process(i) for i in items))

bridge.serve(main())
```

Run it (just one command, no second terminal needed):

```bash
python my_skill.py
```

Three workers spawn, drain the pool in parallel, then exit cleanly.

### Recycle mode — bounded-context sessions

For long-running batches where context bloat would eventually trigger
compaction, pass `recycle=True`. Each session processes a bounded
number of items (default: 1) then exits; a supervisor spawns a fresh
replacement:

```python
# Recycle every item — cleanest context, highest startup overhead
async with spawn_workers(bridge, count=4, recycle=True):
    await producer_logic()

# Recycle every 5 items — amortizes Claude startup over multiple items
async with spawn_workers(bridge, count=4, recycle=True,
                         max_jobs_per_session=5):
    await producer_logic()
```

Pick `max_jobs_per_session` based on per-item context size:

- `1` — large per-item context (long reads, many tool calls), or you
  want maximum cleanliness.
- `3–5` — typical text-classification / extraction batches.
- `10+` — small per-item context; you just want a ceiling so a session
  never runs forever.

Trade-off: each spawn pays ~5–10s of Claude startup. With higher
`max_jobs_per_session` you pay that less often, but each session
accumulates more context before recycling.

### Or: drive it manually from your own Claude sessions

If you'd rather supply workers yourself (e.g. interactive Claude Code
sessions you already have open), skip `spawn_workers` and run the CLI:

```bash
openbridge get --pool demo
# read the prompt, edit the submission.json path it names
openbridge submit --pool demo --work-id <work_id_from_the_prompt>
```

Both modes can coexist on the same pool.

---

## Why pools?

The unit of distribution is the **pool**, not the producer. This means:

- **Project isolation** — pool `data-ingest` and pool `ticket-triage` don't see each other's work.
- **Multi-producer** — five Python processes can all publish into `pool=foo`; workers serve them transparently.
- **Multi-worker** — N Claude sessions can subscribe to `pool=foo` for N-way parallelism, no driver collision.
- **Stateless workers** — a worker only needs the pool name; no daemon awareness or domain knowledge.

---

## Library API

### `Bridge(name, pool, *, redis_url=None, workdir=None)`

- `name` — this producer's unique identity. Used in `openbridge list`, history, state.json.
- `pool` — the work-distribution group workers consume from.
- `redis_url` — defaults to `$REDIS_URL` or `redis://localhost:6379/0`. Auto-bootstraps a docker container if Redis isn't reachable.
- `workdir` — defaults to `<cwd>/.<name>/`. Holds state.json, history.jsonl, scratch submission files, user checkpoints.

Identifier rules (both `name` and `pool`): `^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`.

### `await bridge.ask(*, item_id, prompt, template=None, validate=None, max_validation_retries=5) -> AskResult`

Publish one work item to the pool. Blocks until a worker submits or skips.

- `item_id` — stable string for status output + history.
- `prompt` — what the worker sees on `openbridge get`.
- `template` — pre-filled JSON the worker edits. Default `{}`.
- `validate(data) -> str | None` — accept by returning `None`, reject with an error string. On rejection the prompt is re-published with the error appended and the previous draft preserved; the worker fixes in place.
- `max_validation_retries` — after this many rejections, raises `RuntimeError`.

Concurrent `ask()` calls (via `asyncio.gather`) fan out across workers.

### `bridge.checkpoint(key, default=None)` / `bridge.save(key, data)`

Disk-backed JSON state for crash recovery. The library never auto-saves; call `save()` whenever you reach a durable point.

### `bridge.serve(coro)`

Wrap your `main()`. Registers the producer in the pool, runs the coroutine, starts a background claim-reaper, GCs scratch files, and releases everything on exit.

### `openbridge.spawn.spawn_workers(bridge, *, count, recycle=False)`

Async context manager. Spawns `count` `claude` subprocesses with the bundled openbridge SKILL loaded via `--append-system-prompt-file`. Each subprocess runs autonomously (`--dangerously-skip-permissions`) and consumes from the producer's pool.

Safety guarantees enforced unconditionally:

- `ANTHROPIC_API_KEY` and `ANTHROPIC_AUTH_TOKEN` are stripped from the child env — sessions use your `claude` OAuth subscription, never per-token API billing.
- `--dangerously-skip-permissions` is always passed; sessions are non-interactive and never block on permission prompts.
- `claude` binary is found via `$OPENBRIDGE_CLAUDE_BIN`, then `$PATH`, then a short list of common install paths. Loud failure if missing.
- Worker stdout/stderr stream to `<workdir>/workers/worker-N.log` (full `stream-json` transcripts) so the producer's console stays clean.

Modes:

- `recycle=False` (default) — each worker is a long-lived Claude session that loops through items until the pool drains. Fast per-item; one session accumulates context.
- `recycle=True` — each worker processes up to `max_jobs_per_session` items (default 1) then exits; a supervisor immediately spawns a replacement. Bounded context per session, compaction can be avoided entirely. ~5–10s of Claude startup per spawn — raise `max_jobs_per_session` to amortize.

Parameters:

- `count: int` — number of worker slots to keep populated.
- `recycle: bool = False` — enable bounded-context sessions.
- `max_jobs_per_session: int = 1` — only used when `recycle=True`. Max items per session before exit + respawn. Use `1` for maximum context cleanliness; `3–10` to amortize startup cost over multiple items.

### `AskResult`

```python
@dataclass
class AskResult:
    data: dict          # submission.json contents as the worker saved it
    skipped: bool = False
    skip_reason: str = ""
```

---

## CLI

```bash
openbridge list                                              # all pools + summary
openbridge status --pool POOL                                # detailed pool state
openbridge get --pool POOL                                   # claim next work; prints prompt + work_id
openbridge get --pool POOL --work-id WID [--force]           # re-fetch a specific item (e.g. resume)
openbridge submit --pool POOL --work-id WID [--from PATH]    # signal done; payload on disk
openbridge skip --pool POOL --work-id WID --reason "..."
openbridge claims --pool POOL                                # all in-flight work items
openbridge reclaim --pool POOL --work-id WID [--force]       # manual re-queue (worker died)
openbridge claim-refresh --pool POOL --work-id WID [--ttl SEC]
openbridge redis-up / redis-down [--remove]                  # container control
```

---

## Resilience and failure modes

Built-in:

| Failure | Recovery |
|---|---|
| Worker dies mid-edit | Background **claim reaper** scans every 30s for orphaned work and re-queues it |
| Producer crashes mid-loop | Restart; the deliverable file (yours, via `checkpoint`/`save`) is the resume source |
| Redis container restarts | Producer's BRPOP retries with exponential backoff (up to 6 attempts) |
| Two workers race on same `--work-id` | Refused without `--force`; explicit takeover for post-compaction resume |
| Worker submits malformed JSON | CLI validates before pushing the signal — caller sees the error |
| User's `validate()` raises | Caught and treated as rejection — producer doesn't crash |
| Single item exhausts validation retries | `ask()` raises `RuntimeError`; user code can catch and continue |
| Scratch files accumulate | GC'd at producer startup and graceful exit |

Limitations:

- One BRPOP per ask() — producers don't yet share a single connection across concurrent asks (each holds one); for high concurrency you may want to up Redis's max-connections.
- Workers across machines need a shared filesystem for scratch files (UUID-named JSON under `<producer-workdir>/work/`).
- Pool name + producer name are sanitised against a strict regex; arbitrary unicode names not supported.

---

## Layout

```
OpenBridge/
├── README.md                 (this file)
├── LICENSE                   (MIT)
├── pyproject.toml            (installable via pip install -e .)
├── bin/openbridge            (CLI wrapper — also installed as a console_script)
├── openbridge/
│   ├── __init__.py           (exposes Bridge, AskResult)
│   ├── __main__.py           (python -m openbridge)
│   ├── bridge.py             (Bridge class + ask/checkpoint/save/serve)
│   ├── cli.py                (get/submit/skip/list/status/...)
│   ├── spawn.py              (spawn_workers — auto-spawn claude subprocesses)
│   ├── redis_runtime.py      (Docker auto-bootstrap)
│   └── _skills/openbridge/SKILL.md  (bundled worker skill, loaded by spawn_workers)
├── skills/
│   ├── openbridge/SKILL.md         (canonical worker skill — for manual Claude sessions)
│   └── openbridge-build/SKILL.md   (producer-authoring guide)
├── examples/
│   ├── 01..07_*.py           (seven worked patterns)
│   ├── 97_recycled_workers.py
│   ├── 98_spawned_workers.py
│   └── 99_local_smoketest.py
└── tests/
    └── test_smoke.py         (end-to-end smoke tests)
```

---

## Operator guide (for Claude sessions)

When a Claude session is loaded as a worker, the loop is:

1. `openbridge list` to see what pools exist.
2. For a pool with work: `openbridge get --pool POOL`. The prompt + footer tell you what to read and edit, and the exact submit/skip commands to run.
3. Do **only** what the prompt instructs. The prompt is the domain knowledge; OpenBridge itself is domain-agnostic.
4. `openbridge submit --pool POOL --work-id <id>`. Or `openbridge skip ... --reason ...`.
5. Loop until `openbridge get` reports the pool is empty.

OpenBridge ships **two ready-made Claude skills** under [`skills/`](./skills) — drop them into your agent harness (or Claude Code) and they handle the operator/author conventions for you.

### `skills/openbridge/SKILL.md` — operator (driver) skill

Loaded into a Claude session that should **drive** an OpenBridge pool. Defines:

- The autonomous-loop contract (never ask, never voluntarily stop, never summarise mid-loop, resume seamlessly after context compaction).
- The decision table for every "what do I do when X happens" case (validation rejected, daemon gone, suspiciously fast pacing, etc.).
- The post-compaction recovery procedure using `openbridge claims --pool X`.
- The strict whitelist of conditions where the worker stops and surfaces to the user.

It's **fully generic** — no domain knowledge of any specific project. Any pool with a sensible producer-side prompt can be driven by a worker session running this skill alone.

### `skills/openbridge-build/SKILL.md` — author (producer) skill

Loaded into a Claude session that should **build** a new producer. Defines:

- The `Bridge` API in tutorial form.
- The Redis schema and workdir layout.
- Patterns: resume via deliverable file, progress prints, validation that helps the worker fix in place.
- Gotchas: concurrency, crash recovery, scratch GC.

### Building a domain skill on top of these

To make Claude **start a producer and drive it end-to-end** for a specific project (say, summarising bug reports), write a third **domain skill** alongside these two. It carries:

- What command starts your producer (e.g. `python3 ~/my-domain/producer.py --input ...`).
- What domain-specific working principles workers need (e.g. "always read the full bug report; never summarise from the title alone").
- Stage transitions if your pipeline has them ("when pool A drains, start producer for pool B").

The domain skill **defers** loop discipline to `openbridge` (or duplicates it inline) and the API/architecture to `openbridge-build`. This split keeps domain skills small (~100-200 lines) because the heavy operator/author conventions are inherited.

Example domain skill structure:

```yaml
---
name: my-domain
description: Drives the my-domain pipeline. Loop discipline lives in openbridge/SKILL.md — read that first.
---

# my-domain

## Step 0 — load the operator contract

Read `skills/openbridge/SKILL.md` from this repository first. That defines the autonomous-loop rules.

## Pipeline overview

(your stages, deliverables, defaults)

## Starting a producer (no user confirmation)

(the exact command to launch your producer, with sensible defaults)

## Per-stage working principles

(what's specific to your domain — files to read, validation criteria, etc.)
```

A worked domain skill should be roughly 100–200 lines: the autonomous-loop discipline is inherited from `openbridge`, the API is inherited from `openbridge-build`, and this file just adds the project-specific glue (start command, defaults, per-stage prompts, working principles unique to your domain).

---

## What this is NOT

- **Not a replacement for the Anthropic API or Claude SDK.** It's the *opposite* approach: instead of paying per-token to drive Claude, you use the Claude Code subscription you already have and orchestrate its sessions programmatically. Pick the API when you need pure-machine throughput at any cost; pick OpenBridge when you want zero marginal AI cost on batch work.
- Not a queue for arbitrary async tasks — the worker is assumed to be a Claude Code session (human-in-the-loop or autonomous-loop). The protocol is shaped around that.
- Not a job scheduler — there's no priority, no retry policy beyond validation rejection, no dead-letter queue.

---

## Status

`0.1.0` — works in production for the use case it was designed for (driving multi-session Claude work pools on a single machine). Cross-machine workers require a shared filesystem. PR's welcome to add the missing pieces (cross-host scratch storage, priority queues, dead-letter handling, etc.).
