# Setup guide

Get OpenBridge installed, verified, and running your first pattern in
under 10 minutes.

---

## 1. Prerequisites

- **Python 3.10+** (the library uses `str | None` union syntax).
- **Docker** with daemon running (OpenBridge auto-launches a Redis
  container on first use). If you'd rather use your own Redis, you can
  skip Docker entirely — see [§3](#3-bring-your-own-redis).
- **`pip`** capable of installing in editable mode (`pip install -e`).

Verify:

```bash
python3 --version       # ≥ 3.10.0
docker --version        # any recent version
docker info             # confirms the daemon is reachable
pip --version
```

If Docker complains "Cannot connect to the Docker daemon", start the
Docker service (systemd: `sudo systemctl start docker`; macOS / Windows:
launch Docker Desktop).

---

## 2. Install

```bash
git clone https://github.com/<your-org>/openbridge.git
cd openbridge
pip install -e .
```

That installs the `openbridge` Python package and the `openbridge`
console-script CLI.

Verify both:

```bash
python -c "from openbridge import Bridge, AskResult; print('lib OK')"
openbridge --help | head -3
```

Expected output:

```
lib OK

usage: openbridge [-h]
                  {get,submit,skip,list,status,claims,reclaim,claim-refresh,redis-up,redis-down}
                  ...
```

---

## 3. Bring your own Redis (optional)

By default, OpenBridge runs `docker run -d --name openbridge-redis -p
127.0.0.1:6379:6379 redis:7-alpine` on first use. To opt out:

```bash
export OPENBRIDGE_NO_DOCKER=1
export REDIS_URL=redis://<your-host>:<port>/<db>   # your existing Redis
```

Or use the included `openbridge redis-up` / `redis-down` commands
explicitly:

```bash
openbridge redis-up           # start the container
openbridge redis-down         # stop it (keep data)
openbridge redis-down --remove # stop AND wipe the container
```

---

## 4. First run — verify end-to-end

The repo ships with seven numbered examples in `examples/`, each
demonstrating one pattern. Start with the simplest:

```bash
# Terminal A — the producer
python examples/01_serial_loop.py
```

On first run, OpenBridge auto-bootstraps Redis (you'll see `[openbridge]
launching new container openbridge-redis ...`), publishes work items
into a pool called `word-classifier`, then blocks waiting for a worker.

```bash
# Terminal B — the worker (simulating what a Claude session would do)
openbridge get --pool word-classifier
```

You'll see a prompt asking to classify a word, plus a footer:

```
--- work_id: <uuid>
--- item:    aurora
--- pool:    word-classifier
--- edit:    /<path>/.word-classifier/work/<uuid>.json
--- then:    openbridge submit --pool word-classifier --work-id <uuid>
--- or:      openbridge skip --pool word-classifier --work-id <uuid> --reason ...
```

Edit the file at `--- edit:` to set `part_of_speech` to `noun`, then run
the `--- then:` command. Producer's `await` resumes; loops to next item.

A **Claude session** loaded with `skills/openbridge/SKILL.md` would do
exactly the above autonomously. From your side it's "just a CLI tool."

---

## 5. The mental model

OpenBridge is three pieces:

```
┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
│ Producer (your code) │ → │  Pool (Redis queue)  │ → │  Workers (Claude     │
│                      │ ← │                      │ ← │  sessions or any     │
│  await bridge.ask()  │   │                      │   │  other consumer)     │
└──────────────────────┘   └──────────────────────┘   └──────────────────────┘
```

- **Producer**: a Python script you write. Calls `bridge.ask(prompt=..., template=...)` per work item. Stays running.
- **Pool**: a named queue in Redis. Multiple producers can feed it; multiple workers can drain it.
- **Worker**: any process (typically a Claude session) running `openbridge get --pool POOL`. Pulls one item, does the work, runs `openbridge submit`.

You write the **producer**; you load the **operator skill** into Claude
to act as a worker; you compose them with a **pool name**.

---

## 6. Where things live

After running an example, the workdir on disk looks like:

```
.word-classifier/                     <- workdir; named .<producer-name> by default
├── state.json                        producer session info
├── history.jsonl                     audit log of every submit/skip/validation event
├── work/<uuid>.json                  scratch submission per work item
└── checkpoint_progress.json          progress checkpoint (your code's `bridge.save`)
```

Inside Redis:

```
openbridge:pool:<pool>:queue          LIST of work_ids ready for workers
openbridge:pool:<pool>:work:<id>      HASH with the prompt + paths for one item
openbridge:pool:<pool>:claim:<id>     STR worker_id holding the work, TTL 1800s
openbridge:pool:<pool>:result:<id>    LIST of 1 element, awakens the producer
openbridge:pool:<pool>:producers      SET of active producer IDs
```

You can inspect with `openbridge list`, `openbridge status --pool X`,
and `openbridge claims --pool X`.

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `openbridge --help` says "command not found" | `pip install -e .` didn't put the script on `PATH` | `python -m openbridge ...` works always; otherwise check `pip show openbridge` for the install location and ensure that `bin/` is on `PATH` |
| First run hangs on `[openbridge] launching ... container` | Docker daemon not running, or image pull is slow | `docker info` to verify daemon; `docker pull redis:7-alpine` to pre-fetch |
| `Bridge` constructor raises `ValueError: name must match ...` | Invalid char in name or pool | Use `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}` (no `:`, no spaces, no `/`) |
| Producer blocks indefinitely on `await ask(...)` | No worker is running `openbridge get` on that pool | In another terminal: `openbridge list` to see the pool, then `openbridge get --pool <name>` |
| `openbridge submit` says "work not found" | Either you typed the wrong work_id, or the producer already moved on (claim TTL expired and another worker took it) | Run `openbridge get --pool X` for the next item |
| Two workers seem to be racing on the same work_id | One used `--work-id X --force` to take over | Intentional — `--force` is the post-compaction recovery flag. Without `--force`, the second worker is refused. |
| Producer crashes with `RuntimeError: item X validation failed N times` | Your `validate(data)` keeps rejecting | The submission's not meeting the criteria; either fix what the worker submits, or relax `validate` |
| `[openbridge] reaper re-queued N orphaned work item(s)` | Worker died mid-edit; reaper auto-recovered | Normal — informational only |

---

## 8. Next steps

- Read `docs/PATTERNS.md` for a tour of the seven patterns with working examples.
- Read `skills/openbridge/SKILL.md` to understand what a Claude session sees as a worker.
- Read `skills/openbridge-build/SKILL.md` for the full library API.
- Browse the numbered examples in `examples/`.
