"""openbridge — pool-based work distribution for Claude-driven skills.

A skill is a normal `async def main():` that calls `await bridge.ask(...)`
each time it needs Claude to do something. Each ask() publishes one work
item to a named pool's queue. Any Claude session subscribed to that pool
(via `bridge get --pool <pool>`) can pick it up, do the work, and submit
the result back to that specific work item. The producer's `await`
wakes up when its work item's result arrives.

Multiple daemons can produce into the same pool. Multiple Claude
sessions can consume from the same pool. Pool name namespaces everything
— two projects on the same machine don't collide.

State model
-----------

Per-producer (the daemon process):

    <workdir>/state.json              status, owner, started_at, in-flight work
    <workdir>/history.jsonl           append-only event log
    <workdir>/work/<work_id>.json     scratch submission file (one per ask)
    <workdir>/checkpoint_*.json       user crash-recovery state (bridge.save)

Per-pool (in Redis):

    openbridge:pool:<pool>:queue              LIST work_ids — FIFO backlog
    openbridge:pool:<pool>:work:<work_id>     HASH payload (prompt, item_id, submission_path, producer_id, created_at, attempt)
    openbridge:pool:<pool>:claim:<work_id>    STR worker_id, TTL 300s
    openbridge:pool:<pool>:result:<work_id>   LIST 1-element (BLPOP target for producer)
    openbridge:pool:<pool>:producers          SET of currently active producer IDs
    openbridge:pool:<pool>:workers            SET of currently active worker IDs

The producer registers itself in :producers when started, removes itself
on graceful exit. There is no singleton lock on producers — multiple
daemons can produce in parallel into the same pool.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import redis.asyncio as aioredis
import redis.exceptions as rexc

from .redis_runtime import ensure_redis


# Names and pool identifiers feed directly into Redis keys; reject anything
# that would break key parsing or shell quoting.
_VALID_IDENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")

# Period (seconds) for the background claim reaper to scan for orphaned work.
_REAPER_INTERVAL_S = 30

# Max BRPOP reconnect attempts on transient Redis errors.
_BRPOP_MAX_RETRIES = 6
_BRPOP_INITIAL_BACKOFF_S = 1


# ---------------------------------------------------------------------------
# Data shapes
# ---------------------------------------------------------------------------

@dataclass
class AskResult:
    """What `await bridge.ask(...)` returns."""
    data: dict
    skipped: bool = False
    skip_reason: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json(p: Path, data: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    tmp.replace(p)


def _read_json(p: Path, default: Any = None) -> Any:
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        return default


# ---------------------------------------------------------------------------
# Bridge (producer side)
# ---------------------------------------------------------------------------

class Bridge:
    """Daemon-side handle. One per producer process.

    Parameters
    ----------
    name : str
        This producer's unique identity (used in `bridge list`, status,
        history). Multiple producers can run concurrently with different
        names.
    pool : str
        The work pool to publish into. Workers consume from this pool.
        Multiple producers sharing a pool spread work across the same
        worker pool.
    redis_url : str, optional
        Defaults to $REDIS_URL or redis://localhost:6379/0.
    workdir : str | Path, optional
        Where state.json / history.jsonl / scratch files live.
        Defaults to `<cwd>/.<name>/`.
    """

    def __init__(
        self,
        name: str,
        pool: str,
        *,
        redis_url: str | None = None,
        workdir: str | Path | None = None,
    ):
        if not name or not _VALID_IDENT.match(name):
            raise ValueError(
                f"Bridge `name` must match {_VALID_IDENT.pattern}: {name!r}")
        if not pool or not _VALID_IDENT.match(pool):
            raise ValueError(
                f"Bridge `pool` must match {_VALID_IDENT.pattern}: {pool!r}")
        self.name = name
        self.pool = pool
        self.redis_url = redis_url or os.environ.get(
            "REDIS_URL", "redis://localhost:6379/0")
        self.workdir = Path(workdir or Path.cwd() / f".{name}").resolve()
        self.workdir.mkdir(parents=True, exist_ok=True)
        (self.workdir / "work").mkdir(exist_ok=True)
        self._redis: aioredis.Redis | None = None
        ensure_redis(self.redis_url)

    # ---- file paths ----
    @property
    def state_path(self) -> Path:    return self.workdir / "state.json"
    @property
    def history_path(self) -> Path:  return self.workdir / "history.jsonl"

    def _work_scratch(self, work_id: str) -> Path:
        return self.workdir / "work" / f"{work_id}.json"

    # ---- Redis keys ----
    @property
    def k_queue(self) -> str:     return f"openbridge:pool:{self.pool}:queue"
    @property
    def k_producers(self) -> str: return f"openbridge:pool:{self.pool}:producers"
    @property
    def k_workers(self) -> str:   return f"openbridge:pool:{self.pool}:workers"

    def k_work(self, work_id: str) -> str:
        return f"openbridge:pool:{self.pool}:work:{work_id}"

    def k_claim(self, work_id: str) -> str:
        return f"openbridge:pool:{self.pool}:claim:{work_id}"

    def k_result(self, work_id: str) -> str:
        return f"openbridge:pool:{self.pool}:result:{work_id}"

    async def _r(self) -> aioredis.Redis:
        if self._redis is None:
            # socket_timeout=None: redis-py 8.0 introduced a default 30s
            # socket read timeout that would kill our long-blocking BRPOPs
            # (server-side timeout=0). Restore the prior behavior explicitly.
            self._redis = aioredis.from_url(
                self.redis_url, decode_responses=True, socket_timeout=None,
            )
        return self._redis

    # ---- ask ----
    async def ask(
        self,
        *,
        item_id: str,
        prompt: str,
        template: dict | None = None,
        validate: Callable[[dict], str | None] | None = None,
        max_validation_retries: int = 5,
    ) -> AskResult:
        """Publish a work item, await any worker's response, return it.

        Each call generates a fresh work_id and scratch submission file.
        Multiple ask() calls can run concurrently from one process — each
        gets its own work_id and waits on its own result list.
        """
        r = await self._r()
        template = template or {}
        current_template = dict(template)
        current_prompt = prompt

        for attempt in range(max_validation_retries):
            work_id = str(uuid.uuid4())
            sub_path = self._work_scratch(work_id)
            _write_json(sub_path, current_template)

            payload = {
                "work_id": work_id,
                "pool": self.pool,
                "producer_id": self.name,
                "item_id": item_id,
                "prompt_text": current_prompt,
                "submission_path": str(sub_path),
                "attempt": attempt,
                "created_at": _now(),
            }

            # Publish the work payload + push the work_id onto the queue.
            # Note the LPUSH happens last so workers never see a work_id
            # without its payload being readable.
            await r.hset(self.k_work(work_id),
                         mapping={k: json.dumps(v) for k, v in payload.items()})
            await r.expire(self.k_work(work_id), 3600)
            await r.lpush(self.k_queue, work_id)

            self._update_state(
                last_published_work_id=work_id,
                last_published_item_id=item_id,
                last_published_at=_now(),
            )

            # Block on the result for this specific work_id, retrying
            # through transient Redis connection drops.
            raw = await self._brpop_with_retry(self.k_result(work_id))
            try:
                resp = json.loads(raw) if raw is not None else None
            except json.JSONDecodeError:
                resp = None
            if resp is None:
                # Corrupt or missing signal — re-publish under a new work_id.
                # The previous work item is abandoned; its keys may linger
                # until TTL but that's OK.
                continue

            # Clean up the work/result keys — the worker has been told
            # success. Scratch file is GC'd on next startup.
            await r.delete(self.k_work(work_id), self.k_result(work_id),
                           self.k_claim(work_id))

            if resp.get("kind") == "skip":
                result = AskResult(data={}, skipped=True,
                                   skip_reason=resp.get("reason", ""))
                self._append_history({
                    "ts": _now(), "work_id": work_id, "item_id": item_id,
                    "event": "skipped", "reason": result.skip_reason,
                    "worker": resp.get("worker_id"),
                })
                return result

            # submission data lives on disk; the result signal just
            # confirms the worker is done editing.
            data = _read_json(sub_path, {}) or {}

            if validate is not None:
                # An exception inside validate() is treated as a rejection —
                # don't crash the producer on a buggy validator.
                try:
                    err = validate(data)
                except Exception as e:
                    err = f"validate() raised {type(e).__name__}: {e}"
                if err:
                    self._append_history({
                        "ts": _now(), "work_id": work_id, "item_id": item_id,
                        "event": "validation_failed", "error": err,
                        "worker": resp.get("worker_id"),
                    })
                    print(f"[openbridge] {item_id} ✗ validation failed: "
                          f"{err} (attempt {attempt + 1}, re-publishing)",
                          file=sys.stderr, flush=True)
                    current_prompt = (
                        f"{prompt}\n\n"
                        f"=== PREVIOUS SUBMISSION REJECTED ===\n{err}\n\n"
                        f"submission.json now contains your previous draft — "
                        f"edit it to fix the problem above and re-run `bridge "
                        f"submit`."
                    )
                    current_template = data
                    continue

            self._append_history({
                "ts": _now(), "work_id": work_id, "item_id": item_id,
                "event": "submitted", "worker": resp.get("worker_id"),
            })
            return AskResult(data=data)

        raise RuntimeError(
            f"item {item_id!r}: validation failed "
            f"{max_validation_retries} times — aborting"
        )

    # ---- state helpers ----
    def _update_state(self, **fields: Any) -> None:
        st = _read_json(self.state_path, {}) or {}
        st.update(fields)
        st["updated_at"] = _now()
        _write_json(self.state_path, st)

    def _append_history(self, entry: dict) -> None:
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        with self.history_path.open("a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    # ---- resilient Redis I/O ----
    async def _brpop_with_retry(self, key: str) -> str | None:
        """BRPOP `key` blocking, with reconnect/backoff on transient errors.

        On a clean Redis restart the result list is gone — we return None so
        the caller can re-publish under a new work_id.
        """
        backoff = _BRPOP_INITIAL_BACKOFF_S
        for attempt in range(_BRPOP_MAX_RETRIES):
            try:
                r = await self._r()
                res = await r.brpop([key], timeout=0)
                # res is (key, value) or None on timeout (but we use 0=block forever)
                return res[1] if res else None
            except (rexc.ConnectionError, rexc.TimeoutError, OSError) as e:
                # Force a fresh connection next iteration.
                if self._redis is not None:
                    try:
                        await self._redis.aclose()
                    except Exception:
                        pass
                    self._redis = None
                print(f"[openbridge] redis I/O error on {key}: "
                      f"{type(e).__name__}: {e}; retry "
                      f"{attempt + 1}/{_BRPOP_MAX_RETRIES} in {backoff}s",
                      file=sys.stderr, flush=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise RuntimeError(
            f"Redis unreachable after {_BRPOP_MAX_RETRIES} reconnect attempts")

    # ---- background: claim reaper ----
    async def _claim_reaper(self) -> None:
        """Periodically scan the pool for orphaned work (payload exists,
        no claim, not on queue, no result) and re-queue it.

        This recovers from worker crashes mid-edit — without it, a producer
        would block on BRPOP forever and the operator would have to
        `bridge reclaim` by hand.
        """
        try:
            while True:
                await asyncio.sleep(_REAPER_INTERVAL_S)
                try:
                    await self._reap_once()
                except Exception as e:
                    # Don't let a transient scan failure kill the reaper.
                    print(f"[openbridge] reaper error: "
                          f"{type(e).__name__}: {e}",
                          file=sys.stderr, flush=True)
        except asyncio.CancelledError:
            return

    async def _reap_once(self) -> None:
        r = await self._r()
        work_prefix = f"openbridge:pool:{self.pool}:work:"
        # Snapshot the queue contents once — checking each candidate's
        # membership is cheap if we have it locally.
        queue_items = set(await r.lrange(self.k_queue, 0, -1))
        reclaimed: list[str] = []
        async for k in r.scan_iter(f"{work_prefix}*"):
            work_id = k[len(work_prefix):]
            # Skip if a worker holds a live claim — they're editing.
            if await r.exists(self.k_claim(work_id)):
                continue
            # Skip if still on the queue waiting for a worker.
            if work_id in queue_items:
                continue
            # Skip if a result is pending pickup by the producer.
            if await r.exists(self.k_result(work_id)):
                continue
            # Truly orphaned: re-queue it.
            await r.lpush(self.k_queue, work_id)
            reclaimed.append(work_id)
        if reclaimed:
            for wid in reclaimed:
                self._append_history({
                    "ts": _now(), "work_id": wid,
                    "event": "reclaimed_by_reaper",
                })
            print(f"[openbridge] reaper re-queued {len(reclaimed)} "
                  f"orphaned work item(s) in pool {self.pool}",
                  file=sys.stderr, flush=True)

    # ---- scratch GC ----
    async def _gc_scratch(self) -> None:
        """Delete scratch files whose work payload is no longer in Redis.

        Successful submissions delete the work hash; this sweep catches
        the leftover scratch files. Idempotent, cheap, runs at startup.
        """
        work_dir = self.workdir / "work"
        if not work_dir.exists():
            return
        r = await self._r()
        removed = 0
        for f in work_dir.glob("*.json"):
            work_id = f.stem
            if not await r.exists(self.k_work(work_id)):
                try:
                    f.unlink()
                    removed += 1
                except OSError:
                    pass
        if removed:
            print(f"[openbridge] GC: removed {removed} stale scratch file(s)",
                  file=sys.stderr, flush=True)

    # ---- checkpointing ----
    def checkpoint(self, key: str, default: Any = None) -> Any:
        return _read_json(self.workdir / f"checkpoint_{key}.json",
                          default if default is not None else {})

    def save(self, key: str, data: Any) -> None:
        _write_json(self.workdir / f"checkpoint_{key}.json", data)

    # ---- lifecycle ----
    def serve(self, coro: Awaitable[Any]) -> None:
        try:
            asyncio.run(self._serve(coro))
        except KeyboardInterrupt:
            pass

    async def _serve(self, coro: Awaitable[Any]) -> None:
        r = await self._r()

        # Register producer in pool membership.
        my_id = f"{self.name}@{os.getpid()}@{os.uname().nodename}"
        await r.sadd(self.k_producers, my_id)

        self._update_state(
            name=self.name, pool=self.pool, status="starting",
            owner=my_id, started_at=_now(), workdir=str(self.workdir),
            redis_url=self.redis_url,
        )

        # Startup housekeeping: sweep scratch files for completed work.
        await self._gc_scratch()

        # Start the background claim reaper. It runs for the whole serve()
        # lifetime; cancelled in the finally block.
        reaper_task = asyncio.create_task(self._claim_reaper())

        try:
            await coro
            self._update_state(status="done")
        except Exception as e:
            self._update_state(status=f"error: {type(e).__name__}: {e}")
            raise
        finally:
            reaper_task.cancel()
            try:
                await reaper_task
            except (asyncio.CancelledError, Exception):
                pass
            try:
                await r.srem(self.k_producers, my_id)
                # Final GC pass — clean up scratch files for items that
                # completed during this run.
                await self._gc_scratch()
                await r.aclose()
            except Exception:
                pass
