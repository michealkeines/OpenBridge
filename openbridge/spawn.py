"""Spawn and supervise Claude Code subprocesses that act as openbridge workers.

This module is an **additive** layer — it doesn't change `Bridge`'s public
surface. Customers opt in by importing `spawn_workers` from this submodule:

    from openbridge import Bridge
    from openbridge.spawn import spawn_workers

    bridge = Bridge(name="producer", pool="my-pool")

    async def main():
        async with spawn_workers(bridge, count=4):
            for item in items:
                await bridge.ask(...)

    bridge.serve(main())

Two modes:

- recycle=False (default): each worker is a long-lived Claude session that
  loops through items via the openbridge skill until the pool drains. Fast
  per-item — no per-item Claude startup cost — but a single session
  accumulates context and can eventually hit compaction.

- recycle=True: each worker processes EXACTLY ONE item, then exits; a
  supervisor task immediately spawns a replacement to keep the pool at N
  workers. Every item runs in a fresh Claude session, so context never
  builds up and compaction is impossible. Trade-off: ~5–10s of Claude
  startup latency per item. Use for long-running batches where context
  cleanliness matters more than per-item throughput.

Design constraints (locked):

- Always strip ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN from the child env
  so subprocesses use the user's `claude` OAuth subscription, never API
  billing.
- Always pass --dangerously-skip-permissions — these sessions are
  non-interactive and must never block waiting for a permission prompt.
- `claude` binary is located via $OPENBRIDGE_CLAUDE_BIN, then PATH, then
  a short list of common install locations.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import signal
import sys
import time
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .bridge import Bridge


# Graceful shutdown budget per worker before escalating to SIGTERM/SIGKILL.
_GRACEFUL_EXIT_TIMEOUT_S = 30
_SIGTERM_GRACE_S = 10

# Crash-loop guard: if a worker exits in less than this many seconds after
# spawning, treat it as a failure-to-start and back off before respawning.
_RAPID_EXIT_THRESHOLD_S = 5
_RESPAWN_INITIAL_BACKOFF_S = 1
_RESPAWN_MAX_BACKOFF_S = 30

# Candidate install locations searched when $OPENBRIDGE_CLAUDE_BIN is unset
# and `claude` isn't on PATH.
_CLAUDE_FALLBACK_PATHS = (
    "~/.local/bin/claude",
    "/usr/local/bin/claude",
    "/opt/homebrew/bin/claude",
)

# Env vars that would make `claude` bill via the Anthropic API instead of
# the user's subscription. Both are stripped from every worker subprocess.
_API_BILLING_ENV_VARS = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")


def _locate_claude() -> str:
    override = os.environ.get("OPENBRIDGE_CLAUDE_BIN")
    if override:
        if not Path(override).is_file():
            raise RuntimeError(
                f"$OPENBRIDGE_CLAUDE_BIN points at {override!r} but no such file exists"
            )
        return override
    found = shutil.which("claude")
    if found:
        return found
    for candidate in _CLAUDE_FALLBACK_PATHS:
        expanded = Path(candidate).expanduser()
        if expanded.is_file():
            return str(expanded)
    raise RuntimeError(
        "claude CLI not found. Install Claude Code, add it to PATH, or set "
        "$OPENBRIDGE_CLAUDE_BIN to its absolute path."
    )


def _locate_bundled_skills_dir() -> Path:
    """Return the parent directory containing the bundled openbridge SKILL."""
    pkg_root = resources.files("openbridge")
    skills_root = pkg_root / "_skills"
    path = Path(str(skills_root))
    if not (path / "openbridge" / "SKILL.md").is_file():
        raise RuntimeError(
            f"bundled openbridge skill not found under {path!r}. "
            "Package data may be missing — re-install openbridge."
        )
    return path


def _build_child_env() -> dict[str, str]:
    env = dict(os.environ)
    for key in _API_BILLING_ENV_VARS:
        env.pop(key, None)
    return env


class _Worker:
    """Single supervised `claude` subprocess + its log file handle."""

    def __init__(self, slot: int, gen: int, proc: asyncio.subprocess.Process,
                 log_path: Path):
        self.slot = slot
        self.gen = gen
        self.proc = proc
        self.log_path = log_path
        self.spawned_at = time.monotonic()

    async def stop(self) -> None:
        """Best-effort graceful shutdown: wait, SIGTERM, SIGKILL."""
        if self.proc.returncode is not None:
            return
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.proc.wait(), timeout=_GRACEFUL_EXIT_TIMEOUT_S)
            return
        with contextlib.suppress(ProcessLookupError):
            self.proc.terminate()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.proc.wait(), timeout=_SIGTERM_GRACE_S)
            return
        with contextlib.suppress(ProcessLookupError):
            self.proc.send_signal(signal.SIGKILL)
        with contextlib.suppress(Exception):
            await self.proc.wait()


class ClaudeWorkerPool:
    """Async context manager that supervises N claude worker subprocesses.

    Prefer the `spawn_workers(...)` helper below; this class is exposed
    only for callers that need finer control (e.g. inspecting workers).
    """

    def __init__(self, bridge: "Bridge", *, count: int, recycle: bool = False,
                 max_jobs_per_session: int = 1):
        if count < 1:
            raise ValueError(f"workers count must be >= 1, got {count}")
        if max_jobs_per_session < 1:
            raise ValueError(
                f"max_jobs_per_session must be >= 1, got {max_jobs_per_session}"
            )
        self._bridge = bridge
        self._count = count
        self._recycle = recycle
        self._max_jobs_per_session = max_jobs_per_session
        self._workers: dict[int, _Worker] = {}
        self._supervisors: list[asyncio.Task[None]] = []
        self._closing = False
        self._log_dir = bridge.workdir / "workers"
        # Cached at __aenter__ — resolved once so each respawn doesn't
        # re-pay locate cost or re-validate skill presence.
        self._claude_bin: str = ""
        self._skill_file: Path = Path()
        self._env: dict[str, str] = {}

    @property
    def workers(self) -> list[_Worker]:
        return list(self._workers.values())

    def _build_argv(self) -> list[str]:
        # Long-lived: skill's "keep claiming until drained" loop drives.
        # Recycle: explicit override — process up to N items and exit.
        if self._recycle:
            n = self._max_jobs_per_session
            if n == 1:
                budget_phrase = "process EXACTLY ONE work item"
                exit_phrase = (
                    "Exit. Do NOT claim a second item. A supervisor will "
                    "spawn a fresh session for the next item — this keeps "
                    "context clean and prevents compaction."
                )
            else:
                budget_phrase = (
                    f"process UP TO {n} work items from the pool "
                    f"(stop sooner if the pool drains or the producer "
                    f"deregisters)"
                )
                exit_phrase = (
                    f"After completing your {n}th item — or when the pool "
                    f"drains, whichever comes first — exit. Do NOT process "
                    f"a {n + 1}th item. A supervisor will spawn a fresh "
                    f"session to continue the work in a clean context."
                )
            prompt = (
                f"You are an openbridge worker for pool "
                f"{self._bridge.pool!r}. Your system prompt contains the "
                f"openbridge skill rules for HOW to process work, but for "
                f"THIS session you override the loop: {budget_phrase}, "
                f"then exit.\n\n"
                f"Steps:\n"
                f"  1. Run `openbridge get --pool {self._bridge.pool}` to "
                f"claim one work item.\n"
                f"  2. Do exactly what the prompt instructs (read the files "
                f"it names, edit the submission JSON it names).\n"
                f"  3. Run `openbridge submit --pool {self._bridge.pool} "
                f"--work-id <id>` (or `openbridge skip ...`).\n"
                f"  4. Repeat steps 1–3 until you have processed "
                f"{'one item' if n == 1 else f'up to {n} items'}, then: "
                f"{exit_phrase}"
            )
        else:
            prompt = (
                f"You are an openbridge worker subscribed to pool "
                f"{self._bridge.pool!r}. Follow the openbridge skill rules "
                f"in your system prompt exactly. Begin by running "
                f"`openbridge list`, then drive pool "
                f"{self._bridge.pool!r} until it is drained and no "
                f"producers remain, then exit."
            )

        return [
            self._claude_bin,
            "-p", prompt,
            "--append-system-prompt-file", str(self._skill_file),
            "--add-dir", str(self._bridge.workdir),
            "--dangerously-skip-permissions",
            "--output-format", "stream-json",
            "--verbose",
        ]

    async def _spawn_one(self, slot: int, gen: int) -> _Worker:
        log_path = self._log_dir / f"worker-{slot}.log"
        log_fh = open(log_path, "ab")
        try:
            proc = await asyncio.create_subprocess_exec(
                *self._build_argv(),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=log_fh,
                stderr=asyncio.subprocess.STDOUT,
                env=self._env,
                cwd=str(self._bridge.workdir),
            )
        finally:
            log_fh.close()
        worker = _Worker(slot=slot, gen=gen, proc=proc, log_path=log_path)
        print(
            f"[openbridge] spawned worker-{slot}.gen{gen} "
            f"(pid={proc.pid}) → {log_path}",
            file=sys.stderr, flush=True,
        )
        return worker

    async def _supervise_slot(self, slot: int) -> None:
        """Keep one slot populated. Respawn on exit until _closing flips."""
        gen = 0
        backoff = _RESPAWN_INITIAL_BACKOFF_S
        while not self._closing:
            worker = await self._spawn_one(slot, gen)
            self._workers[slot] = worker
            rc = await worker.proc.wait()
            if self._closing:
                return
            if not self._recycle:
                # Non-recycle mode: a worker exiting on its own means it
                # decided the pool is drained. Don't respawn — that's a
                # normal terminal state.
                print(
                    f"[openbridge] worker-{slot}.gen{gen} exited rc={rc} "
                    f"(non-recycle mode, not respawning)",
                    file=sys.stderr, flush=True,
                )
                return
            lifetime = time.monotonic() - worker.spawned_at
            if lifetime < _RAPID_EXIT_THRESHOLD_S:
                print(
                    f"[openbridge] worker-{slot}.gen{gen} exited rc={rc} "
                    f"after {lifetime:.1f}s — backing off {backoff}s before "
                    f"respawn (likely startup failure; check {worker.log_path})",
                    file=sys.stderr, flush=True,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RESPAWN_MAX_BACKOFF_S)
            else:
                backoff = _RESPAWN_INITIAL_BACKOFF_S
            gen += 1

    async def __aenter__(self) -> "ClaudeWorkerPool":
        self._claude_bin = _locate_claude()
        self._skill_file = _locate_bundled_skills_dir() / "openbridge" / "SKILL.md"
        self._env = _build_child_env()
        self._log_dir.mkdir(parents=True, exist_ok=True)

        # One supervisor task per slot. Each holds its slot populated and
        # handles respawn (recycle=True) or one-shot lifecycle (recycle=False).
        for slot in range(self._count):
            self._supervisors.append(
                asyncio.create_task(self._supervise_slot(slot))
            )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._closing = True

        # Cancel supervisors first so they don't respawn while we're tearing
        # down. Then stop all current workers in parallel.
        for t in self._supervisors:
            t.cancel()
        await asyncio.gather(*self._supervisors, return_exceptions=True)
        await asyncio.gather(
            *(w.stop() for w in self._workers.values()),
            return_exceptions=True,
        )


def spawn_workers(
    bridge: "Bridge",
    *,
    count: int,
    recycle: bool = False,
    max_jobs_per_session: int = 1,
) -> ClaudeWorkerPool:
    """Context-manager helper: spawn `count` claude worker subprocesses
    bound to `bridge`'s pool.

    Parameters
    ----------
    count : int
        Number of worker slots to keep populated.
    recycle : bool, default False
        If True, each worker processes a bounded number of items
        (`max_jobs_per_session`) then exits; a supervisor immediately
        spawns a replacement. Context is bounded by the per-session
        budget, so compaction can be avoided entirely.

        If False (default), each worker is a long-lived Claude session
        that drives the pool until drained. Faster per-item; one session
        accumulates context across all items.
    max_jobs_per_session : int, default 1
        Only meaningful when `recycle=True`. Maximum number of work
        items a single Claude session will process before exiting and
        being replaced. Default 1 = freshest possible context, highest
        startup overhead. Larger values amortize the ~5–10s Claude
        startup cost over more items, at the cost of more context per
        session. Pick based on how chatty each item is:
          - 1   — very large per-item context, or maximum cleanliness
          - 3–5 — typical batches; good balance
          - 10+ — small per-item context; mostly want a recycle ceiling

    Example:
        # Long-lived sessions (default — best per-item throughput):
        async with spawn_workers(bridge, count=4):
            ...

        # Recycle every item (cleanest context, highest overhead):
        async with spawn_workers(bridge, count=4, recycle=True):
            ...

        # Recycle every 5 items (amortized startup, bounded context):
        async with spawn_workers(bridge, count=4, recycle=True,
                                 max_jobs_per_session=5):
            ...
    """
    return ClaudeWorkerPool(
        bridge,
        count=count,
        recycle=recycle,
        max_jobs_per_session=max_jobs_per_session,
    )
