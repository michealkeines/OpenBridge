"""Unit + light-integration tests for openbridge.spawn.

Most tests are pure: they exercise the small functions and the prompt
builder without spawning a real `claude`. Two integration-style tests
use `/usr/bin/true` (and `/usr/bin/false`) as a fake `claude` binary to
verify the supervisor's spawn/respawn logic without touching the
Anthropic API or needing the real Claude Code CLI installed.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

import pytest

from openbridge import Bridge
from openbridge.spawn import (
    ClaudeWorkerPool,
    _API_BILLING_ENV_VARS,
    _build_child_env,
    _locate_bundled_skills_dir,
    _locate_claude,
    spawn_workers,
)


# ---------------------------------------------------------------------------
# Public-API isolation
# ---------------------------------------------------------------------------

def test_spawn_workers_not_in_top_level_export():
    """`spawn_workers` must stay a submodule import — adding it to the
    top-level package would silently change the public surface for
    existing customers."""
    import openbridge
    assert not hasattr(openbridge, "spawn_workers"), (
        "spawn_workers leaked into top-level openbridge namespace; "
        "this is a public-API regression"
    )


def test_bridge_init_signature_unchanged():
    """Existing customer code shape must keep working: Bridge(name, pool)
    with no further required args."""
    # If this constructor sprouts new required parameters, this fails.
    Bridge(name="api-stability-check", pool="api-stability-check")


# ---------------------------------------------------------------------------
# Env stripping — the most safety-critical pure function
# ---------------------------------------------------------------------------

def test_build_child_env_strips_api_billing_vars(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-leak-me")
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "leak-me-too")
    env = _build_child_env()
    for key in _API_BILLING_ENV_VARS:
        assert key not in env, (
            f"{key} was not stripped — workers would bill via API instead "
            "of using the user's subscription"
        )


def test_build_child_env_preserves_unrelated_vars(monkeypatch):
    """Stripping must be precise — must not nuke HOME / PATH / etc."""
    monkeypatch.setenv("HOME", "/some/home")
    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setenv("OPENBRIDGE_DUMMY", "preserved")
    env = _build_child_env()
    assert env["HOME"] == "/some/home"
    assert env["PATH"] == "/usr/bin"
    assert env["OPENBRIDGE_DUMMY"] == "preserved"


def test_build_child_env_when_no_api_key_set(monkeypatch):
    """No-op when there's nothing to strip."""
    for key in _API_BILLING_ENV_VARS:
        monkeypatch.delenv(key, raising=False)
    env = _build_child_env()
    for key in _API_BILLING_ENV_VARS:
        assert key not in env


# ---------------------------------------------------------------------------
# Claude binary discovery
# ---------------------------------------------------------------------------

def test_locate_claude_honors_env_override(monkeypatch, tmp_path):
    fake = tmp_path / "fake-claude"
    fake.write_text("#!/bin/sh\nexit 0\n")
    fake.chmod(0o755)
    monkeypatch.setenv("OPENBRIDGE_CLAUDE_BIN", str(fake))
    assert _locate_claude() == str(fake)


def test_locate_claude_env_override_must_exist(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENBRIDGE_CLAUDE_BIN", str(tmp_path / "does-not-exist"))
    with pytest.raises(RuntimeError, match="OPENBRIDGE_CLAUDE_BIN"):
        _locate_claude()


def test_locate_claude_not_found_raises(monkeypatch):
    """With no override, an empty PATH, and no fallback paths existing,
    discovery must fail loudly."""
    monkeypatch.delenv("OPENBRIDGE_CLAUDE_BIN", raising=False)
    monkeypatch.setenv("PATH", "")
    # Point fallbacks at paths that won't exist.
    monkeypatch.setattr(
        "openbridge.spawn._CLAUDE_FALLBACK_PATHS",
        ("/nonexistent/a", "/nonexistent/b"),
    )
    with pytest.raises(RuntimeError, match="claude CLI not found"):
        _locate_claude()


# ---------------------------------------------------------------------------
# Bundled skill resolution
# ---------------------------------------------------------------------------

def test_bundled_skill_exists():
    """Sanity: pyproject's package-data should ship the SKILL with installs."""
    skills_dir = _locate_bundled_skills_dir()
    assert (skills_dir / "openbridge" / "SKILL.md").is_file()


def test_bundled_skill_matches_canonical():
    """The bundled copy under openbridge/_skills/ must stay byte-identical
    to the canonical skills/openbridge/SKILL.md, otherwise spawned
    workers will see stale rules."""
    repo_root = Path(__file__).resolve().parent.parent
    canonical = repo_root / "skills" / "openbridge" / "SKILL.md"
    bundled = repo_root / "openbridge" / "_skills" / "openbridge" / "SKILL.md"
    if not canonical.is_file():
        pytest.skip("canonical skill not present (running outside source tree)")
    assert canonical.read_bytes() == bundled.read_bytes(), (
        "skills/openbridge/SKILL.md and openbridge/_skills/openbridge/SKILL.md "
        "have drifted — re-sync them"
    )


# ---------------------------------------------------------------------------
# ClaudeWorkerPool argument validation
# ---------------------------------------------------------------------------

def test_pool_rejects_zero_count(tmp_path):
    bridge = Bridge(name="t1", pool="t1", workdir=str(tmp_path))
    with pytest.raises(ValueError, match="count must be >= 1"):
        ClaudeWorkerPool(bridge, count=0)


def test_pool_rejects_negative_count(tmp_path):
    bridge = Bridge(name="t2", pool="t2", workdir=str(tmp_path))
    with pytest.raises(ValueError, match="count must be >= 1"):
        ClaudeWorkerPool(bridge, count=-3)


def test_pool_rejects_zero_max_jobs(tmp_path):
    bridge = Bridge(name="t3", pool="t3", workdir=str(tmp_path))
    with pytest.raises(ValueError, match="max_jobs_per_session must be >= 1"):
        ClaudeWorkerPool(bridge, count=1, max_jobs_per_session=0)


# ---------------------------------------------------------------------------
# Argv / prompt construction — verifies what each spawned claude receives
# ---------------------------------------------------------------------------

def _make_pool(tmp_path, *, recycle=False, max_jobs_per_session=1,
               pool="argv-test") -> ClaudeWorkerPool:
    """Build a pool with claude_bin / skill_file pre-stubbed so we can
    call _build_argv without actually spawning anything."""
    bridge = Bridge(name=pool, pool=pool, workdir=str(tmp_path))
    p = ClaudeWorkerPool(
        bridge, count=1, recycle=recycle,
        max_jobs_per_session=max_jobs_per_session,
    )
    p._claude_bin = "/usr/bin/true"
    p._skill_file = Path("/tmp/fake-skill.md")
    return p


def test_argv_always_includes_skip_permissions(tmp_path):
    """Workers must never block on permission prompts — the flag must be
    present in BOTH modes."""
    for kwargs in [{}, {"recycle": True, "max_jobs_per_session": 1},
                   {"recycle": True, "max_jobs_per_session": 7}]:
        pool = _make_pool(tmp_path, **kwargs)
        argv = pool._build_argv()
        assert "--dangerously-skip-permissions" in argv, (
            f"--dangerously-skip-permissions missing with kwargs={kwargs}"
        )


def test_argv_passes_skill_file(tmp_path):
    pool = _make_pool(tmp_path)
    pool._skill_file = Path("/tmp/canary-skill.md")
    argv = pool._build_argv()
    assert "--append-system-prompt-file" in argv
    idx = argv.index("--append-system-prompt-file")
    assert argv[idx + 1] == "/tmp/canary-skill.md"


def test_argv_passes_workdir_via_add_dir(tmp_path):
    pool = _make_pool(tmp_path)
    argv = pool._build_argv()
    assert "--add-dir" in argv
    idx = argv.index("--add-dir")
    assert argv[idx + 1] == str(pool._bridge.workdir)


def test_argv_uses_stream_json_output(tmp_path):
    pool = _make_pool(tmp_path)
    argv = pool._build_argv()
    assert "--output-format" in argv
    assert argv[argv.index("--output-format") + 1] == "stream-json"


def _extract_prompt(argv: list[str]) -> str:
    """Find the value following `-p` in argv."""
    assert "-p" in argv, argv
    return argv[argv.index("-p") + 1]


def test_argv_recycle_one_says_exactly_one(tmp_path):
    pool = _make_pool(tmp_path, recycle=True, max_jobs_per_session=1)
    prompt = _extract_prompt(pool._build_argv())
    assert "EXACTLY ONE" in prompt, (
        f"recycle+max_jobs=1 prompt should say 'EXACTLY ONE', got:\n{prompt}"
    )
    # Must NOT use the N-item wording when N==1.
    assert "UP TO" not in prompt


def test_argv_recycle_n_says_up_to_n(tmp_path):
    pool = _make_pool(tmp_path, recycle=True, max_jobs_per_session=5)
    prompt = _extract_prompt(pool._build_argv())
    assert "UP TO 5" in prompt, (
        f"recycle+max_jobs=5 prompt should say 'UP TO 5', got:\n{prompt}"
    )
    assert "EXACTLY ONE" not in prompt


def test_argv_non_recycle_drives_pool(tmp_path):
    pool = _make_pool(tmp_path, recycle=False)
    prompt = _extract_prompt(pool._build_argv())
    # Long-lived workers should NOT be told to exit after one item.
    assert "EXACTLY ONE" not in prompt
    assert "UP TO" not in prompt
    # Should reference the skill's loop semantics.
    assert "drive" in prompt.lower() or "drained" in prompt.lower()


def test_argv_prompt_mentions_pool_name(tmp_path):
    pool = _make_pool(tmp_path, recycle=True, max_jobs_per_session=1,
                      pool="canary-pool-name")
    prompt = _extract_prompt(pool._build_argv())
    assert "canary-pool-name" in prompt


# ---------------------------------------------------------------------------
# Supervisor lifecycle — uses /usr/bin/true as a stand-in for `claude`
# so we exercise spawn/wait/respawn without needing the real CLI.
# ---------------------------------------------------------------------------

# Mark these as asyncio tests via the dedicated mode flag (pytest-asyncio).
pytestmark_asyncio = pytest.mark.asyncio


@pytest.mark.asyncio
async def test_non_recycle_supervisor_does_not_respawn(tmp_path, monkeypatch):
    """recycle=False: when a worker exits, it's a terminal state.
    Supervisor must NOT spawn a replacement."""
    monkeypatch.setattr("openbridge.spawn._locate_claude", lambda: "/usr/bin/true")
    monkeypatch.setattr(
        "openbridge.spawn._locate_bundled_skills_dir",
        lambda: _make_fake_skills_dir(tmp_path),
    )
    bridge = Bridge(name="nonrecycle", pool="nonrecycle",
                    workdir=str(tmp_path / "wd"))

    async with spawn_workers(bridge, count=2) as pool:
        # /usr/bin/true exits within milliseconds. Give the supervisors a
        # generous window to either notice the exit + respawn (bug) or
        # notice the exit + stop (correct, non-recycle mode).
        await asyncio.sleep(2.0)
        # Both supervisors should have completed (no respawn).
        for sup in pool._supervisors:
            assert sup.done(), (
                "non-recycle supervisor should exit after worker exits, "
                "but it's still running (likely respawning incorrectly)"
            )
    # Verify no respawn happened — each worker should still be gen 0.
    for w in pool.workers:
        assert w.gen == 0, f"unexpected respawn in non-recycle mode: gen={w.gen}"


@pytest.mark.asyncio
async def test_recycle_supervisor_respawns(tmp_path, monkeypatch):
    """recycle=True: a /usr/bin/true 'worker' exits in microseconds. The
    rapid-exit backoff fires (1s initially). We verify at least one
    respawn happened in a 3s window — i.e. gen advanced past 0."""
    monkeypatch.setattr("openbridge.spawn._locate_claude", lambda: "/usr/bin/true")
    monkeypatch.setattr(
        "openbridge.spawn._locate_bundled_skills_dir",
        lambda: _make_fake_skills_dir(tmp_path),
    )
    bridge = Bridge(name="recycle", pool="recycle-test",
                    workdir=str(tmp_path / "wd"))

    started_at = time.monotonic()
    async with spawn_workers(bridge, count=1, recycle=True) as pool:
        # Wait long enough for: spawn (immediate) → exit (immediate) →
        # rapid-exit backoff (1s) → respawn. Be generous.
        await asyncio.sleep(2.5)
        # The single slot must have advanced past gen 0.
        assert pool.workers, "no workers visible during recycle mode"
        max_gen = max(w.gen for w in pool.workers)
        assert max_gen >= 1, (
            f"recycle mode should respawn after worker exits, but max gen "
            f"observed is {max_gen} after {time.monotonic() - started_at:.1f}s"
        )


@pytest.mark.asyncio
async def test_teardown_does_not_respawn(tmp_path, monkeypatch):
    """On __aexit__, supervisors must be cancelled before any new
    subprocess is spawned. Use /usr/bin/false (exits with rc=1) and
    immediately exit the context: gen should not climb during teardown."""
    monkeypatch.setattr("openbridge.spawn._locate_claude", lambda: "/usr/bin/false")
    monkeypatch.setattr(
        "openbridge.spawn._locate_bundled_skills_dir",
        lambda: _make_fake_skills_dir(tmp_path),
    )
    bridge = Bridge(name="teardown", pool="teardown-test",
                    workdir=str(tmp_path / "wd"))

    async with spawn_workers(bridge, count=1, recycle=True) as pool:
        # Don't sleep — exit immediately. Supervisor should be cancelled
        # cleanly even if a respawn was about to fire.
        pass
    # After __aexit__, supervisors should all be done.
    for sup in pool._supervisors:
        assert sup.done(), "supervisor still running after context exit"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_skills_dir(tmp_path: Path) -> Path:
    """Create a minimal skill tree so _locate_bundled_skills_dir returns
    a valid path without depending on the real bundled file."""
    fake = tmp_path / "fake_skills"
    (fake / "openbridge").mkdir(parents=True, exist_ok=True)
    (fake / "openbridge" / "SKILL.md").write_text("# fake skill for tests\n")
    return fake
