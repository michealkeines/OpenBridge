"""End-to-end smoke test for OpenBridge.

Spins up a producer in-process, fakes a worker, verifies the full round-trip.
Requires a reachable Redis (auto-bootstraps via Docker if needed).
"""
import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from openbridge import Bridge


def test_basic_roundtrip(tmp_path):
    """Single ask() + one worker submit + producer receives the result."""
    workdir = tmp_path / "wd"
    bridge = Bridge(name="smoke", pool="smoke-test", workdir=str(workdir))

    async def producer():
        result = await bridge.ask(
            item_id="x",
            prompt="ignored",
            template={"answer": ""},
        )
        assert result.data["answer"] == "42", result
        assert not result.skipped

    async def worker():
        # Run the blocking-redis worker in a thread so it doesn't stall
        # the producer's event loop.
        def do_work():
            from openbridge.redis_runtime import ensure_redis
            import redis
            r = redis.from_url(ensure_redis(), decode_responses=True)
            for _ in range(50):
                res = r.brpop(["openbridge:pool:smoke-test:queue"], timeout=1)
                if res:
                    break
            else:
                raise TimeoutError("no work in queue")
            _, work_id = res
            work_raw = r.hgetall(f"openbridge:pool:smoke-test:work:{work_id}")
            work = {k: json.loads(v) for k, v in work_raw.items()}
            r.set(f"openbridge:pool:smoke-test:claim:{work_id}",
                  "smoke-worker", ex=60)
            sub = Path(work["submission_path"])
            sub.write_text(json.dumps({"answer": "42"}))
            r.lpush(f"openbridge:pool:smoke-test:result:{work_id}",
                    json.dumps({"work_id": work_id, "kind": "submit",
                                "worker_id": "smoke-worker"}))
        await asyncio.to_thread(do_work)

    async def main():
        await asyncio.gather(producer(), worker())

    bridge.serve(main())


def test_validation_rejection_then_accept(tmp_path):
    """Worker submits empty, then valid; producer's validate rejects then accepts."""
    workdir = tmp_path / "wd"
    bridge = Bridge(name="validate-smoke", pool="validate-test",
                    workdir=str(workdir))

    async def producer():
        result = await bridge.ask(
            item_id="y",
            prompt="ignored",
            template={"answer": ""},
            validate=lambda d: None if d.get("answer") else "answer required",
            max_validation_retries=3,
        )
        assert result.data["answer"] == "ok"

    async def worker():
        def do_work():
            from openbridge.redis_runtime import ensure_redis
            import redis
            r = redis.from_url(ensure_redis(), decode_responses=True)
            for attempt in range(2):
                for _ in range(50):
                    res = r.brpop(["openbridge:pool:validate-test:queue"], timeout=1)
                    if res:
                        break
                else:
                    raise TimeoutError("no work in queue")
                _, work_id = res
                work_raw = r.hgetall(f"openbridge:pool:validate-test:work:{work_id}")
                work = {k: json.loads(v) for k, v in work_raw.items()}
                sub = Path(work["submission_path"])
                sub.write_text(json.dumps(
                    {"answer": ""} if attempt == 0 else {"answer": "ok"}
                ))
                r.lpush(f"openbridge:pool:validate-test:result:{work_id}",
                        json.dumps({"work_id": work_id, "kind": "submit"}))
        await asyncio.to_thread(do_work)

    async def main():
        await asyncio.gather(producer(), worker())

    bridge.serve(main())


def test_ident_validation():
    """Bridge rejects malformed name / pool."""
    import pytest
    with pytest.raises(ValueError):
        Bridge(name="bad:colon", pool="ok")
    with pytest.raises(ValueError):
        Bridge(name="ok", pool="bad space")
    with pytest.raises(ValueError):
        Bridge(name="", pool="ok")


if __name__ == "__main__":
    # Allow running without pytest for a quick check.
    import tempfile
    for fn in (test_basic_roundtrip, test_validation_rejection_then_accept):
        with tempfile.TemporaryDirectory() as td:
            print(f"running {fn.__name__}...")
            fn(Path(td))
            print(f"  PASS")
    print("all smoke tests passed")
