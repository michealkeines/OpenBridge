"""openbridge — pool-based work consumer CLI.

Workers (Claude sessions or any other consumer process) pull work from
named pools. Multiple workers can subscribe to the same pool; each work
item is atomically claimed by one worker via BRPOP.

Typical use from a Claude session:

    openbridge get --pool <pool>                  # claim next work item
    # ... edit the submission.json path the prompt names ...
    openbridge submit --pool <pool> --work-id ID
    openbridge skip --pool <pool> --work-id ID --reason "..."
    openbridge list                               # all pools
    openbridge status --pool <pool>               # detailed pool state
    openbridge claims --pool <pool>               # currently-claimed work items
"""
import argparse
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path

import redis
import redis.exceptions as rexc

from .redis_runtime import ensure_redis, stop_redis


# Default claim TTL — bumped to 30 min so a Claude session has plenty of
# time to read source + edit. Override via $OPENBRIDGE_CLAIM_TTL (seconds).
CLAIM_TTL = int(os.environ.get("OPENBRIDGE_CLAIM_TTL", "1800"))

_VALID_IDENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


def _validate_ident(label: str, value: str) -> None:
    if not value or not _VALID_IDENT.match(value):
        sys.stderr.write(
            f"error: {label} must match {_VALID_IDENT.pattern}: {value!r}\n")
        sys.exit(2)


def _r() -> redis.Redis:
    """Connect with retry on transient errors."""
    url = ensure_redis()
    last_err: Exception | None = None
    backoff = 1
    for attempt in range(5):
        try:
            client = redis.from_url(url, decode_responses=True,
                                    socket_connect_timeout=5)
            client.ping()
            return client
        except (rexc.ConnectionError, rexc.TimeoutError, OSError) as e:
            last_err = e
            time.sleep(backoff)
            backoff = min(backoff * 2, 8)
    sys.stderr.write(f"error: cannot reach Redis at {url}: {last_err}\n")
    sys.exit(1)


def _worker_id() -> str:
    # Unique per `bridge` invocation. The work_id is what ties get/submit
    # together — workers don't need stable identity across invocations.
    return f"{os.getpid()}@{os.uname().nodename}@{uuid.uuid4().hex[:8]}"


def _kq(pool: str) -> str:     return f"openbridge:pool:{pool}:queue"
def _kw(pool: str, wid: str) -> str:  return f"openbridge:pool:{pool}:work:{wid}"
def _kc(pool: str, wid: str) -> str:  return f"openbridge:pool:{pool}:claim:{wid}"
def _kr(pool: str, wid: str) -> str:  return f"openbridge:pool:{pool}:result:{wid}"
def _kproducers(pool: str) -> str: return f"openbridge:pool:{pool}:producers"
def _kworkers(pool: str) -> str:   return f"openbridge:pool:{pool}:workers"


def _fetch_work(r: redis.Redis, pool: str, work_id: str) -> dict | None:
    raw = r.hgetall(_kw(pool, work_id))
    if not raw:
        return None
    return {k: json.loads(v) for k, v in raw.items()}


def cmd_get(args: argparse.Namespace) -> int:
    _validate_ident("pool", args.pool)
    r = _r()
    pool = args.pool

    # If --work-id is given, claim/refresh that specific work item.
    if args.work_id:
        work_id = args.work_id
        work = _fetch_work(r, pool, work_id)
        if work is None:
            sys.stderr.write(f"work {work_id} not found in pool {pool}\n")
            return 1
        # Conflict check: refuse to steal an active claim from another
        # worker unless --force. Common safe path: a Claude session whose
        # context was compacted re-fetching its own work — that session
        # should use --force.
        existing = r.get(_kc(pool, work_id))
        if existing and not args.force:
            sys.stderr.write(
                f"work {work_id} is currently claimed by {existing}\n"
                f"  use --force to take over (e.g. after a session compaction)\n")
            return 1
    else:
        # Block on the pool's queue. Atomic claim via SET after pop.
        result = r.brpop([_kq(pool)], timeout=args.timeout or 0)
        if result is None:
            sys.stderr.write(f"no work in pool {pool} within {args.timeout}s\n")
            return 1
        _, work_id = result
        work = _fetch_work(r, pool, work_id)
        if work is None:
            sys.stderr.write(f"queue had stale work_id {work_id} with no "
                             f"payload (expired). Try again.\n")
            return 1

    worker_id = _worker_id()
    r.set(_kc(pool, work_id), worker_id, ex=CLAIM_TTL)

    sys.stdout.write(work["prompt_text"])
    if not work["prompt_text"].endswith("\n"):
        sys.stdout.write("\n")
    sys.stdout.write(
        f"\n--- work_id: {work_id}\n"
        f"--- item:    {work['item_id']}\n"
        f"--- pool:    {pool}\n"
        f"--- edit:    {work['submission_path']}\n"
        f"--- then:    bridge submit --pool {pool} --work-id {work_id}\n"
        f"--- or:      bridge skip --pool {pool} --work-id {work_id} --reason ...\n"
    )
    return 0


def cmd_submit(args: argparse.Namespace) -> int:
    _validate_ident("pool", args.pool)
    r = _r()
    pool = args.pool
    work_id = args.work_id
    work = _fetch_work(r, pool, work_id)
    if work is None:
        sys.stderr.write(f"work {work_id} not found in pool {pool} "
                         "(may have been completed by another worker or expired)\n")
        return 1
    sub_path = (Path(args.from_file).resolve() if args.from_file
                else Path(work["submission_path"]))
    if not sub_path.exists():
        sys.stderr.write(f"submission file not found: {sub_path}\n")
        return 1
    try:
        json.loads(sub_path.read_text())
    except json.JSONDecodeError as e:
        sys.stderr.write(f"submission JSON malformed: {e}\n")
        return 1

    worker_id = r.get(_kc(pool, work_id)) or "unknown"
    r.lpush(_kr(pool, work_id),
            json.dumps({"work_id": work_id, "kind": "submit",
                        "worker_id": worker_id}))
    sys.stdout.write(
        f"submitted work {work_id} (item {work['item_id']}) to pool {pool}.\n"
        f"run `bridge get --pool {pool}` for the next work.\n"
    )
    return 0


def cmd_skip(args: argparse.Namespace) -> int:
    _validate_ident("pool", args.pool)
    r = _r()
    pool = args.pool
    work_id = args.work_id
    work = _fetch_work(r, pool, work_id)
    if work is None:
        sys.stderr.write(f"work {work_id} not found in pool {pool}\n")
        return 1
    worker_id = r.get(_kc(pool, work_id)) or "unknown"
    r.lpush(_kr(pool, work_id),
            json.dumps({"work_id": work_id, "kind": "skip",
                        "reason": args.reason or "",
                        "worker_id": worker_id}))
    sys.stdout.write(
        f"skipped work {work_id} (item {work['item_id']}) in pool {pool}.\n"
    )
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    r = _r()
    # Enumerate pools by looking at queue keys + producer/worker sets.
    pools = set()
    for k in r.scan_iter("openbridge:pool:*:queue"):
        pools.add(k.split(":")[2])
    for k in r.scan_iter("openbridge:pool:*:producers"):
        pools.add(k.split(":")[2])

    if not pools:
        sys.stdout.write("no openbridge pools currently active.\n")
        return 0

    sys.stdout.write(f"{'POOL':<25} {'QUEUED':>7} {'CLAIMED':>8} "
                     f"{'PRODUCERS':>10}\n")
    for pool in sorted(pools):
        queued = r.llen(_kq(pool))
        claimed = sum(1 for _ in r.scan_iter(f"openbridge:pool:{pool}:claim:*"))
        producers = r.scard(_kproducers(pool))
        sys.stdout.write(f"{pool:<25} {queued:>7} {claimed:>8} "
                         f"{producers:>10}\n")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    r = _r()
    pool = args.pool

    queued = r.llen(_kq(pool))
    producers = sorted(r.smembers(_kproducers(pool)))

    sys.stdout.write(f"pool:              {pool}\n")
    sys.stdout.write(f"queued work items: {queued}\n")
    sys.stdout.write(f"producers ({len(producers)}):\n")
    for p in producers:
        sys.stdout.write(f"  - {p}\n")

    # Currently-claimed (in-flight) work items
    claims = []
    for k in r.scan_iter(f"openbridge:pool:{pool}:claim:*"):
        wid = k.rsplit(":", 1)[1]
        worker_id = r.get(k) or "?"
        ttl = r.ttl(k)
        work = _fetch_work(r, pool, wid)
        item_id = work["item_id"] if work else "(payload gone)"
        claims.append((wid, item_id, worker_id, ttl))

    sys.stdout.write(f"in-flight ({len(claims)}):\n")
    for wid, item_id, worker_id, ttl in claims:
        sys.stdout.write(f"  - work_id={wid}  item={item_id}  "
                         f"worker={worker_id}  ttl={ttl}s\n")

    # Queue head preview
    if queued > 0:
        sys.stdout.write(f"next up:\n")
        for wid in r.lrange(_kq(pool), -3, -1)[::-1]:
            work = _fetch_work(r, pool, wid)
            if work:
                sys.stdout.write(f"  - {work['item_id']}  "
                                 f"(producer={work['producer_id']}, "
                                 f"work_id={wid})\n")
    return 0


def cmd_claims(args: argparse.Namespace) -> int:
    """Show all currently-claimed work items in a pool (in-flight work)."""
    r = _r()
    pool = args.pool
    found = False
    for k in r.scan_iter(f"openbridge:pool:{pool}:claim:*"):
        wid = k.rsplit(":", 1)[1]
        worker_id = r.get(k) or "?"
        ttl = r.ttl(k)
        work = _fetch_work(r, pool, wid)
        item_id = work["item_id"] if work else "(payload gone)"
        sys.stdout.write(f"work_id={wid}  item={item_id}  "
                         f"worker={worker_id}  ttl={ttl}s\n")
        found = True
    if not found:
        sys.stdout.write(f"no claimed work in pool {pool}.\n")
    return 0


def cmd_claim_refresh(args: argparse.Namespace) -> int:
    """Refresh the claim TTL on an in-flight work item — for when the
    worker needs more than the default 30 min to finish editing."""
    _validate_ident("pool", args.pool)
    r = _r()
    existing = r.get(_kc(args.pool, args.work_id))
    if not existing:
        sys.stderr.write(f"no active claim on work {args.work_id} in pool "
                         f"{args.pool} (expired or never claimed)\n")
        return 1
    ttl = args.ttl or CLAIM_TTL
    r.expire(_kc(args.pool, args.work_id), ttl)
    sys.stdout.write(f"refreshed claim on {args.work_id} → {ttl}s remaining\n")
    return 0


def cmd_reclaim(args: argparse.Namespace) -> int:
    """Re-queue a work item whose worker died (lost claim)."""
    r = _r()
    pool = args.pool
    work_id = args.work_id
    if r.exists(_kc(pool, work_id)):
        if not args.force:
            sys.stderr.write(f"work {work_id} is still claimed (use --force "
                             "to override)\n")
            return 1
    if not r.exists(_kw(pool, work_id)):
        sys.stderr.write(f"work {work_id} has no payload in pool {pool} "
                         "(expired or unknown)\n")
        return 1
    r.delete(_kc(pool, work_id))
    r.lpush(_kq(pool), work_id)
    sys.stdout.write(f"re-queued work {work_id} in pool {pool}\n")
    return 0


def cmd_redis_up(args: argparse.Namespace) -> int:
    url = ensure_redis()
    sys.stdout.write(f"Redis ready at {url}\n")
    return 0


def cmd_redis_down(args: argparse.Namespace) -> int:
    stop_redis(remove=args.remove)
    sys.stdout.write("Redis container stopped"
                     f"{' and removed' if args.remove else ''}.\n")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("get", help="claim next work item from a pool")
    g.add_argument("--pool", required=True)
    g.add_argument("--work-id", help="re-fetch a specific work item (for resume)")
    g.add_argument("--timeout", type=int, default=0,
                   help="seconds to wait for work (0 = block forever)")
    g.add_argument("--force", action="store_true",
                   help="with --work-id, take over an active claim from "
                        "another worker (e.g. after a session compaction)")

    s = sub.add_parser("submit", help="submit result for a claimed work item")
    s.add_argument("--pool", required=True)
    s.add_argument("--work-id", required=True)
    s.add_argument("--from", dest="from_file",
                   help="override submission path the work payload named")

    k = sub.add_parser("skip", help="skip a claimed work item")
    k.add_argument("--pool", required=True)
    k.add_argument("--work-id", required=True)
    k.add_argument("--reason")

    sub.add_parser("list", help="list all pools with summary stats")

    st = sub.add_parser("status", help="detailed status for one pool")
    st.add_argument("--pool", required=True)

    cl = sub.add_parser("claims", help="show in-flight (claimed) work items in a pool")
    cl.add_argument("--pool", required=True)

    rc = sub.add_parser("reclaim", help="re-queue a work item whose worker died")
    rc.add_argument("--pool", required=True)
    rc.add_argument("--work-id", required=True)
    rc.add_argument("--force", action="store_true",
                    help="re-queue even if claim is still active")

    rf = sub.add_parser("claim-refresh",
                        help="extend the TTL on an in-flight work item's claim")
    rf.add_argument("--pool", required=True)
    rf.add_argument("--work-id", required=True)
    rf.add_argument("--ttl", type=int,
                    help=f"new TTL in seconds (default: {CLAIM_TTL}, "
                         "or $OPENBRIDGE_CLAIM_TTL)")

    sub.add_parser("redis-up", help="start the openbridge Redis container")
    rd = sub.add_parser("redis-down", help="stop the openbridge Redis container")
    rd.add_argument("--remove", action="store_true",
                    help="also `docker rm` the container (wipes data)")

    args = ap.parse_args()

    handlers = {
        "get": cmd_get, "submit": cmd_submit, "skip": cmd_skip,
        "list": cmd_list, "status": cmd_status, "claims": cmd_claims,
        "reclaim": cmd_reclaim, "claim-refresh": cmd_claim_refresh,
        "redis-up": cmd_redis_up, "redis-down": cmd_redis_down,
    }
    return handlers[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
