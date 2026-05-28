"""Bootstrap a local Redis via Docker so openbridge "just works".

Both the daemon side (`openbridge.Bridge`) and the Claude-side CLI
(`bridge`) call `ensure_redis()` before touching Redis. It:

  1. Tries a TCP connect to the configured URL. If that works → done.
  2. Else, `docker ps -a` to see if an existing container of the expected
     name is stopped — if so, `docker start` it.
  3. Else, `docker run -d --name openbridge-redis -p <host>:<port>:6379
     redis:7-alpine`.
  4. Polls the port until it accepts a PING, up to a timeout.

The container name is shared across skills on purpose — one Redis serves
all skills, namespaced by `openbridge:<name>:*` keys. Auto-bootstrap is
skipped if $OPENBRIDGE_NO_DOCKER=1 (e.g. on a server with managed Redis).
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
from urllib.parse import urlparse


CONTAINER_NAME = os.environ.get("OPENBRIDGE_REDIS_CONTAINER", "openbridge-redis")
IMAGE = os.environ.get("OPENBRIDGE_REDIS_IMAGE", "redis:7-alpine")
START_TIMEOUT_S = 30


def _parse(url: str) -> tuple[str, int]:
    p = urlparse(url)
    return (p.hostname or "localhost", p.port or 6379)


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _docker_available() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        subprocess.run(["docker", "info"], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       timeout=5)
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False


def _container_state(name: str) -> str | None:
    """Returns "running", "stopped", or None (no such container)."""
    try:
        out = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Status}}", name],
            check=True, capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        return out or None
    except subprocess.CalledProcessError:
        return None


def _wait_for_port(host: str, port: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _port_open(host, port):
            return True
        time.sleep(0.3)
    return False


def ensure_redis(redis_url: str | None = None, *, quiet: bool = False) -> str:
    """Ensure a Redis is reachable at `redis_url`. Start a docker container
    if not. Returns the URL actually in use (same as input, for chaining).

    Raises RuntimeError if Redis isn't reachable and we can't bring one up
    (no docker, container start failed, etc.).
    """
    url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    host, port = _parse(url)

    if _port_open(host, port):
        return url

    if os.environ.get("OPENBRIDGE_NO_DOCKER") == "1":
        raise RuntimeError(
            f"Redis at {host}:{port} unreachable and OPENBRIDGE_NO_DOCKER=1 "
            "— start Redis yourself or unset the env var.")

    # Only safe to docker-run if we'd be hitting localhost — refuse to start
    # a local container for a remote-URL miss.
    if host not in ("localhost", "127.0.0.1", "::1"):
        raise RuntimeError(
            f"Redis at {host}:{port} unreachable. Refusing to start a local "
            "container for a non-local URL — fix the URL or start Redis there.")

    if not _docker_available():
        raise RuntimeError(
            f"Redis at {host}:{port} unreachable and docker isn't available. "
            "Install docker or start Redis yourself.")

    def _log(msg: str) -> None:
        if not quiet:
            print(f"[openbridge] {msg}", file=sys.stderr)

    state = _container_state(CONTAINER_NAME)
    if state == "running":
        # Container claims running but port wasn't open — wait briefly; if
        # still nothing, that's a real problem (different port mapping?).
        _log(f"container {CONTAINER_NAME} is running; waiting for port {port}...")
    elif state == "stopped" or state in {"exited", "created"}:
        _log(f"starting existing container {CONTAINER_NAME}...")
        subprocess.run(["docker", "start", CONTAINER_NAME], check=True,
                       stdout=subprocess.DEVNULL)
    else:
        _log(f"launching new container {CONTAINER_NAME} ({IMAGE}) on port {port}...")
        subprocess.run(
            ["docker", "run", "-d", "--name", CONTAINER_NAME,
             "-p", f"127.0.0.1:{port}:6379",
             "--restart", "unless-stopped",
             IMAGE],
            check=True, stdout=subprocess.DEVNULL,
        )

    if not _wait_for_port(host, port, START_TIMEOUT_S):
        raise RuntimeError(
            f"Redis container {CONTAINER_NAME} didn't accept connections on "
            f"{host}:{port} within {START_TIMEOUT_S}s. "
            f"Check: docker logs {CONTAINER_NAME}")

    _log(f"Redis ready at {host}:{port}")
    return url


def stop_redis(*, remove: bool = False) -> None:
    """Stop (and optionally remove) the openbridge Redis container."""
    if _container_state(CONTAINER_NAME) is None:
        return
    subprocess.run(["docker", "stop", CONTAINER_NAME],
                   check=False, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
    if remove:
        subprocess.run(["docker", "rm", CONTAINER_NAME],
                       check=False, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
