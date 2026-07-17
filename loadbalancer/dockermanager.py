"""
DOCKER ORCHESTRATION HELPER

Wraps the Docker Engine API (via the `docker` Python SDK) so the
load balancer can spawn/remove/health-check server-replica
containers on the shared `net1` network. The load balancer
container is started with the host's docker socket mounted in
(and `privileged: true`), which is what gives it permission to
manage sibling containers -- see docker-compose.yml.
"""

import os
import random
import string

import docker
import requests

NETWORK = os.environ.get("DOCKER_NETWORK", "net1")
SERVER_IMAGE = os.environ.get("SERVER_IMAGE", "ds-server:latest")

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = docker.from_env()
    return _client


def random_hostname(prefix: str = "S") -> str:
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{prefix}_{suffix}"


def spawn_server(hostname: str = None) -> str:
    """Start a new server-replica container attached to `net1`,
    with SERVER_ID set to its own hostname. Returns the hostname
    actually used (random one generated if none was supplied)."""
    client = _get_client()
    hostname = hostname or random_hostname()

    # If a stale container with this name already exists (e.g. a
    # crashed one we're about to replace), clean it up first.
    try:
        old = client.containers.get(hostname)
        old.remove(force=True)
    except docker.errors.NotFound:
        pass

    client.containers.run(
        SERVER_IMAGE,
        name=hostname,
        hostname=hostname,
        network=NETWORK,
        environment={"SERVER_ID": hostname},
        detach=True,
    )
    return hostname


def remove_server(hostname: str):
    """Stop and delete a server-replica container."""
    client = _get_client()
    try:
        c = client.containers.get(hostname)
        c.remove(force=True)
    except docker.errors.NotFound:
        pass


def is_alive(hostname: str, port: int = 5000, path: str = "/heartbeat", timeout: float = 2.0) -> bool:
    """Health check used by the heartbeat loop. Hits the server's
    /heartbeat endpoint over the docker-internal DNS name."""
    try:
        r = requests.get(f"http://{hostname}:{port}{path}", timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False
