"""
MODULE 3b (Neema) — LOAD BALANCER API & ORCHESTRATION

Implements Task 3 of the assignment: the customer-facing load
balancer. Wires together the consistent hash map (Module 2) and
the docker orchestration helper (Module 3a) behind the four
required HTTP endpoints, and runs a background heartbeat thread
that detects failed replicas and auto-respawns them so that N
replicas are (almost) always available.

Endpoints
---------
GET    /rep          -> current replica set
POST   /add          -> scale up
DELETE /rm            -> scale down
GET    /<path>        -> route to a replica via consistent hashing
"""

import os
import time
import random
import hashlib
import threading

import requests
from flask import Flask, request, jsonify

from consistent_hash import ConsistentHashMap
import docker_manager as dm

app = Flask(__name__)


# Configuration (Task 2 defaults, overridable via env vars)
NUM_SLOTS = int(os.environ.get("NUM_SLOTS", 512))
NUM_VIRTUAL = int(os.environ.get("NUM_VIRTUAL", 9))
N_TARGET = int(os.environ.get("N", 3))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", 5))
VALID_SERVER_PATHS = {"home", "heartbeat"}

chm = ConsistentHashMap(num_slots=NUM_SLOTS, num_virtual=NUM_VIRTUAL)
lock = threading.RLock()
servers = {}  # hostname -> numeric server_id used for Phi(i, j)


def hostname_to_id(hostname: str) -> int:
    """Deterministically derive a numeric server id from an
    arbitrary hostname string, so that user-supplied hostnames
    (e.g. 'S5', 'S10') work just as well as auto-generated ones."""
    return int(hashlib.md5(hostname.encode()).hexdigest(), 16) % 10_000


def _add_server_locked(hostname: str = None, spawn: bool = True) -> str:
    if spawn:
        hostname = dm.spawn_server(hostname)
    elif hostname is None:
        hostname = dm.random_hostname()
    sid = hostname_to_id(hostname)
    chm.add_server(hostname, sid)
    servers[hostname] = sid
    return hostname


def _remove_server_locked(hostname: str, kill: bool = True):
    chm.remove_server(hostname)
    servers.pop(hostname, None)
    if kill:
        dm.remove_server(hostname)


# Bootstrap: bring up N_TARGET replicas at startup
def _bootstrap():
    with lock:
        for i in range(1, N_TARGET + 1):
            _add_server_locked(f"Server{i}")


# Endpoints
@app.route("/rep", methods=["GET"])
def rep():
    with lock:
        return jsonify({
            "message": {"N": len(servers), "replicas": list(servers.keys())},
            "status": "successful"
        }), 200


@app.route("/add", methods=["POST"])
def add():
    data = request.get_json(force=True, silent=True) or {}
    n = data.get("n", 0)
    hostnames = data.get("hostnames", [])

    if not isinstance(n, int) or n <= 0:
        return jsonify({"message": "<Error> 'n' must be a positive integer", "status": "failure"}), 400

    if len(hostnames) > n:
        return jsonify({
            "message": "<Error> Length of hostname list is more than newly added instances",
            "status": "failure"
        }), 400

    with lock:
        to_add = list(hostnames) + [None] * (n - len(hostnames))
        for h in to_add:
            _add_server_locked(h)
        return jsonify({
            "message": {"N": len(servers), "replicas": list(servers.keys())},
            "status": "successful"
        }), 200


@app.route("/rm", methods=["DELETE"])
def rm():
    data = request.get_json(force=True, silent=True) or {}
    n = data.get("n", 0)
    hostnames = data.get("hostnames", [])

    if not isinstance(n, int) or n <= 0:
        return jsonify({"message": "<Error> 'n' must be a positive integer", "status": "failure"}), 400

    if len(hostnames) > n:
        return jsonify({
            "message": "<Error> Length of hostname list is more than removable instances",
            "status": "failure"
        }), 400

    with lock:
        to_remove = [h for h in hostnames if h in servers]
        pool = [h for h in servers if h not in to_remove]
        random.shuffle(pool)
        while len(to_remove) < n and pool:
            to_remove.append(pool.pop())

        for h in to_remove:
            _remove_server_locked(h)

        return jsonify({
            "message": {"N": len(servers), "replicas": list(servers.keys())},
            "status": "successful"
        }), 200


@app.route("/<path:path>", methods=["GET"])
def route_request(path):
    if path not in VALID_SERVER_PATHS:
        return jsonify({
            "message": f"<Error> '/{path}' endpoint does not exist in server replicas",
            "status": "failure"
        }), 400

    request_id = random.randint(100000, 999999)
    with lock:
        target = chm.get_server(request_id)

    if target is None:
        return jsonify({"message": "<Error> No servers available", "status": "failure"}), 400

    try:
        r = requests.get(f"http://{target}:5000/{path}", timeout=3)
        return (r.content, r.status_code, {"Content-Type": "application/json"})
    except Exception as exc:
        return jsonify({
            "message": f"<Error> Failed to reach server '{target}': {exc}",
            "status": "failure"
        }), 500


# Background heartbeat / self-healing loop
def heartbeat_loop():
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with lock:
            hosts = list(servers.keys())
        for h in hosts:
            if not dm.is_alive(h):
                with lock:
                    if h in servers:  # still tracked (not removed meanwhile)
                        print(f"[heartbeat] '{h}' failed liveness check, respawning...", flush=True)
                        _remove_server_locked(h, kill=True)
                        new_host = _add_server_locked(None)
                        print(f"[heartbeat] replacement replica '{new_host}' is up", flush=True)


_bootstrap()
threading.Thread(target=heartbeat_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
