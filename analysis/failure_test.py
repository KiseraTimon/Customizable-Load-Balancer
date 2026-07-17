"""
MODULE 4c (Neema) — EXPERIMENT A-3

Exercises every load-balancer endpoint (/rep, /add, /rm, /<path>)
and then simulates a server failure by force-killing one of the
replica containers directly via `docker kill`, polling /rep to
measure how long the load balancer takes to notice and spawn a
replacement.

Requires the docker CLI to be available on the machine running
this script (i.e. run it on the host, not inside a container).

Usage:
    python3 analysis/failure_test.py [--url http://localhost:5000]
"""

import argparse
import subprocess
import time

import requests


def section(title):
    print(f"\n{'='*10} {title} {'='*10}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:5000")
    args = parser.parse_args()
    url = args.url

    section("GET /rep")
    r = requests.get(f"{url}/rep")
    print(r.status_code, r.json())
    initial_replicas = set(r.json()["message"]["replicas"])

    section("POST /add (n=2)")
    r = requests.post(f"{url}/add", json={"n": 2, "hostnames": []})
    print(r.status_code, r.json())

    section("GET /home (routed request)")
    r = requests.get(f"{url}/home")
    print(r.status_code, r.json())

    section("GET /nonexistent (expect 400 error)")
    r = requests.get(f"{url}/nonexistent")
    print(r.status_code, r.json())

    section("DELETE /rm (n=1)")
    r = requests.delete(f"{url}/rm", json={"n": 1, "hostnames": []})
    print(r.status_code, r.json())

    section("Failure & recovery timing")
    replicas = requests.get(f"{url}/rep").json()["message"]["replicas"]
    if not replicas:
        print("No replicas to kill, aborting failure test.")
        return
    victim = replicas[0]
    print(f"Killing container '{victim}' via docker kill ...")
    t0 = time.time()
    subprocess.run(["docker", "kill", victim], check=False)

    recovered = False
    while time.time() - t0 < 60:
        try:
            reps = requests.get(f"{url}/rep", timeout=3).json()["message"]["replicas"]
        except Exception:
            reps = []
        if victim not in reps and len(reps) >= len(replicas):
            recovered = True
            elapsed = time.time() - t0
            print(f"Recovered in {elapsed:.2f}s. New replica set: {reps}")
            break
        time.sleep(0.5)

    if not recovered:
        print("Load balancer did not recover within 60s -- check HEARTBEAT_INTERVAL / logs.")


if __name__ == "__main__":
    main()
