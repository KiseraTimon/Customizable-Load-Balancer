"""
=========================================================
 MODULE 4b (Ivy) — EXPERIMENT A-2
=========================================================
Starting from whatever replica set currently exists, scales the
load balancer from N=2 up to N=6 (using /add and /rm), firing
10,000 GET /home requests at each step, and plots the average
load per server and its standard deviation vs N -- a proxy for
how evenly (and how consistently) the load balancer scales.

Usage:
    python3 analysis/scale_test.py [--url http://localhost:5000] [--n-requests 10000]
"""

import argparse
import concurrent.futures
import statistics
from collections import Counter
import time

import requests
import matplotlib.pyplot as plt


def get_replicas(url):
    r = requests.get(f"{url}/rep", timeout=5).json()
    return r["message"]["replicas"]


def set_replica_count(url, target_n):
    current = get_replicas(url)
    diff = target_n - len(current)
    if diff > 0:
        requests.post(f"{url}/add", json={"n": diff, "hostnames": []}, timeout=10)
    elif diff < 0:
        requests.delete(f"{url}/rm", json={"n": -diff, "hostnames": []}, timeout=10)


# Use a session object to reuse connections instead of recreating them
def make_request(session, url):
    try:
        r = session.get(f"{url}/home", timeout=5)
        return r.json()["message"].split("Server: ")[-1]
    except Exception:
        return None


# Configure a connection pool that matches your thread count
def run_load(url, n_requests, workers):
    session = requests.Session()

    # Set pool size to match workers so threads do not block waiting for a socket
    adapter = requests.adapters.HTTPAdapter(pool_connections=workers, pool_maxsize=workers)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        # Pass the session to the request function
        results = list(pool.map(lambda _: make_request(session, url), range(n_requests)))

    # Clean up the session once the load run finishes
    session.close()
    ok = [r for r in results if r]
    return Counter(ok)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:5000")
    parser.add_argument("--n-requests", type=int, default=10000)
    parser.add_argument("--workers", type=int, default=100)
    parser.add_argument("--out", default="A2_scalability.png")
    args = parser.parse_args()

    ns, avgs, stdevs = [], [], []

    for n in range(2, 7):
        print(f"\n--- Scaling to N={n} ---")
        set_replica_count(args.url, n)
        counts = run_load(args.url, args.n_requests, args.workers)
        loads = list(counts.values())
        avg = statistics.mean(loads) if loads else 0
        std = statistics.pstdev(loads) if len(loads) > 1 else 0
        print(f"N={n}: avg load={avg:.1f}, stdev={std:.1f}, servers seen={len(counts)}")
        ns.append(n)
        avgs.append(avg)
        stdevs.append(std)

        time.sleep(10)

    fig, ax1 = plt.subplots(figsize=(8, 5))
    ax1.plot(ns, avgs, marker="o", color="#4C72B0", label="Average load per server")
    ax1.set_xlabel("N (number of server replicas)")
    ax1.set_ylabel("Average requests handled per server", color="#4C72B0")
    ax1.tick_params(axis="y", labelcolor="#4C72B0")

    ax2 = ax1.twinx()
    ax2.plot(ns, stdevs, marker="s", color="#DD8452", label="Load stdev")
    ax2.set_ylabel("Standard deviation of load", color="#DD8452")
    ax2.tick_params(axis="y", labelcolor="#DD8452")

    plt.title("A-2: Scalability -- average load & balance vs N")
    fig.tight_layout()
    plt.savefig(args.out)
    print(f"\nChart saved to {args.out}")


if __name__ == "__main__":
    main()
