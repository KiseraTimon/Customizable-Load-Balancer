"""
SECTOR D — MONITORING, EVALUATION & VISUALIZATION
--------------------------------------------------------------------
Owns: turning what Sectors A/B/C produce into the plots and numbers
that go in the report. This is also where you orchestrate experiments
(spin up N node processes, run a scenario, kill some nodes, repeat).

Expected input: each Java node (Sector A/C) writes one JSON line per
event to a per-node log file, e.g.:

    {"event": "DELIVER", "node": 7, "messageId": "1:42",
    "originMs": 1732200000000, "recvMs": 1732200000031}
    {"event": "SEND",    "node": 1, "messageId": "1:42", "toNode": 4}
    {"event": "NODE_DEATH", "node": 12, "atMs": 1732200005000}

Adjust `parse_log_line` if your team lands on a different schema.

Requires: networkx, matplotlib  (pip install networkx matplotlib)
--------------------------------------------------------------------
"""

import json
import glob
import subprocess
import time
import statistics
from pathlib import Path

import networkx as nx
import matplotlib.pyplot as plt


# 1. LOG PARSING

def parse_log_line(line: str) -> dict:
    return json.loads(line)


def load_events(log_dir: str) -> list[dict]:
    events = []
    for path in glob.glob(f"{log_dir}/node-*.log"):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(parse_log_line(line))
                except json.JSONDecodeError:
                    continue
    return events


# 2. METRICS

def compute_broadcast_latencies(events: list[dict]) -> dict[str, list[int]]:
    """messageId -> list of (recvMs - originMs) across all nodes that delivered it."""
    latencies: dict[str, list[int]] = {}
    for e in events:
        if e.get("event") == "DELIVER":
            delay = e["recvMs"] - e["originMs"]
            latencies.setdefault(e["messageId"], []).append(delay)
    return latencies


def summarize_latency(latencies: list[int]) -> dict:
    if not latencies:
        return {}
    s = sorted(latencies)
    return {
        "min": s[0],
        "max": s[-1],
        "avg": statistics.mean(s),
        "p50": s[len(s) // 2],
        "p95": s[int(len(s) * 0.95) - 1] if len(s) >= 20 else s[-1],
    }


def compute_stress(events: list[dict]) -> dict[str, int]:
    """messageId -> total SEND events (i.e. total network transmissions for that broadcast).
    Compare this across strategies: tree-only should be ~N-1 sends per broadcast,
    tree+gossip repair will be higher — that tradeoff (latency/robustness vs stress)
    is exactly the kind of result a distributed systems report wants."""
    stress: dict[str, int] = {}
    for e in events:
        if e.get("event") == "SEND":
            stress[e["messageId"]] = stress.get(e["messageId"], 0) + 1
    return stress


def compute_recovery_time(events: list[dict], failed_node: int) -> float | None:
    """Time between a NODE_DEATH event and the next successful DELIVER
    downstream of that node's old subtree. Simplified: time between
    NODE_DEATH and the next DELIVER event system-wide after it."""
    death_time = None
    for e in events:
        if e.get("event") == "NODE_DEATH" and e["node"] == failed_node:
            death_time = e["atMs"]
            break
    if death_time is None:
        return None
    for e in sorted((e for e in events if e.get("event") == "DELIVER"), key=lambda x: x["recvMs"]):
        if e["recvMs"] > death_time:
            return e["recvMs"] - death_time
    return None


# 3. VISUALIZATION

def draw_topology(adjacency: dict[int, list[int]], title: str, out_path: str, source_node: int | None = None):
    """adjacency: node -> list of neighbor ids, as produced by Sector B's Topology.
    Exported from Java as JSON, e.g.:  {"1": [2,3], "2": [1,4], ...}"""
    g = nx.Graph()
    for node, neighbors in adjacency.items():
        node = int(node)
        for n in neighbors:
            g.add_edge(node, int(n))

    pos = nx.spring_layout(g, seed=42)
    colors = ["tab:red" if n == source_node else "tab:blue" for n in g.nodes()]

    plt.figure(figsize=(8, 6))
    nx.draw(g, pos, with_labels=True, node_color=colors, node_size=500,
            edge_color="gray", font_color="white", font_size=9)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"Saved topology plot -> {out_path}")


def plot_latency_comparison(strategy_results: dict[str, list[int]], out_path: str):
    """strategy_results: {"random": [...], "shortest_path_tree": [...], "adaptive_gossip": [...]}"""
    plt.figure(figsize=(8, 5))
    labels = list(strategy_results.keys())
    data = [strategy_results[k] for k in labels]
    plt.boxplot(data, labels=labels)
    plt.ylabel("Per-node delivery latency (ms)")
    plt.title("Broadcast latency by overlay topology strategy")
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"Saved comparison plot -> {out_path}")


# 4. EXPERIMENT ORCHESTRATION

def launch_node_cluster(n_nodes: int, base_port: int, jar_path: str, strategy: str, log_dir: str) -> list[subprocess.Popen]:
    """Spins up N real Java node processes (Sectors A+B+C compiled into one jar).
    Each process is genuinely a separate OS process -- this is what makes the
    project a *distributed* system rather than a simulation loop."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    procs = []
    for i in range(n_nodes):
        cmd = [
            "java", "-jar", jar_path,
            "--id", str(i),
            "--port", str(base_port + i),
            "--strategy", strategy,
            "--log", f"{log_dir}/node-{i}.log",
        ]
        procs.append(subprocess.Popen(cmd))
    return procs


def kill_random_node(procs: list[subprocess.Popen], node_index: int):
    """Fault injection: SIGKILL a node mid-broadcast to test Sector C's repair path."""
    procs[node_index].kill()
    print(f"Killed node {node_index} at t={time.time()}")


def run_scenario(n_nodes: int, strategy: str, kill_at_seconds: float | None, kill_index: int, duration_seconds: float, jar_path: str = "overlay-node.jar", base_port: int = 9000):
    log_dir = f"logs/{strategy}"
    procs = launch_node_cluster(n_nodes, base_port, jar_path, strategy, log_dir)
    start = time.time()
    if kill_at_seconds is not None:
        time.sleep(kill_at_seconds)
        kill_random_node(procs, kill_index)
    remaining = max(0, duration_seconds - (time.time() - start))
    time.sleep(remaining)
    for p in procs:
        p.terminate()
    return log_dir


if __name__ == "__main__":
    # Example end-to-end analysis run against already-produced logs.
    events = load_events("logs/adaptive_gossip")
    latencies_by_msg = compute_broadcast_latencies(events)
    all_latencies = [d for vals in latencies_by_msg.values() for d in vals]
    print("Latency summary:", summarize_latency(all_latencies))

    stress = compute_stress(events)
    print("Avg sends per broadcast:", statistics.mean(stress.values()) if stress else "n/a")