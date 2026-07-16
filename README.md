# Overlay Network Topology Optimisation for Low-Latency Broadcast

A small distributed systems project that builds, runs, and measures a real
multi-process broadcast overlay: independent Java node processes talk to
each other over TCP, organise themselves into a latency-aware overlay
topology, disseminate messages across that overlay with a
tree-push-plus-gossip-repair protocol, and survive node failure through
automatic re-parenting. A Python analysis layer then turns the raw event
logs those nodes produce into the latency, stress, and recovery metrics
that make up the project's report.

The system is deliberately split into four cooperating "sectors," each
owning one layer of the stack, plus a Java integration entry point that
wires them together end to end.

---

## Introduction

Broadcasting a message to every node in a distributed system sounds
trivial until you ask *how fast* and *at what cost*. A naive approach —
the source sends directly to everyone — doesn't scale past a handful of
peers, and a single spanning tree is fast and cheap but has zero
redundancy: kill one interior node and its entire subtree goes dark. This
project explores that trade-off directly. It implements several ways of
building the overlay that messages travel across (random mesh, shortest-
path tree, degree-bounded tree, and a self-optimising gossip-based
overlay), disseminates real messages across a real network of OS
processes running that overlay, deliberately kills nodes mid-broadcast,
and measures what happens: how long delivery takes, how many messages the
network had to push to get there, and how long the system takes to
recover once redundancy kicks in.

## Motivation

Overlay networks sit underneath almost everything that needs to move data
to many recipients quickly and reliably — content distribution, blockchain
gossip protocols, pub/sub systems, multiplayer game state sync, and
cluster coordination all rest on some variant of this problem. The
interesting engineering tension is always the same: a pure spanning tree
minimises the number of messages sent (a broadcast to *N* nodes needs only
*N − 1* transmissions) but is fragile, while adding redundancy (extra
edges, periodic anti-entropy gossip) buys resilience at the cost of extra
network "stress." A project that only simulates this trade-off in a
single process is much less convincing than one that actually spins up
independent processes, sends bytes over real sockets with real scheduling
jitter, and kills a process out from under the system while it's mid-flight.
That's the bar this project targets: a genuinely distributed system, not a
loop pretending to be one.

## Concept & Justification

The design separates *how nodes talk to each other* from *what shape the
overlay is* from *how messages actually propagate*, because those are
three genuinely different concerns and conflating them makes the system
hard to reason about or extend:

- **Transport** doesn't know or care what a "broadcast" is — it just
  moves bytes between processes and models the network delay a broadcast
  protocol will experience.
- **Topology construction** doesn't know or care how bytes move — it only
  needs a latency matrix and produces a graph describing who should talk
  to whom.
- **Broadcast dissemination** doesn't know or care how the topology was
  built or how bytes move — it just needs a tree structure to push along
  and a way to send.

This separation is also what makes the experiment matrix possible: any
topology strategy from Sector B can be dropped in front of the same
Sector C dissemination logic and the same Sector A transport, so the
report's comparisons ("adaptive gossip vs. degree-bounded tree vs.
shortest-path tree vs. random") are true apples-to-apples comparisons of
one variable at a time rather than four different pipelines.

## System Architecture

```
                        ┌─────────────────────────────┐
                        │   Sector D (Python, main.py) │
                        │  Orchestration · Metrics ·   │
                        │  Plotting · Fault Injection   │
                        └───────────────┬───────────────┘
                                        │ spawns / kills processes,
                                        │ reads JSON event logs
                        ┌───────────────▼───────────────┐
                        │      Main.java (integration)   │
                        └───────┬───────────┬───────────┘
                                │           │
                ┌───────────────▼──┐   ┌────▼────────────────┐
                │ Sector A          │   │ Sector B             │
                │ OverlayTransport  │   │ TopologyOptimizer     │
                │ Node / Message /  │   │ Random · ShortestPath │
                │ LatencyOracle     │   │ DegreeBounded ·       │
                │                   │   │ AdaptiveGossip        │
                └────────┬──────────┘   └──────────┬────────────┘
                         │  Node.send()/onMessage()  │ Topology
                         └────────────┬──────────────┘
                                      ▼
                        ┌─────────────────────────────┐
                        │ Sector C — BroadcastEngine    │
                        │ BroadcastNode · GossipRepair · │
                        │ FailureRepair                  │
                        └─────────────────────────────┘
```

Each node in the running system is one JVM process. Sector A gives every
process a socket-based mailbox; Sector B is consulted once (or
continuously, for the adaptive strategy) to decide the shape of the
overlay; Sector C runs inside every node and is the thing that actually
decides "who do I forward this message to next." Sector D lives outside
the JVM entirely, treating the whole cluster as a black box it can start,
observe through log files, and kill pieces of.

## How the Project Works

At a high level, one experimental run looks like this: a fixed number of
node processes are started and told about each other's addresses (Sector
A). A latency matrix is derived for the whole cluster from a synthetic,
deterministic latency oracle so the experiment is repeatable without real
geographic distribution. That matrix is handed to one of Sector B's
topology strategies, which returns a graph describing the overlay — for
tree strategies this is turned into an explicit parent/children map via a
breadth-first walk from the source. Every node's Sector C
`BroadcastNode` is configured with its place in that tree. The source
node then calls `initiateBroadcast`, which is really just "process a
message I made myself" — from there, the same dedup-and-relay code path
that handles inbound network messages handles the locally originated one,
pushing it out to tree children over Sector A's transport. As messages
travel, every send and every first-time delivery is appended as a JSON
line to that node's private log file. Once the run finishes (or a node
was killed partway through to test recovery), Sector D reads all of the
per-node logs back in, reconstructs per-message delivery latency and
per-message send counts, and produces the plots and summary statistics
that go into the report.

## Core Modules

### Sector A — Overlay Transport

`OverlayTransport.java` is the physical substrate everything else is
built on, and the only place in the codebase that touches a raw socket.

- **`Node`** is a literal OS-level participant: it opens a
  `ServerSocket`, accepts inbound connections on background daemon
  threads, and exposes only two operations to the rest of the system —
  `send(peerId, message)` and `onMessage(handler)`. Outbound connections
  are cached per peer and lazily reconnected if a send fails, which
  doubles as an implicit failure signal that Sector C's re-parenting
  logic reacts to.
- **`Message`** is the wire format: sender id, a string `type`
  (`BROADCAST`, `TOPOLOGY_UPDATE`, `PING`, …), a payload string, and an
  origin timestamp, serialized as a single line delimited by a
  non-printable `\u0001` separator so payload content never collides
  with the framing.
- **`LatencyOracle`** is what makes single-laptop distributed
  experimentation possible. Rather than needing real geographically
  distributed machines, every node is assigned a synthetic 2D coordinate
  derived deterministically from a global seed and its own node id (so
  every process independently computes the *same* coordinate for a given
  node without any shared state), and latency between two nodes is
  Euclidean distance scaled to milliseconds plus a small amount of
  symmetric jitter seeded from the *pair* of ids so both endpoints agree
  on the delay. `Node.send` schedules the actual socket write after this
  synthetic delay elapses, so the shaped delay is a genuine scheduling
  delay on the sending side, not a cosmetic number added after the fact.

### Sector B — Topology Optimization

`TopologyOptimizer.java` is the project's core intellectual contribution
— everything here is pure graph construction over a latency matrix, with
no networking involved, which is what makes it independently testable and
comparable across strategies.

- **`Topology`** is the common output type for every strategy: an
  adjacency map of node → set of neighbours, with `degree()` doubling as
  a fan-out proxy.
- **`randomOverlay`** is the control group — each node connects to a
  fixed number of random peers, giving a baseline with no latency
  awareness at all.
- **`shortestPathTree`** runs Dijkstra from the broadcast source over the
  latency matrix, producing the tree with the lowest possible per-node
  latency. Its weakness, noted directly in the code, is that the source
  (or any single well-connected node) can end up with an unrealistically
  large fan-out, which real broadcast sources with bounded upload
  bandwidth can't actually sustain.
- **`degreeBoundedTree`** fixes that by growing a Prim-style minimum
  spanning tree while capping how many children any one node may accept
  (`maxFanOut`). This is the strategy `Main.java` actually wires up for
  its demo run; it produces a broadcast tree that still favours short
  edges but never collapses onto a single hub. If every in-tree node
  saturates its fan-out before all nodes are reached, the method exits
  early and leaves nodes unreached — a real failure mode worth reporting
  on rather than silently working around.
- **`AdaptiveGossipOptimizer`** is the piece intended to make the overlay
  self-optimising rather than a one-shot batch computation: each node
  periodically samples a random current neighbour, inspects *that
  neighbour's* neighbours (a gossiped, not globally known, candidate
  set), and swaps in any candidate whose latency beats the node's
  current worst edge, subject to the fan-out cap. Each round performs at
  most one improving swap, which keeps the topology's evolution
  incremental and stable rather than thrashing, and lets the overlay
  converge toward a locality-clustered structure purely from local
  information — no node ever sees the whole graph.
- **`treeDiameterMs`** walks the tree from the source outward and reports
  the longest source-to-leaf path, which is the theoretical best-case
  broadcast latency a given topology can offer before any protocol
  overhead is added on top.

### Sector C — Broadcast Engine

`BroadcastEngine.java` is where a topology becomes an actual broadcast
protocol running independently on every node.

- **`BroadcastMessage`** is the logical envelope carried inside Sector
  A's `Message.payload` — origin node, sequence number, content, a
  derived `messageId` (`originId:seqNum`), and a hop counter.
- **`BroadcastNode.handleIncoming`** is the heart of the protocol: it
  adds the message id to a per-node `seenMessages` set, and if that add
  fails (the id was already present) it stops immediately. This single
  dedup check is what makes it safe to feed the same method messages
  arriving from three different sources — the tree, gossip repair, and a
  locally originated broadcast — without ever delivering or re-relaying
  the same message twice, and it's also what bounds "stress": no matter
  how many redundant copies of a message arrive at a node, only the
  first ever triggers further sends.
- Delivery fires an `onDeliver` callback (wired in `Main.java` to log a
  `DELIVER` event) and then the message is pushed to every tree child
  except whichever peer it just arrived from, using the injected
  `BiSender` abstraction so this class never touches Sector A directly.
- **`GossipRepair`** is the anti-entropy safety net: on a fixed period,
  each node picks one random peer from a wider gossip peer set (not just
  its tree neighbours), asks it which message ids it has seen, computes
  the set difference, and pulls anything missing through the message-id
  route. Because pulled messages flow through the same
  `handleIncoming` dedup path, this composes safely with the tree push
  path rather than needing separate bookkeeping.
- **`findReparentTarget`** is the failure-recovery primitive: given a
  node whose parent has stopped responding (detected via Sector A's
  send-failure signal), it walks up the last-known parent chain until it
  finds an ancestor still present in the current alive-node set, capped
  at 20 hops to avoid an infinite walk on a corrupted map. This lets
  orphaned children re-attach to the nearest living ancestor instead of
  waiting for the topology to be rebuilt from scratch.

### Sector D — Monitoring & Evaluation

`main.py` is intentionally decoupled from the JVM — it never imports or
links against the Java code, it only agrees on a JSON-lines log schema
and a JSON adjacency schema, which keeps the analysis layer usable even
if the Java implementation details change.

- **Log parsing** (`load_events`) reads every `node-*.log` file in a
  results directory, tolerating malformed or blank lines rather than
  failing the whole run over one bad record.
- **`compute_broadcast_latencies`** groups `DELIVER` events by message id
  and returns each node's `recvMs − originMs` delay, and
  **`summarize_latency`** reduces that into min/max/mean/p50/p95 —
  exactly the statistics used in the sample run captured below.
- **`compute_stress`** counts `SEND` events per message id, i.e. the
  total number of network transmissions one broadcast actually cost —
  the number to compare a lean tree-only strategy against a
  tree-plus-gossip strategy on.
- **`compute_recovery_time`** measures, in its current simplified form,
  the gap between a `NODE_DEATH` event for a given node and the next
  system-wide `DELIVER` event after it — a first-pass proxy for "how
  long until the system is making forward progress again" that a more
  advanced version could narrow to deliveries specifically downstream of
  the failed node's old subtree.
- **Visualization** covers both a single topology snapshot
  (`draw_topology`, via `networkx`/`matplotlib`, highlighting the
  broadcast source node) and a cross-strategy latency comparison
  (`plot_latency_comparison`, a box plot per strategy).
- **Orchestration** (`launch_node_cluster`, `kill_random_node`,
  `run_scenario`) is what makes the fault-injection experiments
  possible from Python: it spawns real `java -jar` subprocesses per
  node, optionally `kill()`s one mid-run to simulate a hard crash, waits
  out the scenario duration, and terminates everything cleanly,
  returning the log directory for the metrics functions above to
  consume.

### Main Integration Layer

`Main.java` is the glue that proves the four sectors actually work
together as one system, and doubles as the project's smallest possible
end-to-end demo (5 nodes, source node 1, one broadcast message). It:

1. Boots a `LatencyOracle` and five Sector A `Node`s, fully meshes their
   peer address books, and starts each node listening.
2. Builds a full latency matrix from the oracle and asks Sector B for a
   `degreeBoundedTree` (fan-out cap of 2), then converts that undirected
   topology into an explicit parent/children map via a breadth-first
   walk from the source — the shape Sector C's `BroadcastNode.setTree`
   expects.
3. For each node, wires three bridges: a `BiSender` that logs a `SEND`
   JSON event before handing bytes to Sector A; an `onDeliver` callback
   that logs a `DELIVER` JSON event (using a globally tracked origin
   timestamp so latency is measured from true message creation, not from
   whenever each node happens to notice it); and a Sector A
   `onMessage` handler that reconstructs a `BroadcastMessage` from the
   wire payload and feeds it into Sector C's `handleIncoming`.
4. Kicks off the broadcast from the source, waits two seconds for it to
   fully propagate and for gossip/repair activity to settle, then shuts
   every node down and closes the log writers.

Note that this integration run labels its log directory `adaptive_gossip`
but actually constructs a `degreeBoundedTree` and never invokes
`AdaptiveGossipOptimizer` or starts a `GossipRepair` loop — it's a wiring
demo for the tree-push path specifically. A full experiment matrix would
extend this same wiring pattern to also drive the other three strategies
and to actually run the adaptive optimizer's background rounds and the
gossip repair scheduler for that comparison to be meaningful.

## End-to-End Broadcast Lifecycle

A single message's journey through the system, from source to full
delivery, goes through five distinct stages: the source node constructs a
`BroadcastMessage` and calls `initiateBroadcast`, which routes straight
into the same `handleIncoming` dedup-and-relay logic every other node
uses; the message is marked seen, delivered locally (firing the
`onDeliver` callback and its `DELIVER` log line), and pushed to every
tree child via the `BiSender`, each push also producing a `SEND` log
line; Sector A schedules each of those sends after a synthetic
latency delay derived from the two endpoints' coordinates, so the bytes
genuinely arrive later rather than instantly; on the receiving side,
Sector A's `onMessage` handler decodes the payload back into a
`BroadcastMessage` and calls that node's own `handleIncoming`, repeating
the same dedup check, delivery, and further relay to its own children
(excluding whichever peer it just received from, which prevents an
immediate echo back up the tree); and in parallel, if `GossipRepair` is
running, any node that hasn't yet received a message can independently
pull it in from a random gossip peer, which is what keeps a broadcast
complete even if one interior tree link is severed mid-flight.

## Experimental Strategies

The project is built to compare four ways of shaping the same overlay
under the same transport and dissemination logic:

- **Random overlay** — no latency awareness, a fixed-fanout random mesh;
  the baseline everything else should beat.
- **Shortest-path tree** — Dijkstra-optimal per-node latency, at the risk
  of an unrealistically high-fanout source.
- **Degree-bounded tree** — a fan-out-capped MST-style tree that trades a
  small amount of latency optimality for a source (and every node) that
  never needs more upload bandwidth than the cap allows; this is what
  the current `Main.java` demo exercises.
- **Adaptive gossip optimizer** — the degree-bounded tree (or any
  starting topology) continuously improved in the background by local,
  no-global-view swaps, intended to converge toward a
  locality-clustered structure over time without ever recomputing from
  scratch.

Layered on top of whichever topology is chosen, gossip anti-entropy
repair can be turned on or off, which is the second axis of comparison:
tree-only dissemination should approach the theoretical minimum of
*N − 1* sends per broadcast but has no redundancy, while tree-plus-gossip
costs more sends per broadcast in exchange for surviving node failure —
exactly the latency/stress-vs-robustness trade-off the report is built
to demonstrate.

## Performance Metrics

Three numbers drive the analysis, all computed by Sector D directly from
the JSON event logs rather than measured in-process, so they reflect
what actually happened on the wire:

- **Delivery latency** — per node, per message, `recvMs − originMs`,
  summarized as min/max/mean/p50/p95 across every delivery in a run.
- **Stress** — the count of `SEND` events tied to a given message id,
  i.e. how many total network transmissions one broadcast required; a
  pure tree should sit near *N − 1*, and any excess above that is the
  cost gossip repair is adding.
- **Recovery time** — the elapsed time between a `NODE_DEATH` event and
  the system's next successful delivery, used to characterise how
  quickly the overlay routes around a failure once one is injected.

## Explanation of Results

The two sample runs captured for this project show the pipeline working
end to end, and are worth reading together.

The Java run (`Main.java`) starts five nodes, builds a degree-bounded
tree rooted at node 1, and successfully delivers "Project Integration
Successful!" to all five nodes (including the source itself, which
delivers to itself as part of the same dedup path everything else uses).
The delivery order printed to the console — node 1, then 4, 2, 5, 3 — is
simply a reflection of the synthetic per-edge latencies assigned by the
`LatencyOracle`: nodes with shorter synthetic latency from their parent
in the tree receive and log their `DELIVER` event sooner, even though all
four non-source deliveries were triggered by essentially the same burst
of pushes from the source.

Feeding that run's logs into `main.py` produces a latency summary of
minimum 8 ms, maximum 73 ms, mean 50 ms, median (p50) 54 ms, and — because
the sample has fewer than 20 data points — a p95 that falls back to the
maximum observed value (73 ms), exactly as `summarize_latency`'s
fallback branch is written to do for small samples. The spread between
8 ms and 73 ms across only four non-source deliveries is consistent with
the tree's structure: a node attached close to the source in latency
terms delivers fast, while a node reached only after an extra hop through
an intermediate relay accumulates that relay's own synthetic delay on
top, which is exactly the "diameter" effect `treeDiameterMs` is designed
to quantify at the topology level.

The reported "avg sends per broadcast" of 4 matches the tree's edge count
exactly: five nodes connected as a spanning tree have four edges, and
because `handleIncoming`'s dedup check ensures a message is relayed
along the tree exactly once per edge (no re-sends, no gossip repair
active in this particular run), the total `SEND` count for the one
message broadcast is precisely *N − 1* = 4. That is the strongest
evidence the sample run offers that the tree-push path is behaving
correctly: no wasted transmissions, no missed nodes, and a stress figure
that sits exactly at the theoretical floor for this topology size. A
meaningful next data point for the report would be re-running the same
scenario with `GossipRepair` active (and ideally with a node killed
mid-broadcast) to see that floor rise in exchange for the system now
surviving a failure it couldn't have survived on the tree alone.

## Project Structure

```
.
├── Main.java                 # Integration layer: wires Sectors A, B, C together
├── OverlayTransport.java     # Sector A — Node, Message, LatencyOracle
├── TopologyOptimizer.java    # Sector B — topology strategies + adaptive optimizer
├── BroadcastEngine.java      # Sector C — dissemination, gossip repair, re-parenting
├── main.py                   # Sector D — log parsing, metrics, plotting, orchestration
└── logs/
    └── <strategy-name>/
        └── node-<id>.log     # One JSON-lines event log per node, per run
```

The Java classes are namespaced under `com.transport`, `com.optimizer`,
and `com.engine` respectively; a real build would compile them into a
single runnable jar (referenced in `main.py` as `overlay-node.jar`) so
Sector D can spawn them as `java -jar overlay-node.jar --id … --port …
--strategy … --log …` per the CLI shape `launch_node_cluster` already
assumes.

## Setup & Installation

The Java side needs JDK 17 or newer (no external dependencies — the
transport layer is built entirely on `java.net`/`java.io`/
`java.util.concurrent`) and can be compiled with `javac` or opened
directly in an IDE such as IntelliJ. The Python side needs Python 3.10+
(for the `list[dict]`-style built-in generics used throughout `main.py`)
plus two third-party packages:

```
pip install networkx matplotlib
```

## Running the Project

For a quick end-to-end sanity check, compile and run `Main.java` directly
— it starts five nodes, runs one broadcast, and writes its logs to
`logs/adaptive_gossip/` before shutting everything down:

```
javac -d out src/com/transport/OverlayTransport.java \
             src/com/optimizer/TopologyOptimizer.java \
             src/com/engine/BroadcastEngine.java \
             src/Main.java
java -cp out Main
```

Once logs exist, `main.py` can be run standalone to summarize them:

```
python main.py
```

which loads `logs/adaptive_gossip/`, prints the latency summary, and
prints the average number of sends per broadcast, matching the sample
output captured earlier. For a full multi-strategy, fault-injection
experiment, drive `run_scenario` from a script or REPL — it will spawn
real node processes for a given strategy, optionally kill one part-way
through, and hand back the log directory to feed into
`compute_recovery_time` and `plot_latency_comparison`.

## Future Improvements

Several follow-ups would make the project's story more complete: wiring
`Main.java` (or a new driver) to actually exercise all four Sector B
strategies and to genuinely start `AdaptiveGossipOptimizer` background
rounds and a `GossipRepair` scheduler rather than only demonstrating pure
tree-push; refining `compute_recovery_time` to look specifically at
deliveries downstream of the failed node's old subtree rather than the
next system-wide delivery, which would make the recovery metric far more
precise on a larger cluster; adding a `TOPOLOGY_UPDATE` message type over
Sector A so the adaptive optimizer's gossiped neighbour exchange happens
over the real network rather than only being exercised as an in-process
convenience method; and scaling the sample experiments up from five nodes
to a size where the p95 latency branch in `summarize_latency` reflects a
genuine percentile rather than falling back to the observed maximum.