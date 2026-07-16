import com.optimizer.TopologyOptimizer;
import com.transport.OverlayTransport.*;
import com.optimizer.TopologyOptimizer.*;
import com.engine.BroadcastEngine.*;

import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting Distributed Broadcast Simulation...\n");

        int NUM_NODES = 5;
        int SOURCE_NODE = 1;
        int BASE_PORT = 9000;

        // 1. SECTOR A: Initializing Physical Transport & Latency
        System.out.println("1. Spinning up Transport Layer (Sector A)");
        LatencyOracle oracle = new LatencyOracle(42L, 0.05, 5.0);

        Map<Integer, Node> physicalNodes = new HashMap<>();
        Set<Integer> nodeIds = new HashSet<>();

        // Create and start physical nodes
        for (int i = 1; i <= NUM_NODES; i++) {
            nodeIds.add(i);
            Node node = new Node(i, BASE_PORT + i, oracle);
            physicalNodes.put(i, node);
            node.start();
        }

        // Fully connect the underlying physical overlay
        for (Node n1 : physicalNodes.values()) {
            for (Node n2 : physicalNodes.values()) {
                if (n1.nodeId != n2.nodeId) {
                    n1.addPeer(n2.nodeId, "localhost", BASE_PORT + n2.nodeId);
                }
            }
        }

        // 2. SECTOR B: Build Overlay Topology
        System.out.println("2. Building Broadcast Topology (Sector B)");
        LatencyMatrix latencyMatrix = new LatencyMatrix(oracle.fullMatrix(nodeIds));

        // Using the Degree-Bounded Tree with a max fan-out of 2
        Topology tree = TopologyOptimizer.degreeBoundedTree(nodeIds, latencyMatrix, SOURCE_NODE, 2);

        // Sector B's tree is undirected. A quick BFS figures out
        // who is the parent and who are the children for Sector C.
        Map<Integer, Integer> parentMap = new HashMap<>();
        Map<Integer, Set<Integer>> childrenMap = new HashMap<>();
        for (int i : nodeIds) childrenMap.put(i, new HashSet<>());

        Queue<Integer> queue = new LinkedList<>();
        Set<Integer> visited = new HashSet<>();
        queue.add(SOURCE_NODE);
        visited.add(SOURCE_NODE);

        while (!queue.isEmpty()) {
            int current = queue.poll();
            for (int neighbor : tree.neighborsOf(current)) {
                if (!visited.contains(neighbor)) {
                    visited.add(neighbor);
                    parentMap.put(neighbor, current);
                    childrenMap.get(current).add(neighbor);
                    queue.add(neighbor);
                }
            }
        }

        // 3. SECTOR C: Start the Broadcast Engine
        System.out.println("3. Wiring Broadcast Engine to Transport (Sector C)");
        Map<Integer, BroadcastNode> broadcastNodes = new HashMap<>();

        for (int i = 1; i <= NUM_NODES; i++) {
            final int id = i;
            Node physicalNode = physicalNodes.get(id);

            // BRIDGE 1: Broadcast Engine -> Transport Layer (Sending)
            BroadcastNode.BiSender sender = (peerId, messageId, bMsg) -> {
                // Serialize Sector C message into Sector A string payload
                String payload = bMsg.originId + "::" + bMsg.seqNum + "::" + bMsg.content;
                physicalNode.send(peerId, new Message(id, "BROADCAST", payload));
            };

            BroadcastNode bNode = new BroadcastNode(id, sender);
            bNode.setTree(parentMap.get(id), childrenMap.get(id));

            // Define what happens when a node successfully receives a message
            bNode.onDeliver(m -> {
                System.out.println("[Node " + id + "] Delivered: '" + m.content + "'");
            });

            broadcastNodes.put(id, bNode);

            // BRIDGE 2: Transport Layer -> Broadcast Engine (Receiving)
            physicalNode.onMessage((fromPeer, rawMsg) -> {
                if ("BROADCAST".equals(rawMsg.type)) {
                    // Deserialize Sector A payload back into Sector C message
                    String[] parts = rawMsg.payload.split("::");
                    BroadcastMessage m = new BroadcastMessage(
                            Integer.parseInt(parts[0]),
                            Long.parseLong(parts[1]),
                            parts[2]
                    );
                    broadcastNodes.get(id).handleIncoming(m, fromPeer);
                }
            });
        }

        // 4. Execution & Testing
        System.out.println("4. Initiating Broadcast from Source Node " + SOURCE_NODE + "\n");

        broadcastNodes.get(SOURCE_NODE).initiateBroadcast("Project Integration Successful!", 1001);

        // Give the network a couple of seconds to route all the simulated delayed packets
        Thread.sleep(2000);

        System.out.println("\n5. Shutting down");
        for (Node n : physicalNodes.values()) {
            n.shutdown();
        }
        System.exit(0);
    }
}