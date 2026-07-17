"""
MODULE 2 (Ivy) — CONSISTENT HASH MAP

Implements Task 2 of the assignment: a fixed-size circular
hash map (array-backed) used by the load balancer to decide,
for every incoming client request, which server replica
should handle it.

Design
------
- The ring is represented as a Python list of size `num_slots`
  (default 512), each cell holding either None (empty) or the
  hostname of the (virtual) server occupying that slot. Using
  a flat array keeps clockwise lookups a simple O(num_slots)
  scan in the worst case, and O(1)/O(log) amortized in practice
  since the ring is sparsely populated (N * K << num_slots).

- Request hashing:      H(i)   = i^2 + 2*i + 17           (mod of #slots)
- Virtual server hash:  Phi(i,j) = i^2 + j^2 + 2*j + 25    (mod of #slots)
  where i = server id, j = virtual replica index (0..K-1)

- Collisions (two servers/virtual-servers or a server and a
  request landing on the same slot) are resolved for SERVER
  placement using quadratic probing: slot, slot+1^2, slot+2^2, ...
  (mod #slots) until a free cell is found. Client requests never
  need probing -- they simply walk the ring clockwise from their
  home slot until they hit the first occupied slot (the "nearest
  server in clockwise order" rule from the assignment spec).

- Virtual servers: each physical server container is inserted
  K = ceil(log2(#slots)) times (K=9 for #slots=512) so that a
  single server failure/removal only reshuffles a small, evenly
  spread set of requests instead of dumping all of its load onto
  exactly one neighbour.
"""

import math


class ConsistentHashMap:
    def __init__(self, num_slots: int = 512, num_virtual: int = 0):
        self.num_slots = num_slots

        # K = log2(#slots) per the assignment spec, unless overridden.
        self.num_virtual = num_virtual or max(1, math.ceil(math.log2(num_slots)))

        # slot index -> hostname occupying it (None if empty)
        self.ring = [None] * self.num_slots

        # hostname -> list of slot indices it occupies (its virtual servers)
        self.server_slots = {}

    # Hash functions
    @staticmethod
    def request_hash(request_id: int) -> int:
        """H(i) = i^2 + 2i + 17"""
        return request_id ** 2 + 2 * request_id + 17

    @staticmethod
    def virtual_server_hash(server_id: int, replica_id: int) -> int:
        """Phi(i, j) = i^2 + j^2 + 2j + 25"""
        return server_id ** 2 + replica_id ** 2 + 2 * replica_id + 25

    # Internal helpers
    def _find_free_slot(self, preferred_slot: int) -> int:
        """Quadratic probing to resolve collisions when placing a
        (virtual) server on the ring."""
        slot = preferred_slot % self.num_slots
        i = 0
        while self.ring[slot] is not None:
            i += 1
            if i > self.num_slots:
                raise RuntimeError("Consistent hash map is full")
            slot = (preferred_slot + i * i) % self.num_slots
        return slot

    # Public API
    def add_server(self, hostname: str, server_id: int):
        """Insert `num_virtual` replicas of `hostname` into the ring."""
        if hostname in self.server_slots:
            return  # already present, no-op
        occupied = []
        for j in range(self.num_virtual):
            h = self.virtual_server_hash(server_id, j)
            slot = self._find_free_slot(h)
            self.ring[slot] = hostname
            occupied.append(slot)
        self.server_slots[hostname] = occupied

    def remove_server(self, hostname: str):
        """Remove every virtual replica of `hostname` from the ring."""
        for slot in self.server_slots.pop(hostname, []):
            self.ring[slot] = None

    def get_server(self, request_id: int):
        """Return the hostname that should serve `request_id`, walking
        clockwise from the request's home slot until an occupied slot
        is found. Returns None if the ring has no servers."""
        if not self.server_slots:
            return None
        start = self.request_hash(request_id) % self.num_slots
        for i in range(self.num_slots):
            slot = (start + i) % self.num_slots
            if self.ring[slot] is not None:
                return self.ring[slot]
        return None

    def load_counts_snapshot(self):
        """Number of virtual slots currently owned by each server -
        useful for debugging/analysis of ring balance."""
        return {h: len(slots) for h, slots in self.server_slots.items()}
