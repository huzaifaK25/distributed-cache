# Phase 7 - Partitioning and Consistent Hashing

## Objective

Distribute cache keys across nodes so adding capacity increases total memory and
throughput without reshuffling the whole keyspace.

Phase 7 adds a standalone `internal/partitioning` layer that maps every key to a
fixed logical partition, assigns partitions to physical nodes with a consistent
hash ring, and plans and executes safe partition migrations. Like the earlier
cluster, replication, and failover layers, it is not yet wired into
`cmd/cache`; integration with the server and cluster view is a later step.

## Architecture

```text
 key
  |
  v
+----------------------+       +---------------------------+
| PartitionOf(key)     |       | consistent hash ring      |
| hash(key) % N        |       | node -> virtual nodes     |
+----------+-----------+       +-------------+-------------+
           |                                 ^
           v                                 |
     partition ID ---------------------------+
           |
           v
      owner node -> cache
```

| File | Responsibility |
|---|---|
| `hashring.go` | Immutable ring, node membership, key lookups, replica-order lookups |
| `vnode.go` | Hash function, virtual-node identifiers, token construction and search |
| `partition.go` | Fixed partition table, routing, plans, atomic ownership commits, metrics |
| `migration.go` | Migration interface, writer gate, in-process store migrator, cleanup |

## Consistent Hash Ring

The ring is a sorted list of 64-bit tokens. Every physical node contributes
`VirtualNodes` tokens. A lookup hashes the key, finds the first token at or after
that hash, and wraps to the first token when the hash is past the end.

The ring is immutable. `AddNode` and `RemoveNode` return a new ring while the
partition table keeps serving the old assignment until a migration commits. Ring
construction is independent of node order, and token collisions are resolved by
node ID and virtual-node index so routing is deterministic on every process.

`DefaultHash` applies 64-bit FNV-1a followed by a SplitMix64 finalizer. FNV alone
has weak avalanche behavior for keys that differ only in their final characters,
a common pattern for keys such as `user:1`, `user:2`, and `partition:17`. The
finalizer removes that clustering without adding a dependency.

### Virtual nodes

Virtual nodes spread each physical node around the ring. With one token per node
the ranges tend to be uneven; with the default 128 tokens the distribution is
smooth, and the count is configurable through `Config.VirtualNodes`.

`Ring.LookupN(key, n)` walks the ring clockwise and returns up to `n` distinct
nodes. It is available for future replica placement, but this phase assigns one
owner per partition.

## Key Routing and Ownership

`Partitioner` combines the ring with a fixed number of logical partitions:

1. `PartitionOf(key)` returns `hash(key) % Partitions`.
2. The partition table maps that partition to the ring owner of
   `partitionKey(partition)`.
3. `Route(key)` returns both the partition and the owner node.

Fixed partitions decouple key hashing from membership changes. The partition of
an existing key never changes while the node set changes, and migrations move
whole partitions rather than individual keys. The default is 1024 partitions;
the maximum is 65536.

`OwnerOf(partition)` reports the responsible node for callers that need to query
ownership directly. Routing reads are lock-free: an atomic pointer publishes an
immutable ownership table.

## Rebalancing and Minimal Movement

Membership changes are prepared as plans:

```go
plan, err := partitioner.PlanAddNode(node)
plan, err := partitioner.PlanRemoveNode(nodeID)
plan, err := partitioner.PlanNodes(allNodes)
```

A plan contains only partitions whose owner differs between the current and
candidate rings. When a node joins, those partitions move to the new node; other
partitions stay in place. When a node leaves, only partitions owned by that node
move to their ring successors. This is the consistent-hashing property that
makes horizontal scaling cheap.

Plans do not mutate live routing. They carry the generation they were built
from, and `Apply` rejects a plan if another membership change committed first.
Planning and applying are serialized against each other, so a plan can never
silently drop a node that joined concurrently.

## Migration Protocol

`Apply` executes a plan in four phases:

1. **Quiesce (optional).** If an `ApplyOptions.Gate` is supplied,
   `Quiesce` receives every moved partition and must block new writes to them.
2. **Copy.** Every `Move` is passed to a `Migrator`. The migrator copies all live
   keys in the partition from source to target, including remaining TTLs, and
   must leave the source untouched. Copies are idempotent.
3. **Commit.** Only after every move succeeds does the partitioner publish the
   new ownership table with one atomic pointer store.
4. **Cleanup.** If the migrator also implements `Cleaner`, source copies are
   removed after commit. Cleanup is best-effort and its errors are reported in
   `ApplyResult.CleanupErrors`; they do not roll ownership back.

The failure rules are deliberate:

- A copy failure leaves routing and membership on the previous generation.
- Source data is never deleted before commit, so a failed plan cannot lose the
  old copy.
- A failed copy may leave partial target entries; retrying the move overwrites
  them.
- If cleanup fails after commit, the new owner is authoritative and the old
  source retains a stale copy until cleanup is retried.

Without a gate, the caller must guarantee that writes to moved partitions are
paused for the duration of `Apply`, or that writes are routed through the same
ownership generation. The built-in `StoreMigrator` operates on in-process cache
stores; networked deployments can implement `Migrator` and `Cleaner` over their
own RPC transport.

## Node Removal

Removing a node is planned like any other membership change. Every partition
owned by the removed node is copied to the successor chosen by the remaining
ring, the new table is committed, and the removed node's moved partitions are
cleaned up. The partitioner refuses to remove the final node, because no target
would exist for its partitions.

## Metrics

`Partitioner.Stats()` returns a point-in-time snapshot:

| Field | Meaning |
|---|---|
| `Nodes` | Physical node count |
| `Partitions` | Configured logical partition count |
| `VirtualNodes` | Virtual nodes per physical node |
| `Tokens` | Total ring tokens |
| `Generation` | Ownership table generation |
| `Rebalances` | Membership changes committed |
| `Migrations` | Partition copies completed |
| `FailedMigrations` | Partition copies that failed |
| `MovedKeys` | Keys copied across all successful partitions |
| `MigrationDuration` | Cumulative partition-copy time |
| `LastMigrationDuration` | Duration of the most recent copy |
| `LastRebalance` | Time of the last committed rebalance |

`ApplyResult` reports the same operation from the caller's perspective:
whether it applied, the resulting generation, move count, moved keys, total
duration, and any cleanup errors.

## Configuration

Zero values use `DefaultConfig`.

| Option | Default | Behavior |
|---|---:|---|
| `Nodes` | empty | Initial membership; empty is valid during bootstrap. |
| `VirtualNodes` | `128` | Ring tokens per physical node; must be positive. |
| `Partitions` | `1024` | Fixed logical partitions; maximum 65536. |
| `Hash` | `DefaultHash` | Deterministic hash shared by every node. |

## Usage

```go
partitioner, err := partitioning.New(partitioning.Config{
	Nodes: []partitioning.Node{
		{ID: "node-a", Address: "127.0.0.1:7001"},
		{ID: "node-b", Address: "127.0.0.1:7002"},
	},
	VirtualNodes: 128,
	Partitions:   1024,
})
if err != nil {
	log.Fatal(err)
}

route, ok := partitioner.Route("user:1")
if !ok {
	log.Fatal("cluster has no nodes")
}
fmt.Println(route.Partition, route.Node.ID)

plan, err := partitioner.PlanAddNode(partitioning.Node{
	ID:      "node-c",
	Address: "127.0.0.1:7003",
})
if err != nil {
	log.Fatal(err)
}

migrator, err := partitioning.NewStoreMigrator(partitioner)
if err != nil {
	log.Fatal(err)
}
for id, store := range stores {
	if err := migrator.RegisterStore(id, store); err != nil {
		log.Fatal(err)
	}
}

result, err := partitioner.Apply(context.Background(), plan, migrator)
if err != nil {
	log.Fatal(err)
}
fmt.Printf("moved %d keys in %s\n", result.MovedKeys, result.Duration)
```

Use `ApplyWithOptions` with a `WriteGate` when writes can run concurrently with
a rebalance.

## Tests

The package tests cover:

- deterministic routing independent of node order and repeated lookups;
- virtual-node token counts and key-distribution balance;
- minimal movement when adding and removing a node;
- replica-order lookups and deterministic token-collision tie-breaking;
- key to partition to owner consistency;
- add, remove, no-op, bootstrap, and stale-plan flows;
- failure atomicity, context cancellation, and missing-migrator errors;
- writer-gate quiesce and release behavior;
- full multi-store rebalancing with data integrity and no stale copies;
- TTL preservation and node-removal redistribution;
- source preservation on migration failure and best-effort cleanup failure;
- partition, migration, moved-key, and duration metrics.

## Limitations

- The layer is standalone; the TCP cache server, cluster membership view,
  replication, and failover are not yet composed with it.
- The partition count is fixed for the lifetime of a partitioner.
- One owner is assigned per partition. Replicas can use `Ring.LookupN`, but
  partition-level replication is a future integration concern.
- Rebalancing relies on the caller for write quiescence unless a `WriteGate` is
  supplied.
- `StoreMigrator` is in-process. Remote migration needs a transport
  implementation of `Migrator` and `Cleaner`.
- Failed cleanup leaves stale source copies; ownership remains correct.

## Acceptance Criteria

- [x] Consistent hashing operational
- [x] Virtual nodes implemented
- [x] Rebalancing working
- [x] Partition migration working
- [x] Horizontal scaling enabled
