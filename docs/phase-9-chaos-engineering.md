# Phase 9 - Chaos Engineering

## Objective

Validate that the distributed cache survives adverse conditions. Phase 9 adds a
standalone `internal/chaos` package that injects latency, packet loss, node
crashes, network partitions, and resource pressure, then verifies recovery and
produces availability, error-rate, and latency SLO reports.

The package is an experiment harness, not a production fault switch. Cluster,
replication, failover, and server code keep their original behavior; chaos wraps
their connections and controls their lifecycles from the outside.

## Architecture

```text
                  +-------------------------------+
                  | chaos.Runner                  |
                  |  scenario -> steps -> probes  |
                  +----+----------+----------+----+
                       |          |          |
          +------------+   +------+------+   +----------------+
          | CrashFault |   | Partition   |   | ResourcePressure|
          | stop/start |   | gate links  |   | CPU + memory    |
          +------------+   +-------------+   +----------------+
                       |          |          |
                       v          v          v
                  +-------------------------------+
                  | Targets / net.Conn wrappers   |
                  | cluster, replication, server  |
                  +---------------+---------------+
                                  |
                                  v
                  +-------------------------------+
                  | SLORecorder                   |
                  | availability / errors / p95   |
                  +---------------+---------------+
                                  |
                                  v
                  +-------------------------------+
                  | Report + recovery validation  |
                  +-------------------------------+
```

| File | Responsibility |
|---|---|
| `faults.go` | `Target`, packet loss, random crash injection, CPU/memory pressure |
| `latency.go` | Latency injector, delayed connection, latency listener |
| `partition.go` | Named-link partition groups, `ErrPartitioned`, heal support |
| `runner.go` | Scenario orchestration, SLO recorder/report, recovery validation, events |

## Fault Model

`chaos.Target` represents one node or service the runner may control:

```go
type Target struct {
	ID      string
	Stop    func() error
	Recover func() error
	Healthy func() bool
}
```

- `Stop` should simulate an abrupt process failure. It must not perform a
  graceful leave if the experiment is meant to test crash recovery.
- `Recover` should start a fresh instance with the same identity. For cluster
  members this means rebinding the same fixed address and joining a live seed,
  because a `cluster.Cluster` cannot be restarted after `Stop`.
- `Healthy` is optional and must be safe for concurrent use. It is sampled by
  the runner and checked again during recovery validation.

A target without a `Stop` callback cannot be crashed. A crashed target without a
`Recover` callback remains stopped and makes recovery validation fail.

## Latency Injection

`Latency` adds a uniformly distributed delay around every connection read and
write:

```go
latency, err := chaos.NewLatency(chaos.LatencyConfig{
	Min:  10 * time.Millisecond,
	Max:  25 * time.Millisecond,
	Seed: 7,
})
if err != nil {
	return err
}
conn = latency.Wrap(conn)
```

A request followed by a response therefore observes roughly two delays. The
same injector can wrap every accepted socket:

```go
listener = chaos.NewLatencyListener(listener, latency)
```

`Delay` returns a value in `[Min, Max]`; omitting `Max` fixes the delay at
`Min`. `Sleep(ctx)` allows scheduled actions to wait for the next delay and
honors context cancellation.

## Packet Loss

`Loss` drops a configurable fraction of whole writes:

```go
loss, err := chaos.NewLoss(chaos.LossConfig{
	Probability: 0.05,
	Seed:        11,
})
conn = loss.Wrap(conn)
```

A dropped write returns the full input length and a nil error, matching a
packet that left the sender but never arrived. Wrapping a client connection
drops outbound requests; wrapping a server connection drops outbound responses.
Use two injectors when both directions should be impaired. `Stats` reports
cumulative attempts and drops, and `SetProbability` changes the rate while the
experiment is running.

## Node Crashes

`CrashFault` randomly stops up to N running targets:

```go
fault, err := chaos.NewCrashFault(chaos.CrashConfig{
	Targets: targets,
	Seed:    42,
})
crashed, err := fault.Crash(2)
// ... experiment ...
err = fault.Recover()
```

- A non-zero seed makes victim selection reproducible; zero uses the clock.
- Target IDs are sorted before shuffling, so the same targets and seed select
  the same victims across runs.
- Already crashed targets are excluded from later selections.
- Counts larger than the available target set are clamped.
- Only successfully stopped targets are tracked for recovery.
- `Recover` restarts tracked targets in sorted order and keeps any target whose
  recovery fails marked as crashed.

The runner exposes the same operation as `CrashRandom`, `RecoverCrashed`, and
`CrashAction(count)`.

## Network Partitions

`Partition` gates communication between named nodes using disjoint groups:

```go
partition := chaos.NewPartition()
if err := partition.Split(
	[]string{"node-a", "node-b"},
	[]string{"node-c"},
); err != nil {
	return err
}

conn := partition.Wrap(rawConn, "node-a", "node-c")
// Reads and writes now return chaos.ErrPartitioned.

partition.Heal()
// The same wrapped connection works again.
```

- At least two non-empty groups are required.
- Duplicate IDs and empty IDs are rejected.
- Links inside one group remain available; every cross-group link is blocked.
- IDs that are not listed in any group are unconstrained.
- Splitting again replaces the previous partition.
- `Groups` returns a deep copy, and `IsSplit` reports whether a split is active.

Partitions are enforced at the wrapped connection boundary. To partition real
traffic, wrap every link that should be affected or use the partition in a
health check that models cluster reachability.

## Resource Pressure

`ResourcePressure` creates bounded, explicitly releasable load:

```go
pressure, err := chaos.NewResourcePressure(chaos.PressureConfig{
	CPUWorkers:  4,
	MemoryBytes: 256 << 20,
	MemoryChunk: 1 << 20,
})
runner.StartPressure(pressure)
// Recovery validation releases it automatically.
```

CPU workers busy-loop with periodic `runtime.Gosched` calls so the scheduler,
probes, and test harness can still run. Memory is retained in chunks until
`Stop`, which prevents the garbage collector from reclaiming it early.
`MemoryChunk` defaults to 1 MiB. `Start` is rejected while already running;
`Stop` is idempotent.

## Recovery Validation

After a scenario's measurement window ends, the runner automatically:

1. Stops every pressure generator started through the runner.
2. Heals an active network partition.
3. Restarts every target stopped by crash injection.
4. Polls health until every target, request probe, and service health check has
   been healthy for `Recovery.StableFor`.
5. Fails with `ErrRecoveryFailed` if stability is not reached within
   `Recovery.Timeout` after cleanup.

Health is sampled every `Recovery.Interval`. Any failure resets the stable
window. This catches nodes that restart but immediately crash again, partitions
that heal on one side only, and services that recover briefly before failing.

If the scenario context is canceled, cleanup still runs, but stability
validation is skipped and the context error is returned instead of
`ErrRecoveryFailed`. A step error does not skip recovery validation; the runner
still reports whether the system returned to a stable state.

## SLO Monitoring

`SLOConfig` defines three targets:

| Target | Default | Meaning |
|---|---:|---|
| `Availability` | `0.99` | Minimum healthy fraction in `[0, 1]`. |
| `ErrorRate` | `0.01` | Maximum failed-request fraction in `[0, 1]`. |
| `P95Latency` | `100ms` | Maximum 95th-percentile request latency. |

`SLORecorder` accumulates request latency/error observations and availability
checks. It is safe for concurrent use and is reset at the start of each run.

`SLOReport` includes:

- requests and failures;
- health checks and healthy checks;
- availability, error rate, p50, p95, p99, and maximum latency;
- the configured targets and measurement window;
- `AvailabilityMet`, `ErrorRateMet`, `LatencyMet`, `Passed`, and a compact
  `String` rendering.

Availability is derived from health observations when any are present, and from
request success otherwise. Percentiles use the nearest-rank method over retained
latency samples. When target health callbacks are the only health source, the
runner samples them at `ProbeInterval`, so a target-only configuration still
produces availability data.

The runner stops SLO probes before recovery cleanup. Reports therefore describe
the experiment window, not the time spent restarting nodes or healing links.

## Scenario Orchestration

`Scenario` contains a duration and chronologically ordered `Step` values:

```go
runner, err := chaos.NewRunner(chaos.RunnerConfig{
	Targets:       targets,
	Probe:         probe,
	HealthCheck:   clusterHealthy,
	ProbeInterval: 50 * time.Millisecond,
	SLO: chaos.SLOConfig{
		Availability: 0.99,
		ErrorRate:    0.01,
		P95Latency:   100 * time.Millisecond,
	},
	Recovery: chaos.RecoveryConfig{
		Timeout:   10 * time.Second,
		StableFor: 2 * time.Second,
		Interval:  50 * time.Millisecond,
	},
	Seed: 42,
})
if err != nil {
	return err
}

report, err := runner.Run(ctx, chaos.Scenario{
	Name:     "crash-and-partition",
	Duration: 30 * time.Second,
	Steps: []chaos.Step{
		{At: 5 * time.Second, Name: "crash-one", Action: chaos.CrashAction(1)},
		{At: 12 * time.Second, Name: "split", Action: chaos.SplitAction(
			[]string{"node-a"}, []string{"node-b", "node-c"},
		)},
		{At: 20 * time.Second, Name: "heal", Action: chaos.HealAction()},
	},
})
```

Built-in actions include `CrashAction`, `SplitAction`, `HealAction`,
`StartPressureAction`, and `StopPressureAction`. Any `Action` can issue custom
faults or record application-specific assertions.

`Report` contains the scenario name, start time, total duration, SLO report,
recovery report, and timestamped events. Event kinds are `scenario`, `step`,
`crash`, `recover`, `partition`, `heal`, and `error`.

## Configuration

`RunnerConfig`:

| Option | Default | Behavior |
|---|---|---|
| `Targets` | required | At least one unique node/service target. |
| `Probe` | optional | Request-level health and latency measurement. |
| `HealthCheck` | optional | Service-wide availability check. |
| `ProbeInterval` | `100ms` | Probe cadence during the scenario. |
| `SLO` | package defaults | Availability, error-rate, and p95 targets. |
| `Recovery` | package defaults | Timeout, stable window, and poll interval. |
| `Seed` | clock | Reproducible crash selection. |

`RecoveryConfig` defaults to a `10s` timeout, `1s` stable window, and `50ms`
poll interval. `StableFor` must not exceed `Timeout`.

`LatencyConfig`:

| Option | Default | Behavior |
|---|---|---|
| `Min` | `0` | Minimum delay per read/write. |
| `Max` | `Min` | Maximum delay; must be at least `Min`. |
| `Seed` | clock | Reproducible jitter sequence. |

`LossConfig`:

| Option | Default | Behavior |
|---|---|---|
| `Probability` | required | Drop probability in `[0, 1]`. |
| `Seed` | clock | Reproducible drop sequence. |

`PressureConfig`:

| Option | Default | Behavior |
|---|---|---|
| `CPUWorkers` | `0` | Busy-loop goroutines. |
| `MemoryBytes` | `0` | Retained memory allocation. |
| `MemoryChunk` | `1 MiB` | Allocation unit. |

At least one of `CPUWorkers` or `MemoryBytes` must be positive.

## Behavior Notes

- Faults are opt-in. No production package imports `internal/chaos`, and there
  are no runtime fault switches in cluster, replication, failover, or server
  code.
- Dropped writes report success to the caller and never reach the wrapped
  connection.
- Latency is applied to each `Read` and `Write` separately, not per logical
  request.
- Partitions affect only connections wrapped with the same `Partition`.
- Unlisted nodes are not constrained by a partition.
- A split replaces the previous partition rather than composing with it.
- Crash selection is reproducible for the same target set and seed.
- Resource pressure is local to the process and released during recovery.
- SLO probes stop before partition healing and node restart.
- Recovery succeeds only after all configured health sources remain healthy for
  the full stable window.
- Reports are retained in memory and intended for test output, CI assertions,
  or experiment artifacts.
- The runner can be reused for multiple scenarios; each `Run` resets events and
  SLO observations.

## Tests

`internal/chaos` tests cover:

- loss bounds, deterministic distribution, stats, and whole-write dropping;
- crash validation, seeded selection, clamping, partial failures, and recovery;
- pressure validation, CPU progress, retained memory, and idempotent stop;
- latency bounds, context-aware sleep, delayed reads/writes, and listeners;
- partition validation, link matrices, wrapped-connection blocking, healing,
  and deep-copy group snapshots;
- SLO defaults, availability/error/latency calculations, percentile ranks,
  reset behavior, and concurrent recording;
- runner validation, step ordering and default names, event capture, step
  errors, context cancellation, pressure cleanup, and recovery success/failure.

Integration tests cover:

- three real `cluster.Cluster` members started on fixed loopback addresses;
- crashing one node through `chaos.Runner`, starting a replacement on the same
  address, rejoining a live seed, and reconverging all three membership views;
- latency and partition wrapping around a real TCP cache server, including
  blocked traffic and successful traffic after `Heal`.

## Limitations

- Faults are implemented with application-level callbacks and `net.Conn`
  wrappers, not kernel `tc`, eBPF, or process signals.
- Latency and loss affect only connections that are explicitly wrapped.
- A `Partition` does not discover or alter the cluster's own transport. Real
  partition tests must wrap the affected links or model reachability through
  target health checks.
- Crash abruptness depends on the target's `Stop` callback. A target that leaves
  gracefully does not test the same path as a real process kill.
- Recovery validation checks health and stability, not data integrity. Combine
  it with the replication and failover suites when validating acknowledged
  writes.
- Resource pressure is bounded by the experiment process and is not a substitute
  for OS-level memory or cgroup limits.
- SLO reports are in-process snapshots. Long-term retention, aggregation, and
  alerting remain the responsibility of Prometheus and the Phase 8 assets.
- The recorder retains one latency sample per request for the duration of a
  scenario, so extremely long runs with high probe rates increase memory use.
- Replacement identity, fixed addressing, and seed selection are owned by the
  caller's `Recover` callback.

## Acceptance Criteria

- [x] Fault injection operational
- [x] Crash recovery verified
- [x] Partition recovery verified
- [x] SLO reports generated
