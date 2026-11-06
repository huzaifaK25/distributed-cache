// Package chaos provides deterministic fault injection, recovery validation,
// and SLO reporting for distributed cache resilience tests.
//
// The package is intentionally self-contained: latency and packet loss wrap
// net.Conn values, partitions gate named links, and the runner controls node
// lifecycles through callbacks. This keeps production layers free of test
// switches while allowing the cluster, replication, failover, and server code
// to be exercised under adverse conditions.
package chaos

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Configuration and lifecycle errors.
var (
	ErrInvalidConfig  = errors.New("chaos: invalid config")
	ErrNoTargets      = errors.New("chaos: no targets available")
	ErrAlreadyRunning = errors.New("chaos: already running")
)

// Target is one node or service the runner can stop, recover, and probe.
//
// Stop should simulate an abrupt process failure. Recover should start a fresh
// instance with the same identity and rejoin the cluster. Healthy is optional;
// when supplied it must be safe for concurrent use.
type Target struct {
	ID      string
	Stop    func() error
	Recover func() error
	Healthy func() bool
}

// newTargetMap validates targets and returns them indexed by ID.
func newTargetMap(targets []Target) (map[string]Target, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: at least one target is required", ErrInvalidConfig)
	}

	indexed := make(map[string]Target, len(targets))
	for _, target := range targets {
		if target.ID == "" {
			return nil, fmt.Errorf("%w: target id must not be empty", ErrInvalidConfig)
		}
		if _, duplicate := indexed[target.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate target %q", ErrInvalidConfig, target.ID)
		}
		indexed[target.ID] = target
	}
	return indexed, nil
}

// randomSource is a mutex-protected deterministic random generator.
//
// Seed zero uses the current time so separate runs explore different failure
// combinations. Supply a non-zero seed for reproducible experiments.
type randomSource struct {
	mu  sync.Mutex
	rng *rand.Rand
}

// newRandomSource creates a PCG-backed random source.
func newRandomSource(seed uint64) *randomSource {
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	return &randomSource{
		rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
}

// Float64 returns a pseudo-random value in [0, 1).
func (r *randomSource) Float64() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rng.Float64()
}

// IntN returns a pseudo-random value in [0, n).
func (r *randomSource) IntN(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rng.IntN(n)
}

// Shuffle randomizes the supplied slice.
func (r *randomSource) Shuffle(n int, swap func(i, j int)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rng.Shuffle(n, swap)
}

// LossConfig configures application-level packet loss.
type LossConfig struct {
	// Probability is the chance in [0, 1] that one Write is dropped.
	Probability float64

	// Seed makes packet selection reproducible. Zero uses the current time.
	Seed uint64
}

// LossStats is a cumulative snapshot of packet-loss activity.
type LossStats struct {
	Attempts uint64
	Dropped  uint64
}

// Loss drops a configurable fraction of whole writes.
//
// Dropped writes return the full length and a nil error, matching a packet that
// left the sender but never arrived. Wrapping a client connection drops outbound
// requests; wrapping a server connection drops outbound responses. Use two Loss
// values to impair both directions.
type Loss struct {
	probability atomic.Uint64 // math.Float64bits.
	attempts    atomic.Uint64
	dropped     atomic.Uint64
	rng         *randomSource
}

// NewLoss validates config and creates a packet-loss injector.
func NewLoss(config LossConfig) (*Loss, error) {
	loss := &Loss{rng: newRandomSource(config.Seed)}
	if err := loss.SetProbability(config.Probability); err != nil {
		return nil, err
	}
	return loss, nil
}

// Probability returns the current drop probability.
func (l *Loss) Probability() float64 {
	if l == nil {
		return 0
	}
	return math.Float64frombits(l.probability.Load())
}

// SetProbability updates the drop probability.
func (l *Loss) SetProbability(probability float64) error {
	if l == nil {
		return fmt.Errorf("%w: nil loss injector", ErrInvalidConfig)
	}
	if math.IsNaN(probability) || math.IsInf(probability, 0) ||
		probability < 0 || probability > 1 {
		return fmt.Errorf("%w: loss probability %v must be in [0, 1]", ErrInvalidConfig, probability)
	}
	l.probability.Store(math.Float64bits(probability))
	return nil
}

// Stats returns cumulative attempt and drop counts.
func (l *Loss) Stats() LossStats {
	if l == nil {
		return LossStats{}
	}
	return LossStats{
		Attempts: l.attempts.Load(),
		Dropped:  l.dropped.Load(),
	}
}

// Allow reports whether one write should be delivered.
func (l *Loss) Allow() bool {
	if l == nil {
		return true
	}

	l.attempts.Add(1)
	probability := l.Probability()
	if probability <= 0 {
		return true
	}
	if probability >= 1 {
		l.dropped.Add(1)
		return false
	}
	if l.rng.Float64() < probability {
		l.dropped.Add(1)
		return false
	}
	return true
}

// Wrap returns a connection whose writes are subject to loss.
func (l *Loss) Wrap(conn net.Conn) net.Conn {
	if l == nil || conn == nil {
		return conn
	}
	return &lossConn{Conn: conn, loss: l}
}

// lossConn applies loss before forwarding writes.
type lossConn struct {
	net.Conn
	loss *Loss
}

// Write silently discards the complete write when the loss injector drops it.
func (c *lossConn) Write(p []byte) (int, error) {
	if !c.loss.Allow() {
		return len(p), nil
	}
	return c.Conn.Write(p)
}

// CrashConfig configures random node termination.
type CrashConfig struct {
	// Targets are the nodes available for crash injection.
	Targets []Target

	// Seed makes victim selection reproducible. Zero uses the current time.
	Seed uint64
}

// CrashFault randomly stops targets and recovers them later.
//
// It tracks only nodes it successfully stopped, so callers can always call
// Recover to restore the pre-experiment set.
type CrashFault struct {
	mu      sync.Mutex
	targets map[string]Target
	crashed map[string]Target
	ids     []string
	rng     *randomSource
}

// NewCrashFault validates targets and creates a crash injector.
func NewCrashFault(config CrashConfig) (*CrashFault, error) {
	targets, err := newTargetMap(config.Targets)
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	sortStrings(ids)

	return &CrashFault{
		targets: targets,
		crashed: make(map[string]Target),
		ids:     ids,
		rng:     newRandomSource(config.Seed),
	}, nil
}

// Crash randomly stops up to count running targets and returns their IDs.
//
// If count exceeds the number of running targets, every running target is
// stopped. Nodes without a Stop callback cannot be crashed and are skipped with
// an error. Successfully stopped nodes are tracked for Recover.
func (f *CrashFault) Crash(count int) ([]string, error) {
	if f == nil {
		return nil, fmt.Errorf("%w: nil crash fault", ErrInvalidConfig)
	}
	if count <= 0 {
		return nil, fmt.Errorf("%w: crash count must be positive", ErrInvalidConfig)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	available := make([]string, 0, len(f.ids))
	for _, id := range f.ids {
		if _, crashed := f.crashed[id]; !crashed {
			available = append(available, id)
		}
	}
	if len(available) == 0 {
		return nil, ErrNoTargets
	}

	f.rng.Shuffle(len(available), func(i, j int) {
		available[i], available[j] = available[j], available[i]
	})
	if count > len(available) {
		count = len(available)
	}

	crashed := make([]string, 0, count)
	var errs []error
	for _, id := range available[:count] {
		target := f.targets[id]
		if target.Stop == nil {
			errs = append(errs, fmt.Errorf("target %q has no Stop callback", id))
			continue
		}
		if err := target.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("stop target %q: %w", id, err))
			continue
		}
		f.crashed[id] = target
		crashed = append(crashed, id)
	}

	return crashed, errors.Join(errs...)
}

// Recover restarts every node this fault stopped.
//
// Nodes are recovered in sorted order for deterministic logs. A node without a
// Recover callback remains marked as crashed and produces an error.
func (f *CrashFault) Recover() error {
	if f == nil {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	ids := make([]string, 0, len(f.crashed))
	for id := range f.crashed {
		ids = append(ids, id)
	}
	sortStrings(ids)

	var errs []error
	for _, id := range ids {
		target := f.crashed[id]
		if target.Recover == nil {
			errs = append(errs, fmt.Errorf("target %q has no Recover callback", id))
			continue
		}
		if err := target.Recover(); err != nil {
			errs = append(errs, fmt.Errorf("recover target %q: %w", id, err))
			continue
		}
		delete(f.crashed, id)
	}
	return errors.Join(errs...)
}

// Crashed returns a sorted snapshot of nodes currently stopped by this fault.
func (f *CrashFault) Crashed() []string {
	if f == nil {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	ids := make([]string, 0, len(f.crashed))
	for id := range f.crashed {
		ids = append(ids, id)
	}
	sortStrings(ids)
	return ids
}

// sortStrings sorts a small string slice without pulling sort into callers.
func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

// PressureConfig configures CPU and memory pressure.
type PressureConfig struct {
	// CPUWorkers is the number of goroutines that busy-loop until Stop.
	CPUWorkers int

	// MemoryBytes is the number of bytes retained to create memory pressure.
	MemoryBytes int64

	// MemoryChunk is the allocation unit. Zero defaults to 1 MiB.
	MemoryChunk int64
}

// PressureStats is a point-in-time snapshot of resource pressure.
type PressureStats struct {
	Running       bool
	CPUWorkers    int
	MemoryBytes   int64
	CPUIterations uint64
}

// ResourcePressure generates bounded CPU and memory pressure.
//
// Memory is retained until Stop so the garbage collector cannot reclaim it
// early. Stop is idempotent and releases both the goroutines and the memory.
type ResourcePressure struct {
	config PressureConfig

	mu         sync.Mutex
	stop       chan struct{}
	running    bool
	memory     []byte
	iterations atomic.Uint64
	wg         sync.WaitGroup
}

// NewResourcePressure validates config and creates a pressure generator.
func NewResourcePressure(config PressureConfig) (*ResourcePressure, error) {
	if config.CPUWorkers < 0 {
		return nil, fmt.Errorf("%w: cpu workers must not be negative", ErrInvalidConfig)
	}
	if config.MemoryBytes < 0 {
		return nil, fmt.Errorf("%w: memory bytes must not be negative", ErrInvalidConfig)
	}
	if config.CPUWorkers == 0 && config.MemoryBytes == 0 {
		return nil, fmt.Errorf("%w: request cpu workers or memory bytes", ErrInvalidConfig)
	}
	if config.MemoryChunk < 0 {
		return nil, fmt.Errorf("%w: memory chunk must not be negative", ErrInvalidConfig)
	}
	if config.MemoryChunk == 0 {
		config.MemoryChunk = 1 << 20
	}
	return &ResourcePressure{config: config}, nil
}

// Start launches CPU workers and allocates the configured memory.
func (p *ResourcePressure) Start() error {
	if p == nil {
		return fmt.Errorf("%w: nil pressure generator", ErrInvalidConfig)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return ErrAlreadyRunning
	}

	var memory []byte
	if p.config.MemoryBytes > 0 {
		memory = make([]byte, 0, p.config.MemoryBytes)
		for remaining := p.config.MemoryBytes; remaining > 0; {
			size := p.config.MemoryChunk
			if size > remaining {
				size = remaining
			}
			chunk := make([]byte, size)
			touchMemory(chunk)
			memory = append(memory, chunk...)
			remaining -= size
		}
	}

	p.memory = memory
	p.stop = make(chan struct{})
	p.running = true
	p.iterations.Store(0)

	for worker := 0; worker < p.config.CPUWorkers; worker++ {
		p.wg.Add(1)
		go p.cpuWorker(p.stop)
	}
	return nil
}

// Stop halts CPU workers and releases retained memory.
//
// Stop is idempotent and safe to call concurrently. The pressure mutex is held
// across the worker wait so a second Stop cannot close an already closed
// channel and Start cannot overlap a previous worker set.
func (p *ResourcePressure) Stop() {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return
	}
	close(p.stop)
	p.running = false
	p.wg.Wait()
	p.stop = nil
	p.memory = nil
}

// Running reports whether pressure is currently active.
func (p *ResourcePressure) Running() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// Stats returns a pressure snapshot.
func (p *ResourcePressure) Stats() PressureStats {
	if p == nil {
		return PressureStats{}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return PressureStats{
		Running:       p.running,
		CPUWorkers:    p.config.CPUWorkers,
		MemoryBytes:   int64(len(p.memory)),
		CPUIterations: p.iterations.Load(),
	}
}

// cpuWorker burns CPU until stop closes.
func (p *ResourcePressure) cpuWorker(stop <-chan struct{}) {
	defer p.wg.Done()

	var state uint64 = 1
	for {
		select {
		case <-stop:
			return
		default:
		}

		for i := 0; i < 10_000; i++ {
			state = state*6364136223846793005 + 1442695040888963407
		}
		p.iterations.Add(10_000)
		// Yield periodically so the scheduler, probes, and test harness can
		// still run while pressure is active.
		runtime.Gosched()
	}
}

// touchMemory writes one byte per page so the OS commits the allocation.
func touchMemory(memory []byte) {
	for i := 0; i < len(memory); i += 4096 {
		memory[i] = byte(i)
	}
	if len(memory) > 0 {
		memory[len(memory)-1] = byte(len(memory))
	}
}
