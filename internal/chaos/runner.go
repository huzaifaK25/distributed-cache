package chaos

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// ErrRecoveryFailed is returned when the cluster does not return to a stable
// state within the recovery deadline.
var ErrRecoveryFailed = errors.New("chaos: recovery validation failed")

// SLOConfig defines the availability, latency, and error-rate targets used in
// an SLO report. Zero values are replaced by the package defaults.
type SLOConfig struct {
	// Availability is the minimum healthy fraction in [0, 1].
	Availability float64

	// ErrorRate is the maximum failed-request fraction in [0, 1].
	ErrorRate float64

	// P95Latency is the maximum acceptable 95th-percentile request latency.
	P95Latency time.Duration
}

// withDefaults fills zero SLO targets.
func (c SLOConfig) withDefaults() SLOConfig {
	if c.Availability <= 0 {
		c.Availability = 0.99
	}
	if c.Availability > 1 {
		c.Availability = 1
	}
	if c.ErrorRate <= 0 {
		c.ErrorRate = 0.01
	}
	if c.ErrorRate > 1 {
		c.ErrorRate = 1
	}
	if c.P95Latency <= 0 {
		c.P95Latency = 100 * time.Millisecond
	}
	return c
}

// SLOReport is a point-in-time report of availability, latency, and errors.
type SLOReport struct {
	Targets       SLOConfig
	StartedAt     time.Time
	Duration      time.Duration
	Requests      int
	Failures      int
	HealthChecks  int
	HealthyChecks int
	Availability  float64
	ErrorRate     float64
	P50Latency    time.Duration
	P95Latency    time.Duration
	P99Latency    time.Duration
	MaxLatency    time.Duration
}

// AvailabilityMet reports whether the availability target was met.
func (r SLOReport) AvailabilityMet() bool {
	observations := r.Requests + r.HealthChecks
	return observations > 0 && r.Availability >= r.Targets.Availability
}

// ErrorRateMet reports whether the error-rate target was met.
func (r SLOReport) ErrorRateMet() bool {
	return r.Requests == 0 || r.ErrorRate <= r.Targets.ErrorRate
}

// LatencyMet reports whether the p95 latency target was met.
func (r SLOReport) LatencyMet() bool {
	return r.Requests == 0 || r.P95Latency <= r.Targets.P95Latency
}

// Passed reports whether every SLO target was met.
func (r SLOReport) Passed() bool {
	return r.AvailabilityMet() && r.ErrorRateMet() && r.LatencyMet()
}

// String renders a compact human-readable SLO report.
func (r SLOReport) String() string {
	status := "PASS"
	if !r.Passed() {
		status = "FAIL"
	}
	return fmt.Sprintf(
		"SLO %s: availability=%.2f%% (target %.2f%%) errorRate=%.2f%% (target %.2f%%) p95=%s (target %s) requests=%d failures=%d healthChecks=%d",
		status,
		r.Availability*100, r.Targets.Availability*100,
		r.ErrorRate*100, r.Targets.ErrorRate*100,
		r.P95Latency, r.Targets.P95Latency,
		r.Requests, r.Failures, r.HealthChecks,
	)
}

// SLORecorder accumulates request and health observations.
//
// It is safe for concurrent use. Request latency samples are retained for the
// lifetime of one experiment; the runner resets the recorder before each run.
type SLORecorder struct {
	targets SLOConfig

	mu            sync.Mutex
	startedAt     time.Time
	requests      int
	failures      int
	latencies     []time.Duration
	healthChecks  int
	healthyChecks int
}

// NewSLORecorder creates a recorder with normalized SLO targets.
func NewSLORecorder(targets SLOConfig) *SLORecorder {
	return &SLORecorder{targets: targets.withDefaults()}
}

// Reset clears all observations and starts a new measurement window.
func (r *SLORecorder) Reset(startedAt time.Time) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.startedAt = startedAt
	r.requests = 0
	r.failures = 0
	r.latencies = nil
	r.healthChecks = 0
	r.healthyChecks = 0
	r.mu.Unlock()
}

// RecordRequest records one request and its end-to-end latency.
func (r *SLORecorder) RecordRequest(latency time.Duration, err error) {
	if r == nil {
		return
	}
	if latency < 0 {
		latency = 0
	}

	r.mu.Lock()
	r.requests++
	if err != nil {
		r.failures++
	}
	r.latencies = append(r.latencies, latency)
	r.mu.Unlock()
}

// RecordHealth records one availability observation.
func (r *SLORecorder) RecordHealth(up bool) {
	if r == nil {
		return
	}

	r.mu.Lock()
	r.healthChecks++
	if up {
		r.healthyChecks++
	}
	r.mu.Unlock()
}

// Report builds a report from the observations collected so far.
func (r *SLORecorder) Report() SLOReport {
	if r == nil {
		return SLOReport{Targets: SLOConfig{}.withDefaults()}
	}

	r.mu.Lock()
	targets := r.targets
	startedAt := r.startedAt
	requests := r.requests
	failures := r.failures
	healthChecks := r.healthChecks
	healthyChecks := r.healthyChecks
	latencies := append([]time.Duration(nil), r.latencies...)
	r.mu.Unlock()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	report := SLOReport{
		Targets:       targets,
		StartedAt:     startedAt,
		Requests:      requests,
		Failures:      failures,
		HealthChecks:  healthChecks,
		HealthyChecks: healthyChecks,
	}
	if !startedAt.IsZero() {
		report.Duration = time.Since(startedAt)
	}

	if requests > 0 {
		report.ErrorRate = float64(failures) / float64(requests)
	}
	switch {
	case healthChecks > 0:
		report.Availability = float64(healthyChecks) / float64(healthChecks)
	case requests > 0:
		report.Availability = float64(requests-failures) / float64(requests)
	}

	report.P50Latency = percentile(latencies, 0.50)
	report.P95Latency = percentile(latencies, 0.95)
	report.P99Latency = percentile(latencies, 0.99)
	if len(latencies) > 0 {
		report.MaxLatency = latencies[len(latencies)-1]
	}
	return report
}

// percentile returns a nearest-rank percentile from a sorted sample slice.
func percentile(sorted []time.Duration, fraction float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(fraction*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// Probe performs one request-level health check and reports its latency.
type Probe func(context.Context) error

// HealthCheck reports whether the service as a whole is available.
type HealthCheck func(context.Context) bool

// Action is one scheduled fault or recovery step.
type Action func(context.Context, *Runner) error

// Step schedules an action at a fixed offset from the scenario start.
type Step struct {
	// At is the delay from the scenario start.
	At time.Duration

	// Name is included in the report events.
	Name string

	// Action performs the step. It must not be nil.
	Action Action
}

// Scenario is one chaos experiment.
type Scenario struct {
	// Name identifies the experiment in the report.
	Name string

	// Duration is the experiment window, including every scheduled step.
	Duration time.Duration

	// Steps are executed in chronological order.
	Steps []Step
}

// RecoveryConfig controls post-experiment stability validation.
type RecoveryConfig struct {
	// Timeout bounds the stability wait after fault cleanup.
	Timeout time.Duration

	// StableFor is how long every probe must remain healthy.
	StableFor time.Duration

	// Interval is the delay between recovery probes.
	Interval time.Duration
}

// withDefaults fills zero recovery timings.
func (c RecoveryConfig) withDefaults() RecoveryConfig {
	if c.Timeout <= 0 {
		c.Timeout = 10 * time.Second
	}
	if c.StableFor <= 0 {
		c.StableFor = time.Second
	}
	if c.Interval <= 0 {
		c.Interval = 50 * time.Millisecond
	}
	return c
}

// RunnerConfig configures fault orchestration and SLO collection.
type RunnerConfig struct {
	// Targets are the nodes controlled by crash injection.
	Targets []Target

	// Probe measures request latency and errors. It is optional when
	// HealthCheck or target health callbacks are supplied.
	Probe Probe

	// HealthCheck measures service availability. It is optional when Probe or
	// target health callbacks are supplied.
	HealthCheck HealthCheck

	// ProbeInterval is how often Probe and HealthCheck run during a scenario.
	ProbeInterval time.Duration

	// SLO defines availability, error-rate, and latency targets.
	SLO SLOConfig

	// Recovery controls post-fault stability validation.
	Recovery RecoveryConfig

	// Seed makes crash victim selection reproducible. Zero uses the clock.
	Seed uint64
}

// EventKind classifies one experiment event.
type EventKind string

// Event kinds recorded by the runner.
const (
	EventScenario  EventKind = "scenario"
	EventStep      EventKind = "step"
	EventCrash     EventKind = "crash"
	EventRecover   EventKind = "recover"
	EventPartition EventKind = "partition"
	EventHeal      EventKind = "heal"
	EventError     EventKind = "error"
)

// Event is one timestamped runner action or observation.
type Event struct {
	Time    time.Time
	Kind    EventKind
	Message string
	Targets []string
}

// RecoveryReport summarizes post-experiment recovery validation.
type RecoveryReport struct {
	Recovered bool
	Attempts  int
	Duration  time.Duration
	StableFor time.Duration
	Error     string
}

// Report is the complete result of one chaos scenario.
type Report struct {
	Scenario  string
	StartedAt time.Time
	Duration  time.Duration
	SLO       SLOReport
	Recovery  RecoveryReport
	Events    []Event
}

// Runner orchestrates crash, partition, resource, and recovery steps while
// collecting SLO observations.
type Runner struct {
	config    RunnerConfig
	crash     *CrashFault
	partition *Partition
	slo       *SLORecorder

	pressuresMu sync.Mutex
	pressures   map[*ResourcePressure]struct{}

	eventsMu sync.Mutex
	events   []Event
}

// NewRunner validates config and creates a scenario runner.
func NewRunner(config RunnerConfig) (*Runner, error) {
	if _, err := newTargetMap(config.Targets); err != nil {
		return nil, err
	}
	if config.Probe == nil && config.HealthCheck == nil {
		hasHealth := false
		for _, target := range config.Targets {
			if target.Healthy != nil {
				hasHealth = true
				break
			}
		}
		if !hasHealth {
			return nil, fmt.Errorf(
				"%w: at least one probe, health check, or target health callback is required",
				ErrInvalidConfig,
			)
		}
	}
	if config.ProbeInterval <= 0 {
		config.ProbeInterval = 100 * time.Millisecond
	}
	config.SLO = config.SLO.withDefaults()
	config.Recovery = config.Recovery.withDefaults()
	if config.Recovery.StableFor > config.Recovery.Timeout {
		return nil, fmt.Errorf("%w: recovery stable duration must not exceed timeout", ErrInvalidConfig)
	}

	crash, err := NewCrashFault(CrashConfig{Targets: config.Targets, Seed: config.Seed})
	if err != nil {
		return nil, err
	}

	return &Runner{
		config:    config,
		crash:     crash,
		partition: NewPartition(),
		slo:       NewSLORecorder(config.SLO),
		pressures: make(map[*ResourcePressure]struct{}),
	}, nil
}

// Partition returns the runner's partition controller so connections can be
// wrapped with Partition.Wrap.
func (r *Runner) Partition() *Partition {
	if r == nil {
		return NewPartition()
	}
	return r.partition
}

// SLORecorder returns the underlying observer.
func (r *Runner) SLORecorder() *SLORecorder {
	if r == nil {
		return NewSLORecorder(SLOConfig{})
	}
	return r.slo
}

// StartPressure starts resource pressure and registers it for automatic
// cleanup during recovery validation.
func (r *Runner) StartPressure(pressure *ResourcePressure) error {
	if r == nil {
		return fmt.Errorf("%w: nil runner", ErrInvalidConfig)
	}
	if pressure == nil {
		return fmt.Errorf("%w: nil pressure generator", ErrInvalidConfig)
	}
	if err := pressure.Start(); err != nil {
		return err
	}

	r.pressuresMu.Lock()
	if r.pressures == nil {
		r.pressures = make(map[*ResourcePressure]struct{})
	}
	r.pressures[pressure] = struct{}{}
	r.pressuresMu.Unlock()
	return nil
}

// StopPressure stops one pressure generator and unregisters it.
func (r *Runner) StopPressure(pressure *ResourcePressure) {
	if pressure == nil {
		return
	}
	pressure.Stop()

	if r == nil {
		return
	}
	r.pressuresMu.Lock()
	delete(r.pressures, pressure)
	r.pressuresMu.Unlock()
}

// stopPressures releases every pressure generator started through the runner.
func (r *Runner) stopPressures() {
	if r == nil {
		return
	}

	r.pressuresMu.Lock()
	pressures := make([]*ResourcePressure, 0, len(r.pressures))
	for pressure := range r.pressures {
		pressures = append(pressures, pressure)
	}
	r.pressures = nil
	r.pressuresMu.Unlock()

	for _, pressure := range pressures {
		pressure.Stop()
	}
}

// hasHealthSource reports whether availability can be observed from a service
// health check or from target health callbacks.
func (r *Runner) hasHealthSource() bool {
	if r.config.HealthCheck != nil {
		return true
	}
	for _, target := range r.config.Targets {
		if target.Healthy != nil {
			return true
		}
	}
	return false
}

// availability combines target health callbacks and the service health check.
func (r *Runner) availability(ctx context.Context) bool {
	for _, target := range r.config.Targets {
		if target.Healthy != nil && !target.Healthy() {
			return false
		}
	}
	return r.config.HealthCheck == nil || r.config.HealthCheck(ctx)
}

// CrashRandom stops up to count randomly selected targets.
func (r *Runner) CrashRandom(count int) ([]string, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: nil runner", ErrInvalidConfig)
	}
	ids, err := r.crash.Crash(count)
	if len(ids) > 0 {
		r.recordEvent(EventCrash, "targets crashed", ids...)
	}
	if err != nil {
		r.recordEvent(EventError, err.Error())
	}
	return ids, err
}

// RecoverCrashed restarts every target stopped by this runner.
func (r *Runner) RecoverCrashed() error {
	if r == nil {
		return nil
	}

	crashed := r.crash.Crashed()
	if len(crashed) == 0 {
		return nil
	}

	err := r.crash.Recover()

	remaining := make(map[string]struct{}, len(crashed))
	for _, id := range r.crash.Crashed() {
		remaining[id] = struct{}{}
	}
	recovered := make([]string, 0, len(crashed))
	for _, id := range crashed {
		if _, stillCrashed := remaining[id]; !stillCrashed {
			recovered = append(recovered, id)
		}
	}
	if len(recovered) > 0 {
		r.recordEvent(EventRecover, "targets recovered", recovered...)
	}
	if err != nil {
		r.recordEvent(EventError, err.Error())
	}
	return err
}

// Split applies a network partition to the named groups.
func (r *Runner) Split(groups ...[]string) error {
	if r == nil {
		return fmt.Errorf("%w: nil runner", ErrInvalidConfig)
	}
	if err := r.partition.Split(groups...); err != nil {
		r.recordEvent(EventError, err.Error())
		return err
	}

	var ids []string
	for _, group := range groups {
		ids = append(ids, group...)
	}
	r.recordEvent(EventPartition, "network partition applied", ids...)
	return nil
}

// Heal removes the active network partition.
func (r *Runner) Heal() {
	if r == nil || !r.partition.IsSplit() {
		return
	}
	r.partition.Heal()
	r.recordEvent(EventHeal, "network partition healed")
}

// Run executes a scenario, heals partitions, recovers crashed nodes, validates
// stability, and returns an SLO report.
//
// SLO probes stop before recovery validation so the report measures the
// experiment window rather than cleanup time.
func (r *Runner) Run(ctx context.Context, scenario Scenario) (Report, error) {
	if r == nil {
		return Report{}, fmt.Errorf("%w: nil runner", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	steps, err := validateScenario(scenario)
	if err != nil {
		return Report{}, err
	}
	if scenario.Name == "" {
		scenario.Name = "chaos-scenario"
	}

	startedAt := time.Now()
	r.slo.Reset(startedAt)
	r.resetEvents()
	r.recordEvent(EventScenario, "scenario started")

	probeCtx, stopProbes := context.WithCancel(ctx)
	var probes sync.WaitGroup

	if r.config.Probe != nil {
		probes.Add(1)
		go r.requestProbeLoop(probeCtx, &probes)
	}
	if r.hasHealthSource() {
		probes.Add(1)
		go r.healthProbeLoop(probeCtx, &probes)
	}

	runErr := r.runScenario(ctx, startedAt, scenario.Duration, steps)
	stopProbes()
	probes.Wait()

	sloReport := r.slo.Report()
	recovery := r.recover(ctx)

	report := Report{
		Scenario:  scenario.Name,
		StartedAt: startedAt,
		Duration:  time.Since(startedAt),
		SLO:       sloReport,
		Recovery:  recovery,
		Events:    r.eventSnapshot(),
	}

	if !recovery.Recovered {
		if ctx.Err() != nil {
			if runErr == nil {
				runErr = ctx.Err()
			}
			return report, runErr
		}
		return report, errors.Join(runErr, fmt.Errorf("%w: %s", ErrRecoveryFailed, recovery.Error))
	}
	return report, runErr
}

// validateScenario checks and sorts the scenario steps.
func validateScenario(scenario Scenario) ([]Step, error) {
	if scenario.Duration <= 0 {
		return nil, fmt.Errorf("%w: scenario duration must be positive", ErrInvalidConfig)
	}

	steps := append([]Step(nil), scenario.Steps...)
	for i, step := range steps {
		if step.At < 0 {
			return nil, fmt.Errorf("%w: step %q has negative offset", ErrInvalidConfig, step.Name)
		}
		if step.At > scenario.Duration {
			return nil, fmt.Errorf("%w: step %q starts after the scenario ends", ErrInvalidConfig, step.Name)
		}
		if step.Action == nil {
			return nil, fmt.Errorf("%w: step %q has no action", ErrInvalidConfig, step.Name)
		}
		if step.Name == "" {
			steps[i].Name = fmt.Sprintf("step-%d", i+1)
		}
	}

	sort.SliceStable(steps, func(i, j int) bool {
		return steps[i].At < steps[j].At
	})
	return steps, nil
}

// runScenario executes scheduled steps and then waits for the window to end.
func (r *Runner) runScenario(ctx context.Context, startedAt time.Time, duration time.Duration, steps []Step) error {
	for _, step := range steps {
		if err := waitUntil(ctx, startedAt.Add(step.At)); err != nil {
			r.recordEvent(EventError, err.Error())
			return err
		}
		r.recordEvent(EventStep, step.Name)
		if err := step.Action(ctx, r); err != nil {
			r.recordEvent(EventError, fmt.Sprintf("step %s: %v", step.Name, err))
			return fmt.Errorf("chaos: step %s: %w", step.Name, err)
		}
	}

	if err := waitUntil(ctx, startedAt.Add(duration)); err != nil {
		r.recordEvent(EventError, err.Error())
		return err
	}
	return nil
}

// requestProbeLoop records request latency and errors until ctx is done.
func (r *Runner) requestProbeLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	r.recordRequestProbe(ctx)
	ticker := time.NewTicker(r.config.ProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.recordRequestProbe(ctx)
		}
	}
}

// recordRequestProbe runs one request probe and records the result.
func (r *Runner) recordRequestProbe(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	started := time.Now()
	err := r.config.Probe(ctx)
	r.slo.RecordRequest(time.Since(started), err)
}

// healthProbeLoop records service availability until ctx is done.
func (r *Runner) healthProbeLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	r.recordHealthProbe(ctx)
	ticker := time.NewTicker(r.config.ProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.recordHealthProbe(ctx)
		}
	}
}

// recordHealthProbe records availability from target health and service checks.
func (r *Runner) recordHealthProbe(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	r.slo.RecordHealth(r.availability(ctx))
}

// recover heals partitions, restarts crashed targets, and waits for stability.
func (r *Runner) recover(ctx context.Context) RecoveryReport {
	started := time.Now()
	report := RecoveryReport{StableFor: r.config.Recovery.StableFor}

	r.stopPressures()

	if r.partition.IsSplit() {
		r.Heal()
	}

	if len(r.crash.Crashed()) > 0 {
		if err := r.RecoverCrashed(); err != nil {
			report.Error = err.Error()
		}
	}

	if report.Error == "" {
		if err := ctx.Err(); err != nil {
			report.Error = err.Error()
		} else {
			attempts, err := r.waitForRecovery(ctx)
			report.Attempts = attempts
			if err != nil {
				report.Error = err.Error()
			}
		}
	}

	report.Recovered = report.Error == ""
	report.Duration = time.Since(started)
	return report
}

// waitForRecovery polls health until every check has been healthy for the
// configured stability window.
func (r *Runner) waitForRecovery(ctx context.Context) (int, error) {
	deadline := time.Now().Add(r.config.Recovery.Timeout)
	stableSince := time.Time{}
	lastFailure := ""
	attempts := 0

	for {
		attempts++
		if err := ctx.Err(); err != nil {
			return attempts, err
		}

		healthy, reason := r.healthy(ctx)
		now := time.Now()
		if healthy {
			if stableSince.IsZero() {
				stableSince = now
			}
			if now.Sub(stableSince) >= r.config.Recovery.StableFor {
				return attempts, nil
			}
		} else {
			stableSince = time.Time{}
			lastFailure = reason
		}

		if !now.Before(deadline) {
			if lastFailure == "" {
				lastFailure = "targets did not remain stable"
			}
			return attempts, fmt.Errorf("%w: %s", ErrRecoveryFailed, lastFailure)
		}

		wait := r.config.Recovery.Interval
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		if wait <= 0 {
			continue
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return attempts, ctx.Err()
		case <-timer.C:
		}
	}
}

// healthy runs every configured probe and target health callback.
func (r *Runner) healthy(ctx context.Context) (bool, string) {
	for _, target := range r.config.Targets {
		if target.Healthy != nil && !target.Healthy() {
			return false, fmt.Sprintf("target %s is unhealthy", target.ID)
		}
	}
	if r.config.Probe != nil {
		if err := r.config.Probe(ctx); err != nil {
			return false, fmt.Sprintf("request probe: %v", err)
		}
	}
	if r.config.HealthCheck != nil && !r.config.HealthCheck(ctx) {
		return false, "service health check failed"
	}
	return true, ""
}

// waitUntil blocks until when or ctx is done.
func waitUntil(ctx context.Context, when time.Time) error {
	delay := time.Until(when)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// recordEvent appends one timestamped runner event.
func (r *Runner) recordEvent(kind EventKind, message string, targets ...string) {
	event := Event{
		Time:    time.Now(),
		Kind:    kind,
		Message: message,
		Targets: append([]string(nil), targets...),
	}

	r.eventsMu.Lock()
	r.events = append(r.events, event)
	r.eventsMu.Unlock()
}

// resetEvents clears events from a previous run.
func (r *Runner) resetEvents() {
	r.eventsMu.Lock()
	r.events = nil
	r.eventsMu.Unlock()
}

// eventSnapshot returns a copy of every recorded event.
func (r *Runner) eventSnapshot() []Event {
	r.eventsMu.Lock()
	defer r.eventsMu.Unlock()
	return append([]Event(nil), r.events...)
}

// CrashAction returns a step that randomly crashes count targets.
func CrashAction(count int) Action {
	return func(_ context.Context, runner *Runner) error {
		_, err := runner.CrashRandom(count)
		return err
	}
}

// SplitAction returns a step that applies a network partition.
func SplitAction(groups ...[]string) Action {
	copied := make([][]string, len(groups))
	for i, group := range groups {
		copied[i] = append([]string(nil), group...)
	}
	return func(_ context.Context, runner *Runner) error {
		return runner.Split(copied...)
	}
}

// HealAction returns a step that removes the active partition.
func HealAction() Action {
	return func(_ context.Context, runner *Runner) error {
		runner.Heal()
		return nil
	}
}

// StartPressureAction returns a step that starts resource pressure and
// registers it for cleanup during recovery.
func StartPressureAction(pressure *ResourcePressure) Action {
	return func(_ context.Context, runner *Runner) error {
		return runner.StartPressure(pressure)
	}
}

// StopPressureAction returns a step that stops resource pressure.
func StopPressureAction(pressure *ResourcePressure) Action {
	return func(_ context.Context, runner *Runner) error {
		runner.StopPressure(pressure)
		return nil
	}
}
