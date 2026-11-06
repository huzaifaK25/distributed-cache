package chaos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	if got := percentile(nil, 0.95); got != 0 {
		t.Fatalf("percentile(nil) = %s, want 0", got)
	}
	if got := percentile([]time.Duration{time.Second}, 0.95); got != time.Second {
		t.Fatalf("percentile(single) = %s, want 1s", got)
	}

	samples := make([]time.Duration, 100)
	for i := range samples {
		samples[i] = time.Duration(i+1) * time.Millisecond
	}
	tests := []struct {
		fraction float64
		want     time.Duration
	}{
		{0.50, 50 * time.Millisecond},
		{0.95, 95 * time.Millisecond},
		{0.99, 99 * time.Millisecond},
		{1.00, 100 * time.Millisecond},
		{0, time.Millisecond},
		{1.50, 100 * time.Millisecond},
	}
	for _, test := range tests {
		if got := percentile(samples, test.fraction); got != test.want {
			t.Errorf("percentile(%v) = %s, want %s", test.fraction, got, test.want)
		}
	}
}

func TestSLOConfigDefaults(t *testing.T) {
	defaults := SLOConfig{}.withDefaults()
	if defaults.Availability != 0.99 || defaults.ErrorRate != 0.01 ||
		defaults.P95Latency != 100*time.Millisecond {
		t.Fatalf("defaults = %+v, want 0.99 availability, 0.01 error rate, 100ms p95", defaults)
	}

	clamped := SLOConfig{Availability: 2, ErrorRate: 2}.withDefaults()
	if clamped.Availability != 1 || clamped.ErrorRate != 1 {
		t.Fatalf("clamped = %+v, want both targets capped at 1", clamped)
	}

	custom := SLOConfig{
		Availability: 0.999,
		ErrorRate:    0.001,
		P95Latency:   25 * time.Millisecond,
	}.withDefaults()
	if custom.Availability != 0.999 || custom.ErrorRate != 0.001 ||
		custom.P95Latency != 25*time.Millisecond {
		t.Fatalf("custom = %+v, want supplied values", custom)
	}
}

func TestSLORecorderReport(t *testing.T) {
	recorder := NewSLORecorder(SLOConfig{
		Availability: 0.9,
		ErrorRate:    0.2,
		P95Latency:   50 * time.Millisecond,
	})
	started := time.Now().Add(-time.Second)
	recorder.Reset(started)

	for i := 0; i < 9; i++ {
		recorder.RecordRequest(10*time.Millisecond, nil)
	}
	recorder.RecordRequest(100*time.Millisecond, errors.New("boom"))
	for i := 0; i < 8; i++ {
		recorder.RecordHealth(true)
	}
	for i := 0; i < 2; i++ {
		recorder.RecordHealth(false)
	}

	report := recorder.Report()
	if report.Requests != 10 || report.Failures != 1 {
		t.Fatalf("requests = %d/%d, want 10 requests and 1 failure", report.Requests, report.Failures)
	}
	if report.HealthChecks != 10 || report.HealthyChecks != 8 {
		t.Fatalf("health = %d/%d, want 8 of 10 healthy", report.HealthyChecks, report.HealthChecks)
	}
	if report.Availability != 0.8 {
		t.Fatalf("Availability = %v, want 0.8", report.Availability)
	}
	if report.ErrorRate != 0.1 {
		t.Fatalf("ErrorRate = %v, want 0.1", report.ErrorRate)
	}
	if report.P50Latency != 10*time.Millisecond ||
		report.P95Latency != 100*time.Millisecond ||
		report.P99Latency != 100*time.Millisecond ||
		report.MaxLatency != 100*time.Millisecond {
		t.Fatalf("latencies = p50 %s p95 %s p99 %s max %s",
			report.P50Latency, report.P95Latency, report.P99Latency, report.MaxLatency)
	}
	if report.Duration <= 0 {
		t.Fatal("Duration = 0, want a positive measurement window")
	}
	if report.StartedAt != started {
		t.Fatalf("StartedAt = %s, want %s", report.StartedAt, started)
	}

	if report.AvailabilityMet() {
		t.Fatal("AvailabilityMet() = true, want false")
	}
	if !report.ErrorRateMet() {
		t.Fatal("ErrorRateMet() = false, want true")
	}
	if report.LatencyMet() {
		t.Fatal("LatencyMet() = true, want false")
	}
	if report.Passed() {
		t.Fatal("Passed() = true, want false")
	}
	if text := report.String(); !strings.Contains(text, "FAIL") ||
		!strings.Contains(text, "p95=100ms") {
		t.Fatalf("String() = %q, want FAIL and p95 details", text)
	}
}

func TestSLORecorderFallsBackToRequestAvailability(t *testing.T) {
	recorder := NewSLORecorder(SLOConfig{})
	recorder.Reset(time.Now())
	recorder.RecordRequest(5*time.Millisecond, nil)
	recorder.RecordRequest(5*time.Millisecond, nil)
	recorder.RecordRequest(5*time.Millisecond, nil)
	recorder.RecordRequest(5*time.Millisecond, errors.New("boom"))

	report := recorder.Report()
	if report.HealthChecks != 0 {
		t.Fatalf("HealthChecks = %d, want 0", report.HealthChecks)
	}
	if report.Availability != 0.75 {
		t.Fatalf("Availability = %v, want 0.75 from request success", report.Availability)
	}
	if report.ErrorRate != 0.25 {
		t.Fatalf("ErrorRate = %v, want 0.25", report.ErrorRate)
	}
	if report.AvailabilityMet() {
		t.Fatal("AvailabilityMet() = true, want false for a 99% target")
	}
}

func TestSLORecorderPassesAndResets(t *testing.T) {
	recorder := NewSLORecorder(SLOConfig{})
	recorder.Reset(time.Now())
	recorder.RecordRequest(5*time.Millisecond, nil)
	recorder.RecordHealth(true)

	report := recorder.Report()
	if !report.Passed() {
		t.Fatalf("Passed() = false, want true: %+v", report)
	}
	if text := report.String(); !strings.Contains(text, "PASS") {
		t.Fatalf("String() = %q, want PASS", text)
	}

	recorder.Reset(time.Now())
	report = recorder.Report()
	if report.Requests != 0 || report.HealthChecks != 0 || report.Failures != 0 {
		t.Fatalf("Report() after Reset() = %+v, want zero observations", report)
	}
	if report.AvailabilityMet() {
		t.Fatal("AvailabilityMet() = true with no observations")
	}
	if !report.ErrorRateMet() || !report.LatencyMet() {
		t.Fatal("error rate and latency should tolerate no observations")
	}

	var nilRecorder *SLORecorder
	nilRecorder.Reset(time.Now())
	nilRecorder.RecordRequest(time.Second, nil)
	nilRecorder.RecordHealth(false)
	nilReport := nilRecorder.Report()
	if nilReport.Targets.Availability != 0.99 {
		t.Fatalf("nil Report().Targets = %+v, want defaults", nilReport.Targets)
	}
}

func TestSLORecorderConcurrentUse(t *testing.T) {
	recorder := NewSLORecorder(SLOConfig{})
	recorder.Reset(time.Now())

	const goroutines = 8
	const requests = 100
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requests; j++ {
				recorder.RecordRequest(time.Millisecond, nil)
				recorder.RecordHealth(true)
			}
		}()
	}
	wg.Wait()

	report := recorder.Report()
	if report.Requests != goroutines*requests {
		t.Fatalf("Requests = %d, want %d", report.Requests, goroutines*requests)
	}
	if report.HealthChecks != goroutines*requests || report.HealthyChecks != goroutines*requests {
		t.Fatalf("health = %d/%d, want all healthy", report.HealthyChecks, report.HealthChecks)
	}
}

func TestNewRunnerValidation(t *testing.T) {
	healthy := func() bool { return true }

	tests := []RunnerConfig{
		{},
		{Targets: []Target{{ID: "a", Healthy: healthy}, {ID: "a", Healthy: healthy}}},
		{Targets: []Target{{ID: "a"}}},
		{
			Targets:  []Target{{ID: "a", Healthy: healthy}},
			Recovery: RecoveryConfig{Timeout: time.Second, StableFor: 2 * time.Second},
		},
	}
	for _, config := range tests {
		if _, err := NewRunner(config); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("NewRunner(%+v) error = %v, want ErrInvalidConfig", config, err)
		}
	}

	runner, err := NewRunner(RunnerConfig{Targets: []Target{{ID: "a", Healthy: healthy}}})
	if err != nil {
		t.Fatalf("NewRunner() returned error: %v", err)
	}
	if runner.config.ProbeInterval != 100*time.Millisecond {
		t.Fatalf("ProbeInterval = %s, want 100ms default", runner.config.ProbeInterval)
	}
	if runner.config.Recovery.Timeout != 10*time.Second ||
		runner.config.Recovery.StableFor != time.Second ||
		runner.config.Recovery.Interval != 50*time.Millisecond {
		t.Fatalf("Recovery defaults = %+v", runner.config.Recovery)
	}
}

func TestRunnerCrashRecoveryAndSLO(t *testing.T) {
	nodes := []*testNode{newTestNode("a"), newTestNode("b"), newTestNode("c")}
	runner, err := NewRunner(RunnerConfig{
		Targets:       []Target{nodes[0].target(), nodes[1].target(), nodes[2].target()},
		ProbeInterval: 10 * time.Millisecond,
		Seed:          13,
		Recovery: RecoveryConfig{
			Timeout:   2 * time.Second,
			StableFor: 40 * time.Millisecond,
			Interval:  5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewRunner() returned error: %v", err)
	}

	report, err := runner.Run(context.Background(), Scenario{
		Name:     "crash-recovery",
		Duration: 150 * time.Millisecond,
		Steps: []Step{{
			At:     20 * time.Millisecond,
			Name:   "crash-one",
			Action: CrashAction(1),
		}},
	})
	if err != nil {
		t.Fatalf("Run() returned error: %v", err)
	}
	if report.Scenario != "crash-recovery" {
		t.Fatalf("Scenario = %q, want crash-recovery", report.Scenario)
	}
	if report.Duration < 150*time.Millisecond {
		t.Fatalf("Duration = %s, want at least the scenario window", report.Duration)
	}
	if !report.Recovery.Recovered {
		t.Fatalf("Recovery = %+v, want recovered", report.Recovery)
	}
	if report.Recovery.Attempts == 0 {
		t.Fatal("Recovery.Attempts = 0, want at least one probe")
	}

	if report.SLO.HealthChecks == 0 {
		t.Fatal("HealthChecks = 0, want target health sampling")
	}
	if report.SLO.HealthyChecks >= report.SLO.HealthChecks {
		t.Fatalf("health = %d/%d, want a fault-induced failure",
			report.SLO.HealthyChecks, report.SLO.HealthChecks)
	}
	if report.SLO.Passed() {
		t.Fatal("SLO Passed() = true during a crash scenario")
	}

	var crashTargets []string
	kinds := make(map[EventKind]int)
	for _, event := range report.Events {
		kinds[event.Kind]++
		if event.Kind == EventCrash {
			crashTargets = event.Targets
		}
	}
	for _, kind := range []EventKind{EventScenario, EventStep, EventCrash, EventRecover} {
		if kinds[kind] == 0 {
			t.Errorf("missing %q event: %+v", kind, report.Events)
		}
	}
	if len(crashTargets) != 1 {
		t.Fatalf("crash event targets = %v, want one", crashTargets)
	}

	stops, recovers := 0, 0
	for _, node := range nodes {
		nodeStops, nodeRecovers := node.counters()
		stops += nodeStops
		recovers += nodeRecovers
		if !node.isRunning() {
			t.Fatalf("node %s is still stopped after Run()", node.id)
		}
	}
	if stops != 1 || recovers != 1 {
		t.Fatalf("stop/recover calls = %d/%d, want 1/1", stops, recovers)
	}
}

func TestRunnerRecoveryFailure(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	target := Target{
		ID:      "stuck",
		Stop:    func() error { healthy.Store(false); return nil },
		Recover: func() error { return nil },
		Healthy: healthy.Load,
	}

	runner, err := NewRunner(RunnerConfig{
		Targets:       []Target{target},
		ProbeInterval: 10 * time.Millisecond,
		Recovery: RecoveryConfig{
			Timeout:   120 * time.Millisecond,
			StableFor: 50 * time.Millisecond,
			Interval:  10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewRunner() returned error: %v", err)
	}

	report, err := runner.Run(context.Background(), Scenario{
		Name:     "stuck-node",
		Duration: 30 * time.Millisecond,
		Steps: []Step{{
			At:     5 * time.Millisecond,
			Name:   "crash",
			Action: CrashAction(1),
		}},
	})
	if !errors.Is(err, ErrRecoveryFailed) {
		t.Fatalf("Run() error = %v, want ErrRecoveryFailed", err)
	}
	if report.Recovery.Recovered {
		t.Fatal("Recovery.Recovered = true, want false")
	}
	if !strings.Contains(report.Recovery.Error, "unhealthy") {
		t.Fatalf("Recovery.Error = %q, want unhealthy target detail", report.Recovery.Error)
	}
	if healthy.Load() {
		t.Fatal("target is healthy after failed recovery")
	}
}

func TestRunnerPartitionPressureAndAutoCleanup(t *testing.T) {
	nodes := []*testNode{newTestNode("a"), newTestNode("b")}
	pressure, err := NewResourcePressure(PressureConfig{
		CPUWorkers:  1,
		MemoryBytes: 4096,
		MemoryChunk: 1024,
	})
	if err != nil {
		t.Fatalf("NewResourcePressure() returned error: %v", err)
	}

	runner, err := NewRunner(RunnerConfig{
		Targets:       []Target{nodes[0].target(), nodes[1].target()},
		ProbeInterval: 10 * time.Millisecond,
		Recovery: RecoveryConfig{
			Timeout:   time.Second,
			StableFor: 20 * time.Millisecond,
			Interval:  5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewRunner() returned error: %v", err)
	}
	partition := runner.Partition()
	link := partition.Wrap(newMemoryConn(""), "a", "b")

	var blockVerified, healVerified bool
	report, err := runner.Run(context.Background(), Scenario{
		Name:     "partition-pressure",
		Duration: 100 * time.Millisecond,
		Steps: []Step{
			{At: 5 * time.Millisecond, Name: "pressure", Action: StartPressureAction(pressure)},
			{At: 15 * time.Millisecond, Name: "split", Action: SplitAction([]string{"a"}, []string{"b"})},
			{At: 25 * time.Millisecond, Name: "verify-block", Action: func(context.Context, *Runner) error {
				if partition.Allowed("a", "b") {
					return errors.New("link remained allowed during partition")
				}
				if _, err := link.Write([]byte("blocked")); !errors.Is(err, ErrPartitioned) {
					return fmt.Errorf("partitioned write error = %v, want ErrPartitioned", err)
				}
				blockVerified = true
				return nil
			}},
			{At: 40 * time.Millisecond, Name: "heal", Action: HealAction()},
			{At: 50 * time.Millisecond, Name: "verify-heal", Action: func(context.Context, *Runner) error {
				if !partition.Allowed("a", "b") {
					return errors.New("link remained blocked after heal")
				}
				if _, err := link.Write([]byte("healed")); err != nil {
					return fmt.Errorf("write after heal: %w", err)
				}
				healVerified = true
				return nil
			}},
		},
	})
	if err != nil {
		t.Fatalf("Run() returned error: %v", err)
	}
	if !blockVerified || !healVerified {
		t.Fatalf("partition checks = block:%t heal:%t, want both true", blockVerified, healVerified)
	}
	if report.Recovery.Recovered != true {
		t.Fatalf("Recovery = %+v, want recovered", report.Recovery)
	}
	if partition.IsSplit() {
		t.Fatal("partition is still split after Run()")
	}
	if pressure.Running() {
		t.Fatal("resource pressure is still running after Run()")
	}

	kinds := make(map[EventKind]bool)
	for _, event := range report.Events {
		kinds[event.Kind] = true
	}
	if !kinds[EventPartition] || !kinds[EventHeal] {
		t.Fatalf("events = %+v, want partition and heal events", report.Events)
	}
}

func TestRunnerStepOrderingAndDefaultNames(t *testing.T) {
	node := newTestNode("a")
	runner, err := NewRunner(RunnerConfig{
		Targets: []Target{node.target()},
		Recovery: RecoveryConfig{
			Timeout:   time.Second,
			StableFor: 10 * time.Millisecond,
			Interval:  5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewRunner() returned error: %v", err)
	}

	var order []string
	report, err := runner.Run(context.Background(), Scenario{
		Duration: 40 * time.Millisecond,
		Steps: []Step{
			{At: 20 * time.Millisecond, Action: func(context.Context, *Runner) error {
				order = append(order, "second")
				return nil
			}},
			{At: 5 * time.Millisecond, Name: "early", Action: func(context.Context, *Runner) error {
				order = append(order, "early")
				return nil
			}},
		},
	})
	if err != nil {
		t.Fatalf("Run() returned error: %v", err)
	}
	if report.Scenario != "chaos-scenario" {
		t.Fatalf("Scenario = %q, want default name", report.Scenario)
	}
	if len(order) != 2 || order[0] != "early" || order[1] != "second" {
		t.Fatalf("execution order = %v, want [early second]", order)
	}

	var stepNames []string
	for _, event := range report.Events {
		if event.Kind == EventStep {
			stepNames = append(stepNames, event.Message)
		}
	}
	if len(stepNames) != 2 || stepNames[0] != "early" || stepNames[1] != "step-1" {
		t.Fatalf("step names = %v, want [early step-1]", stepNames)
	}
}

func TestRunnerStepErrorStillValidatesRecovery(t *testing.T) {
	node := newTestNode("a")
	runner, err := NewRunner(RunnerConfig{
		Targets: []Target{node.target()},
		Recovery: RecoveryConfig{
			Timeout:   time.Second,
			StableFor: 10 * time.Millisecond,
			Interval:  5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewRunner() returned error: %v", err)
	}

	stepErr := errors.New("step failed")
	report, err := runner.Run(context.Background(), Scenario{
		Name:     "step-error",
		Duration: 50 * time.Millisecond,
		Steps: []Step{{
			At:     0,
			Name:   "fail",
			Action: func(context.Context, *Runner) error { return stepErr },
		}},
	})
	if !errors.Is(err, stepErr) {
		t.Fatalf("Run() error = %v, want step error", err)
	}
	if errors.Is(err, ErrRecoveryFailed) {
		t.Fatalf("Run() error = %v, recovery should still succeed", err)
	}
	if !report.Recovery.Recovered {
		t.Fatalf("Recovery = %+v, want recovered", report.Recovery)
	}

	foundError := false
	for _, event := range report.Events {
		if event.Kind == EventError {
			foundError = true
		}
	}
	if !foundError {
		t.Fatalf("events = %+v, want an error event", report.Events)
	}
}

func TestRunnerContextCancellationCleansUpFaults(t *testing.T) {
	node := newTestNode("a")
	runner, err := NewRunner(RunnerConfig{
		Targets:       []Target{node.target()},
		ProbeInterval: 10 * time.Millisecond,
		Recovery: RecoveryConfig{
			Timeout:   time.Second,
			StableFor: 20 * time.Millisecond,
			Interval:  5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewRunner() returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	report, err := runner.Run(ctx, Scenario{
		Name:     "canceled",
		Duration: 5 * time.Second,
		Steps: []Step{{
			At:     5 * time.Millisecond,
			Name:   "crash",
			Action: CrashAction(1),
		}},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context deadline", err)
	}
	if errors.Is(err, ErrRecoveryFailed) {
		t.Fatalf("Run() error = %v, cancellation is not a recovery failure", err)
	}
	if report.Recovery.Recovered {
		t.Fatal("Recovery.Recovered = true after cancellation")
	}
	if report.Recovery.Error == "" {
		t.Fatal("Recovery.Error is empty after cancellation")
	}
	if !node.isRunning() {
		t.Fatal("crashed target was not restarted during cancellation cleanup")
	}
}

func TestRunnerRejectsInvalidScenarios(t *testing.T) {
	node := newTestNode("a")
	runner, err := NewRunner(RunnerConfig{Targets: []Target{node.target()}})
	if err != nil {
		t.Fatalf("NewRunner() returned error: %v", err)
	}

	scenarios := []Scenario{
		{},
		{Duration: time.Second, Steps: []Step{{At: -1, Action: func(context.Context, *Runner) error { return nil }}}},
		{Duration: time.Second, Steps: []Step{{At: 2 * time.Second, Action: func(context.Context, *Runner) error { return nil }}}},
		{Duration: time.Second, Steps: []Step{{At: 0}}},
	}
	for _, scenario := range scenarios {
		if _, err := runner.Run(context.Background(), scenario); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("Run(%+v) error = %v, want ErrInvalidConfig", scenario, err)
		}
	}
}

func TestNilRunner(t *testing.T) {
	var runner *Runner

	if runner.Partition() == nil {
		t.Fatal("nil Partition() returned nil")
	}
	if runner.SLORecorder() == nil {
		t.Fatal("nil SLORecorder() returned nil")
	}
	if _, err := runner.CrashRandom(1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil CrashRandom() error = %v, want ErrInvalidConfig", err)
	}
	if err := runner.RecoverCrashed(); err != nil {
		t.Fatalf("nil RecoverCrashed() error = %v, want nil", err)
	}
	if err := runner.Split([]string{"a"}, []string{"b"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil Split() error = %v, want ErrInvalidConfig", err)
	}
	runner.Heal() // Must not panic.
	if _, err := runner.Run(context.Background(), Scenario{Duration: time.Millisecond}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil Run() error = %v, want ErrInvalidConfig", err)
	}
	if err := runner.StartPressure(nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil StartPressure() error = %v, want ErrInvalidConfig", err)
	}
	runner.StopPressure(nil) // Must not panic.
}

func TestRunnerRecoverCrashedWhenNone(t *testing.T) {
	node := newTestNode("a")
	runner, err := NewRunner(RunnerConfig{Targets: []Target{node.target()}})
	if err != nil {
		t.Fatalf("NewRunner() returned error: %v", err)
	}
	if err := runner.RecoverCrashed(); err != nil {
		t.Fatalf("RecoverCrashed() error = %v, want nil", err)
	}
	if err := runner.StartPressure(nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("StartPressure(nil) error = %v, want ErrInvalidConfig", err)
	}
	runner.StopPressure(nil)
}
