package chaos

import (
	"errors"
	"math"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestLossValidation(t *testing.T) {
	loss, err := NewLoss(LossConfig{Probability: 0.25, Seed: 1})
	if err != nil {
		t.Fatalf("NewLoss() returned error: %v", err)
	}
	if got := loss.Probability(); got != 0.25 {
		t.Fatalf("Probability() = %v, want 0.25", got)
	}

	invalid := []float64{-0.01, 1.01, math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, probability := range invalid {
		if _, err := NewLoss(LossConfig{Probability: probability}); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("NewLoss(%v) error = %v, want ErrInvalidConfig", probability, err)
		}
		if err := loss.SetProbability(probability); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("SetProbability(%v) error = %v, want ErrInvalidConfig", probability, err)
		}
	}

	var nilLoss *Loss
	if !nilLoss.Allow() {
		t.Fatal("nil Loss.Allow() = false, want true")
	}
	if stats := nilLoss.Stats(); stats != (LossStats{}) {
		t.Fatalf("nil Loss.Stats() = %+v, want zero", stats)
	}
	if got := nilLoss.Probability(); got != 0 {
		t.Fatalf("nil Loss.Probability() = %v, want 0", got)
	}
	if err := nilLoss.SetProbability(0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil Loss.SetProbability() error = %v, want ErrInvalidConfig", err)
	}
}

func TestLossAllowBounds(t *testing.T) {
	never, err := NewLoss(LossConfig{Probability: 0})
	if err != nil {
		t.Fatalf("NewLoss() returned error: %v", err)
	}
	for i := 0; i < 100; i++ {
		if !never.Allow() {
			t.Fatalf("Allow() = false at probability 0 on call %d", i)
		}
	}
	if stats := never.Stats(); stats.Attempts != 100 || stats.Dropped != 0 {
		t.Fatalf("Stats() = %+v, want 100 attempts and 0 drops", stats)
	}

	always, err := NewLoss(LossConfig{Probability: 1})
	if err != nil {
		t.Fatalf("NewLoss() returned error: %v", err)
	}
	for i := 0; i < 100; i++ {
		if always.Allow() {
			t.Fatalf("Allow() = true at probability 1 on call %d", i)
		}
	}
	if stats := always.Stats(); stats.Attempts != 100 || stats.Dropped != 100 {
		t.Fatalf("Stats() = %+v, want 100 attempts and 100 drops", stats)
	}
}

func TestLossAllowDistribution(t *testing.T) {
	loss, err := NewLoss(LossConfig{Probability: 0.3, Seed: 42})
	if err != nil {
		t.Fatalf("NewLoss() returned error: %v", err)
	}

	const attempts = 2000
	dropped := 0
	for i := 0; i < attempts; i++ {
		if !loss.Allow() {
			dropped++
		}
	}

	// A deterministic seed makes this a stable regression check rather than a
	// probabilistic assertion.
	if dropped < 450 || dropped > 750 {
		t.Fatalf("dropped %d of %d, want roughly 600", dropped, attempts)
	}
	if stats := loss.Stats(); stats.Attempts != attempts || stats.Dropped != uint64(dropped) {
		t.Fatalf("Stats() = %+v, want %d attempts and %d drops", stats, attempts, dropped)
	}
}

func TestLossWrapDropsWholeWrites(t *testing.T) {
	conn := newMemoryConn("response")

	loss, err := NewLoss(LossConfig{Probability: 1, Seed: 1})
	if err != nil {
		t.Fatalf("NewLoss() returned error: %v", err)
	}
	wrapped := loss.Wrap(conn)
	if wrapped == conn {
		t.Fatal("Wrap() returned the original connection")
	}

	written, err := wrapped.Write([]byte("request"))
	if err != nil {
		t.Fatalf("Write() returned error: %v", err)
	}
	if written != len("request") {
		t.Fatalf("Write() = %d, want %d", written, len("request"))
	}
	if got := conn.writtenString(); got != "" {
		t.Fatalf("underlying connection received %q, want nothing", got)
	}
	if stats := loss.Stats(); stats.Attempts != 1 || stats.Dropped != 1 {
		t.Fatalf("Stats() = %+v, want one dropped attempt", stats)
	}

	// Loss only affects writes; reads pass through unchanged.
	buffer := make([]byte, len("response"))
	read, err := wrapped.Read(buffer)
	if err != nil {
		t.Fatalf("Read() returned error: %v", err)
	}
	if string(buffer[:read]) != "response" {
		t.Fatalf("Read() = %q, want response", buffer[:read])
	}

	noLoss, err := NewLoss(LossConfig{Probability: 0})
	if err != nil {
		t.Fatalf("NewLoss() returned error: %v", err)
	}
	delivered := noLoss.Wrap(newMemoryConn(""))
	if _, err := delivered.Write([]byte("request")); err != nil {
		t.Fatalf("Write() with no loss returned error: %v", err)
	}

	var nilLoss *Loss
	if got := nilLoss.Wrap(conn); got != conn {
		t.Fatal("nil Loss.Wrap() did not return the original connection")
	}
	if got := loss.Wrap(nil); got != nil {
		t.Fatal("Wrap(nil) returned a non-nil connection")
	}
}

func TestCrashFaultValidation(t *testing.T) {
	tests := []CrashConfig{
		{},
		{Targets: []Target{{ID: ""}}},
		{Targets: []Target{{ID: "a"}, {ID: "a"}}},
	}
	for _, config := range tests {
		if _, err := NewCrashFault(config); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("NewCrashFault(%+v) error = %v, want ErrInvalidConfig", config, err)
		}
	}

	var nilFault *CrashFault
	if _, err := nilFault.Crash(1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil Crash() error = %v, want ErrInvalidConfig", err)
	}
	if err := nilFault.Recover(); err != nil {
		t.Fatalf("nil Recover() error = %v, want nil", err)
	}
	if crashed := nilFault.Crashed(); crashed != nil {
		t.Fatalf("nil Crashed() = %v, want nil", crashed)
	}
}

func TestCrashFaultCrashesRandomTargetsAndRecovers(t *testing.T) {
	nodes := []*testNode{newTestNode("a"), newTestNode("b"), newTestNode("c")}
	targets := []Target{nodes[0].target(), nodes[1].target(), nodes[2].target()}

	fault, err := NewCrashFault(CrashConfig{Targets: targets, Seed: 7})
	if err != nil {
		t.Fatalf("NewCrashFault() returned error: %v", err)
	}
	if _, err := fault.Crash(0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Crash(0) error = %v, want ErrInvalidConfig", err)
	}

	crashed, err := fault.Crash(2)
	if err != nil {
		t.Fatalf("Crash(2) returned error: %v", err)
	}
	if len(crashed) != 2 {
		t.Fatalf("Crash(2) = %v, want two targets", crashed)
	}
	if crashed[0] == crashed[1] {
		t.Fatalf("Crash(2) returned duplicate target %q", crashed[0])
	}

	stopped := 0
	for _, node := range nodes {
		if !node.isRunning() {
			stopped++
		}
	}
	if stopped != 2 {
		t.Fatalf("%d nodes stopped, want 2", stopped)
	}

	tracked := fault.Crashed()
	if len(tracked) != 2 {
		t.Fatalf("Crashed() = %v, want two targets", tracked)
	}
	if !sort.StringsAreSorted(tracked) {
		t.Fatalf("Crashed() = %v, want sorted order", tracked)
	}

	// Another crash excludes nodes that are already down.
	remaining, err := fault.Crash(2)
	if err != nil {
		t.Fatalf("second Crash(2) returned error: %v", err)
	}
	if len(remaining) != 1 {
		t.Fatalf("second Crash(2) = %v, want the one remaining target", remaining)
	}
	if _, err := fault.Crash(1); !errors.Is(err, ErrNoTargets) {
		t.Fatalf("Crash() with no running targets error = %v, want ErrNoTargets", err)
	}

	if err := fault.Recover(); err != nil {
		t.Fatalf("Recover() returned error: %v", err)
	}
	if tracked := fault.Crashed(); len(tracked) != 0 {
		t.Fatalf("Crashed() after Recover() = %v, want empty", tracked)
	}
	for _, node := range nodes {
		if !node.isRunning() {
			t.Fatalf("node %s is still stopped after Recover()", node.id)
		}
	}
}

func TestCrashFaultClampsCount(t *testing.T) {
	nodes := []*testNode{newTestNode("a"), newTestNode("b")}
	fault, err := NewCrashFault(CrashConfig{
		Targets: []Target{nodes[0].target(), nodes[1].target()},
		Seed:    3,
	})
	if err != nil {
		t.Fatalf("NewCrashFault() returned error: %v", err)
	}

	crashed, err := fault.Crash(10)
	if err != nil {
		t.Fatalf("Crash(10) returned error: %v", err)
	}
	if len(crashed) != 2 {
		t.Fatalf("Crash(10) = %v, want both targets", crashed)
	}
}

func TestCrashFaultReportsTargetErrors(t *testing.T) {
	stopErr := errors.New("stop failed")
	broken := newTestNode("broken")
	broken.stopErr = stopErr
	healthy := newTestNode("healthy")

	fault, err := NewCrashFault(CrashConfig{
		Targets: []Target{broken.target(), healthy.target()},
		Seed:    11,
	})
	if err != nil {
		t.Fatalf("NewCrashFault() returned error: %v", err)
	}

	crashed, err := fault.Crash(2)
	if !errors.Is(err, stopErr) {
		t.Fatalf("Crash(2) error = %v, want stop failure", err)
	}
	if len(crashed) != 1 || crashed[0] != "healthy" {
		t.Fatalf("Crash(2) = %v, want [healthy]", crashed)
	}
	if !broken.isRunning() {
		t.Fatal("target with a failing Stop callback was marked as stopped")
	}
	if tracked := fault.Crashed(); len(tracked) != 1 || tracked[0] != "healthy" {
		t.Fatalf("Crashed() = %v, want [healthy]", tracked)
	}
}

func TestCrashFaultSkipsTargetsWithoutCallbacks(t *testing.T) {
	noStop, err := NewCrashFault(CrashConfig{
		Targets: []Target{{ID: "no-stop"}},
		Seed:    1,
	})
	if err != nil {
		t.Fatalf("NewCrashFault() returned error: %v", err)
	}
	if _, err := noStop.Crash(1); err == nil {
		t.Fatal("Crash() with no Stop callback returned nil error")
	}
	if tracked := noStop.Crashed(); len(tracked) != 0 {
		t.Fatalf("Crashed() = %v, want empty", tracked)
	}

	node := newTestNode("node")
	noRecover, err := NewCrashFault(CrashConfig{
		Targets: []Target{{ID: node.id, Stop: node.stop}},
		Seed:    1,
	})
	if err != nil {
		t.Fatalf("NewCrashFault() returned error: %v", err)
	}
	if _, err := noRecover.Crash(1); err != nil {
		t.Fatalf("Crash(1) returned error: %v", err)
	}
	if err := noRecover.Recover(); err == nil {
		t.Fatal("Recover() with no Recover callback returned nil error")
	}
	if tracked := noRecover.Crashed(); len(tracked) != 1 || tracked[0] != "node" {
		t.Fatalf("Crashed() = %v, want [node]", tracked)
	}

	recoverErr := errors.New("recover failed")
	failing := newTestNode("failing")
	failing.recoverErr = recoverErr
	fault, err := NewCrashFault(CrashConfig{Targets: []Target{failing.target()}, Seed: 1})
	if err != nil {
		t.Fatalf("NewCrashFault() returned error: %v", err)
	}
	if _, err := fault.Crash(1); err != nil {
		t.Fatalf("Crash(1) returned error: %v", err)
	}
	if err := fault.Recover(); !errors.Is(err, recoverErr) {
		t.Fatalf("Recover() error = %v, want recover failure", err)
	}
	if tracked := fault.Crashed(); len(tracked) != 1 {
		t.Fatalf("Crashed() = %v, want the failed target retained", tracked)
	}
}

func TestResourcePressureValidation(t *testing.T) {
	tests := []PressureConfig{
		{},
		{CPUWorkers: -1},
		{MemoryBytes: -1},
		{CPUWorkers: 1, MemoryChunk: -1},
	}
	for _, config := range tests {
		if _, err := NewResourcePressure(config); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("NewResourcePressure(%+v) error = %v, want ErrInvalidConfig", config, err)
		}
	}

	var nilPressure *ResourcePressure
	if err := nilPressure.Start(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil Start() error = %v, want ErrInvalidConfig", err)
	}
	nilPressure.Stop()
	if nilPressure.Running() {
		t.Fatal("nil Running() = true")
	}
	if stats := nilPressure.Stats(); stats != (PressureStats{}) {
		t.Fatalf("nil Stats() = %+v, want zero", stats)
	}
}

func TestResourcePressureGeneratesLoadAndStops(t *testing.T) {
	const memoryBytes = int64(64 * 1024)
	pressure, err := NewResourcePressure(PressureConfig{
		CPUWorkers:  2,
		MemoryBytes: memoryBytes,
		MemoryChunk: 16 * 1024,
	})
	if err != nil {
		t.Fatalf("NewResourcePressure() returned error: %v", err)
	}
	if pressure.Running() {
		t.Fatal("Running() = true before Start()")
	}

	if err := pressure.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	if !pressure.Running() {
		t.Fatal("Running() = false after Start()")
	}
	if err := pressure.Start(); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Start() error = %v, want ErrAlreadyRunning", err)
	}

	stats := pressure.Stats()
	if stats.CPUWorkers != 2 || stats.MemoryBytes != memoryBytes {
		t.Fatalf("Stats() = %+v, want 2 workers and %d bytes", stats, memoryBytes)
	}
	if !waitForCondition(t, 2*time.Second, func() bool {
		return pressure.Stats().CPUIterations > 0
	}) {
		t.Fatal("CPU workers made no progress")
	}

	pressure.Stop()
	if pressure.Running() {
		t.Fatal("Running() = true after Stop()")
	}
	stats = pressure.Stats()
	if stats.MemoryBytes != 0 || stats.CPUIterations == 0 {
		t.Fatalf("Stats() after Stop() = %+v, want released memory and retained iterations", stats)
	}

	// Stop is idempotent.
	pressure.Stop()
}

func TestResourcePressureMemoryOnly(t *testing.T) {
	pressure, err := NewResourcePressure(PressureConfig{MemoryBytes: 8192})
	if err != nil {
		t.Fatalf("NewResourcePressure() returned error: %v", err)
	}
	if err := pressure.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	defer pressure.Stop()

	if stats := pressure.Stats(); stats.MemoryBytes != 8192 || stats.CPUWorkers != 0 {
		t.Fatalf("Stats() = %+v, want 8192 bytes and no workers", stats)
	}
}

func TestResourcePressureConcurrentStopAndRestart(t *testing.T) {
	pressure, err := NewResourcePressure(PressureConfig{CPUWorkers: 2})
	if err != nil {
		t.Fatalf("NewResourcePressure() returned error: %v", err)
	}
	if err := pressure.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pressure.Stop()
		}()
	}
	wg.Wait()

	if pressure.Running() {
		t.Fatal("Running() = true after concurrent Stop()")
	}
	if err := pressure.Start(); err != nil {
		t.Fatalf("Start() after Stop() returned error: %v", err)
	}
	if !pressure.Running() {
		t.Fatal("Running() = false after restart")
	}
	pressure.Stop()
}
