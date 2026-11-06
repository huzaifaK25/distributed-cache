package chaos

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

// memoryConn is a deterministic net.Conn backed by byte buffers.
type memoryConn struct {
	mu            sync.Mutex
	read          bytes.Buffer
	written       bytes.Buffer
	closed        bool
	readDeadline  time.Time
	writeDeadline time.Time
}

// newMemoryConn returns a connection preloaded with response data.
func newMemoryConn(input string) *memoryConn {
	conn := &memoryConn{}
	conn.read.WriteString(input)
	return conn
}

// Read consumes buffered response data.
func (c *memoryConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	return c.read.Read(p)
}

// Write records outbound data.
func (c *memoryConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	return c.written.Write(p)
}

// Close marks the connection closed.
func (c *memoryConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

// LocalAddr returns a dummy loopback address.
func (c *memoryConn) LocalAddr() net.Addr { return testAddr("local") }

// RemoteAddr returns a dummy loopback address.
func (c *memoryConn) RemoteAddr() net.Addr { return testAddr("remote") }

// SetDeadline records both deadlines.
func (c *memoryConn) SetDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.readDeadline = deadline
	c.writeDeadline = deadline
	c.mu.Unlock()
	return nil
}

// SetReadDeadline records the read deadline.
func (c *memoryConn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.readDeadline = deadline
	c.mu.Unlock()
	return nil
}

// SetWriteDeadline records the write deadline.
func (c *memoryConn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.writeDeadline = deadline
	c.mu.Unlock()
	return nil
}

// writtenString returns all bytes written to the connection.
func (c *memoryConn) writtenString() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.written.String()
}

// testAddr implements net.Addr for memory connections.
type testAddr string

func (a testAddr) Network() string { return "memory" }
func (a testAddr) String() string  { return string(a) }

// testNode is a controllable target for crash and recovery tests.
type testNode struct {
	mu sync.Mutex

	id           string
	running      bool
	healthy      bool
	stopCalls    int
	recoverCalls int
	stopErr      error
	recoverErr   error
}

// newTestNode returns a running, healthy target.
func newTestNode(id string) *testNode {
	return &testNode{id: id, running: true, healthy: true}
}

// target exposes the node through the chaos Target contract.
func (n *testNode) target() Target {
	return Target{
		ID:      n.id,
		Stop:    n.stop,
		Recover: n.recover,
		Healthy: n.health,
	}
}

// stop simulates an abrupt node failure.
func (n *testNode) stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.stopCalls++
	if n.stopErr != nil {
		return n.stopErr
	}
	n.running = false
	n.healthy = false
	return nil
}

// recover simulates starting a fresh node instance.
func (n *testNode) recover() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.recoverCalls++
	if n.recoverErr != nil {
		return n.recoverErr
	}
	n.running = true
	n.healthy = true
	return nil
}

// health reports the simulated node health.
func (n *testNode) health() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.healthy
}

// setHealthy changes the health state without changing the process state.
func (n *testNode) setHealthy(healthy bool) {
	n.mu.Lock()
	n.healthy = healthy
	n.mu.Unlock()
}

// counters returns the stop and recovery call counts.
func (n *testNode) counters() (stops, recovers int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stopCalls, n.recoverCalls
}

// isRunning reports the simulated process state.
func (n *testNode) isRunning() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.running
}

// waitForCondition polls condition until it becomes true or the timeout expires.
func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return condition()
}
