package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

// errRequestTooLarge is returned when a command line exceeds MaxRequestSize.
var errRequestTooLarge = errors.New("server: request line too large")

// connection tracks one accepted client socket.
type connection struct {
	net.Conn

	remoteAddr  string
	connectedAt time.Time
	lastActive  atomic.Int64 // Unix nanoseconds; read by Stats.
}

// newConnection wraps a raw socket with tracking metadata.
func newConnection(conn net.Conn) *connection {
	return &connection{
		Conn:        conn,
		remoteAddr:  conn.RemoteAddr().String(),
		connectedAt: time.Now(),
	}
}

// touch records activity for idle/accounting purposes.
func (c *connection) touch() {
	c.lastActive.Store(time.Now().UnixNano())
}

// lastActiveAt returns the time of the last completed command read.
func (c *connection) lastActiveAt() time.Time {
	nanos := c.lastActive.Load()
	if nanos == 0 {
		return c.connectedAt
	}
	return time.Unix(0, nanos)
}

// readCommandLine reads one newline-terminated line without allocating an
// unbounded buffer. The returned line excludes the newline and optional CR.
func readCommandLine(r *bufio.Reader, max int) (string, error) {
	var line []byte

	for {
		fragment, err := r.ReadSlice('\n')
		if len(line)+len(fragment) > max {
			return "", errRequestTooLarge
		}
		line = append(line, fragment...)

		switch {
		case err == nil:
			text := strings.TrimSuffix(string(line), "\n")
			text = strings.TrimSuffix(text, "\r")
			return text, nil

		case errors.Is(err, bufio.ErrBufferFull):
			continue

		default:
			return "", err
		}
	}
}

// serveConnection reads and executes commands until the client disconnects,
// becomes idle, or the server starts shutting down.
func (s *Server) serveConnection(conn *connection) {
	defer s.connWG.Done()
	defer s.unregisterConnection(conn)
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		if s.isStopping() {
			return
		}

		if err := conn.SetReadDeadline(time.Now().Add(s.config.IdleTimeout)); err != nil {
			return
		}

		line, err := readCommandLine(reader, s.config.MaxRequestSize)
		if err != nil {
			if errors.Is(err, errRequestTooLarge) {
				_ = s.writeResponse(conn, responseTooLarge)
			}
			return
		}
		conn.touch()
		s.requests.Add(1)

		command, parseErr := ParseCommand(line)
		operation := "PARSE"
		if parseErr == nil {
			operation = string(command.Type)
		}

		// Start observation after parsing so the operation label is known.
		// Parse failures are still traced, logged, and counted as requests.
		request := s.config.Telemetry.StartRequest(context.Background(), operation, conn.remoteAddr)

		var response string
		if parseErr != nil {
			response = "ERR " + parseErr.Error()
		} else {
			response = s.execute(command)
		}

		writeOK := s.writeResponse(conn, response)
		request.Finish(response, writeOK)
		if !writeOK {
			return
		}
		if s.isStopping() {
			return
		}
	}
}

// execute runs a parsed command on the fixed worker pool.
//
// The result channel is buffered so a worker can finish even if the client
// disconnects before the response is written.
func (s *Server) execute(command Command) string {
	result := make(chan string, 1)

	job := func() {
		defer func() {
			if recover() != nil {
				result <- responseInternal
			}
		}()
		result <- s.handler.Handle(command)
	}

	if err := s.workerPool.Submit(job); err != nil {
		return responseBusy
	}
	return <-result
}

// writeResponse writes one newline-terminated response with a deadline.
func (s *Server) writeResponse(conn *connection, response string) bool {
	if err := conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
		return false
	}
	_, err := io.WriteString(conn, response+"\n")
	return err == nil
}

// rejectConnection tells an over-capacity client why it was refused.
func (s *Server) rejectConnection(conn *connection) {
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = io.WriteString(conn, responseTooManyConnections+"\n")
	_ = conn.Close()
}
