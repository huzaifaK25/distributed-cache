package server

import "testing"

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name string
		line string
		want Command
	}{
		{name: "ping", line: "PING", want: Command{Type: CommandPing}},
		{name: "lowercase ping", line: "ping", want: Command{Type: CommandPing}},
		{name: "padded ping", line: "  PING  ", want: Command{Type: CommandPing}},
		{name: "ping with CR", line: "PING\r", want: Command{Type: CommandPing}},
		{name: "set", line: "SET user:1 hello", want: Command{Type: CommandSet, Key: "user:1", Value: "hello"}},
		{name: "set value with spaces", line: "set user:1 hello world", want: Command{Type: CommandSet, Key: "user:1", Value: "hello world"}},
		{name: "set value with tab", line: "SET key hello\tworld", want: Command{Type: CommandSet, Key: "key", Value: "hello\tworld"}},
		{name: "get", line: "GET user:1", want: Command{Type: CommandGet, Key: "user:1"}},
		{name: "del", line: "DEL user:1", want: Command{Type: CommandDel, Key: "user:1"}},
		{name: "stats", line: "STATS", want: Command{Type: CommandStats}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCommand(tt.line)
			if err != nil {
				t.Fatalf("ParseCommand(%q) returned error: %v", tt.line, err)
			}
			if got != tt.want {
				t.Fatalf("ParseCommand(%q) = %+v, want %+v", tt.line, got, tt.want)
			}
		})
	}
}

func TestParseCommandRejectsInvalidInput(t *testing.T) {
	lines := []string{
		"",
		"   ",
		"NOPE",
		"PING extra",
		"STATS extra",
		"GET",
		"GET key extra",
		"DEL",
		"DEL key extra",
		"SET",
		"SET key",
		"SET key ",
		"\t",
	}

	for _, line := range lines {
		t.Run(line, func(t *testing.T) {
			if _, err := ParseCommand(line); err == nil {
				t.Fatalf("ParseCommand(%q) succeeded, want an error", line)
			}
		})
	}
}
