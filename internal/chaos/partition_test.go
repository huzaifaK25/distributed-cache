package chaos

import (
	"errors"
	"testing"
)

func TestPartitionSplitValidation(t *testing.T) {
	partition := NewPartition()

	tests := [][][]string{
		{},
		{{"a"}},
		{{"a"}, {}},
		{{}, {"b"}},
		{{""}, {"b"}},
		{{"a", "b"}, {"b", "c"}},
	}
	for _, groups := range tests {
		if err := partition.Split(groups...); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("Split(%v) error = %v, want ErrInvalidConfig", groups, err)
		}
	}
}

func TestPartitionAllowedLinks(t *testing.T) {
	partition := NewPartition()
	if !partition.Allowed("a", "b") {
		t.Fatal("Allowed() = false before Split()")
	}
	if partition.IsSplit() {
		t.Fatal("IsSplit() = true before Split()")
	}

	if err := partition.Split([]string{"a", "b"}, []string{"c"}); err != nil {
		t.Fatalf("Split() returned error: %v", err)
	}
	if !partition.IsSplit() {
		t.Fatal("IsSplit() = false after Split()")
	}

	cases := []struct {
		from string
		to   string
		want bool
	}{
		{"a", "b", true},
		{"b", "a", true},
		{"a", "a", true},
		{"c", "c", true},
		{"a", "c", false},
		{"c", "a", false},
		{"b", "c", false},
		{"a", "unknown", true}, // Unlisted nodes are unconstrained.
		{"unknown", "c", true}, // Unlisted nodes are unconstrained.
		{"unknown", "other", true},
	}
	for _, test := range cases {
		if got := partition.Allowed(test.from, test.to); got != test.want {
			t.Errorf("Allowed(%q, %q) = %t, want %t", test.from, test.to, got, test.want)
		}
	}

	// Splitting again replaces the previous groups.
	if err := partition.Split([]string{"a"}, []string{"b", "c"}); err != nil {
		t.Fatalf("second Split() returned error: %v", err)
	}
	if partition.Allowed("a", "b") || partition.Allowed("b", "a") {
		t.Fatal("old partition is still enforced after replacement")
	}
	if !partition.Allowed("b", "c") {
		t.Fatal("allowed link in the new partition is blocked")
	}

	partition.Heal()
	if partition.IsSplit() {
		t.Fatal("IsSplit() = true after Heal()")
	}
	if !partition.Allowed("a", "b") {
		t.Fatal("Allowed() = false after Heal()")
	}
	partition.Heal() // Idempotent.
}

func TestPartitionWrapBlocksAndHealsLinks(t *testing.T) {
	partition := NewPartition()
	if err := partition.Split([]string{"a"}, []string{"b"}); err != nil {
		t.Fatalf("Split() returned error: %v", err)
	}

	conn := newMemoryConn("hello")
	link := partition.Wrap(conn, "a", "b")
	if link == conn {
		t.Fatal("Wrap() returned the original connection")
	}

	if _, err := link.Write([]byte("request")); !errors.Is(err, ErrPartitioned) {
		t.Fatalf("Write() error = %v, want ErrPartitioned", err)
	}
	if _, err := link.Read(make([]byte, 5)); !errors.Is(err, ErrPartitioned) {
		t.Fatalf("Read() error = %v, want ErrPartitioned", err)
	}
	if got := conn.writtenString(); got != "" {
		t.Fatalf("partitioned write reached the underlying connection: %q", got)
	}

	local := partition.Wrap(newMemoryConn("hello"), "a", "a")
	buffer := make([]byte, 5)
	if _, err := local.Read(buffer); err != nil {
		t.Fatalf("same-group Read() returned error: %v", err)
	}
	if string(buffer) != "hello" {
		t.Fatalf("same-group Read() = %q, want hello", buffer)
	}

	partition.Heal()
	if _, err := link.Write([]byte("request")); err != nil {
		t.Fatalf("Write() after Heal() returned error: %v", err)
	}
	if got := conn.writtenString(); got != "request" {
		t.Fatalf("underlying connection received %q after heal, want request", got)
	}
	read, err := link.Read(buffer)
	if err != nil {
		t.Fatalf("Read() after Heal() returned error: %v", err)
	}
	if string(buffer[:read]) != "hello" {
		t.Fatalf("Read() after Heal() = %q, want hello", buffer[:read])
	}
}

func TestPartitionGroupsReturnsCopy(t *testing.T) {
	partition := NewPartition()
	if groups := partition.Groups(); groups != nil {
		t.Fatalf("Groups() before Split() = %v, want nil", groups)
	}

	if err := partition.Split([]string{"a", "b"}, []string{"c"}); err != nil {
		t.Fatalf("Split() returned error: %v", err)
	}
	groups := partition.Groups()
	if len(groups) != 2 || len(groups[0]) != 2 || len(groups[1]) != 1 {
		t.Fatalf("Groups() = %v, want [[a b] [c]]", groups)
	}

	groups[0][0] = "mutated"
	fresh := partition.Groups()
	if fresh[0][0] != "a" {
		t.Fatalf("Groups() = %v, want the original group contents preserved", fresh)
	}
	if !partition.Allowed("a", "b") {
		t.Fatal("Groups() did not preserve the original group contents")
	}
}

func TestNilPartition(t *testing.T) {
	var partition *Partition

	if err := partition.Split([]string{"a"}, []string{"b"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil Split() error = %v, want ErrInvalidConfig", err)
	}
	if !partition.Allowed("a", "b") {
		t.Fatal("nil Allowed() = false")
	}
	if partition.IsSplit() {
		t.Fatal("nil IsSplit() = true")
	}
	if groups := partition.Groups(); groups != nil {
		t.Fatalf("nil Groups() = %v, want nil", groups)
	}
	partition.Heal() // Must not panic.

	conn := newMemoryConn("")
	if got := partition.Wrap(conn, "a", "b"); got != conn {
		t.Fatal("nil Partition.Wrap() did not return the original connection")
	}
}
