package cluster

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
	"time"
)

func TestFrameRoundTrip(t *testing.T) {
	now := time.Date(2026, time.April, 11, 12, 30, 0, 0, time.UTC)
	want := wireMessage{
		Type:      messageGossip,
		ClusterID: "test-cluster",
		Sender: Node{
			ID:       "a",
			Address:  "127.0.0.1:7001",
			Status:   StatusOnline,
			JoinedAt: now,
			LastSeen: now.Add(time.Second),
			Version:  3,
		},
		Metadata: Metadata{ClusterID: "test-cluster", NodeCount: 2, Version: 7},
		Nodes: []Node{
			{ID: "b", Address: "127.0.0.1:7002", Status: StatusDegraded, Version: 2},
		},
		Left: map[NodeID]uint64{"c": 4},
	}

	var buffer bytes.Buffer
	if err := writeFrame(&buffer, want, 1<<20); err != nil {
		t.Fatalf("writeFrame() returned error: %v", err)
	}

	got, err := readFrame(&buffer, 1<<20)
	if err != nil {
		t.Fatalf("readFrame() returned error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestWriteFrameRejectsOversizedMessage(t *testing.T) {
	message := wireMessage{
		Type:      messageGossip,
		ClusterID: "cluster",
		Sender:    Node{ID: "a", Address: "127.0.0.1:7001"},
	}

	var buffer bytes.Buffer
	if err := writeFrame(&buffer, message, 16); err == nil {
		t.Fatal("writeFrame() accepted a message larger than the limit")
	}
	if buffer.Len() != 0 {
		t.Fatalf("writeFrame() wrote %d bytes before failing, want 0", buffer.Len())
	}
}

func TestReadFrameRejectsInvalidSizes(t *testing.T) {
	tests := []uint32{0, 65}

	for _, size := range tests {
		var header [frameHeaderSize]byte
		binary.BigEndian.PutUint32(header[:], size)

		if _, err := readFrame(bytes.NewReader(header[:]), 64); err == nil {
			t.Errorf("readFrame() accepted size %d with limit 64", size)
		}
	}
}
