package raft

import "testing"

type fixedTestRand struct{}

func (fixedTestRand) Intn(n int) int { return 0 }

func TestProposeCopiesCallerData(t *testing.T) {
	n := New(Config{
		ID:             1,
		Peers:          []NodeID{1},
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		Rand:           fixedTestRand{},
	}, Persisted{}).(*raftNode)

	// Make this single node a leader for the test.
	n.role = Leader
	n.matchIndex = map[NodeID]Index{1: 0}

	input := []byte("original command")

	if !n.Propose(input) {
		t.Fatal("Propose unexpectedly rejected the command")
	}

	// Change the caller's buffer after Propose accepts it.
	input[0] = 'X'

	if got := string(n.log[0].Data); got != "original command" {
		t.Fatalf("stored command changed: got %q, want %q", got, "original command")
	}
}
