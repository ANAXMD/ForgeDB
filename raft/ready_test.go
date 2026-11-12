package raft

import (
	"reflect"
	"testing"
)

func newReadyTestNode() *raftNode {
	return New(Config{
		ID:             1,
		Peers:          []NodeID{1, 2, 3},
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		Rand:           fixedTestRand{},
	}, Persisted{}).(*raftNode)
}

func TestAdvanceWithoutReadyPreservesPendingOutput(t *testing.T) {
	n := newReadyTestNode()

	n.setHardState(1, 2)
	n.send(Message{
		Type: MsgVote,
		To:   2,
		Term: 1,
	})

	// Advance without Ready must not acknowledge anything.
	n.Advance()

	rd, ok := n.Ready()
	if !ok {
		t.Fatal("Ready returned no output after unmatched Advance")
	}
	if rd.HardState == nil {
		t.Fatal("pending HardState was lost")
	}
	if *rd.HardState != (HardState{Term: 1, VotedFor: 2}) {
		t.Fatalf("HardState = %+v, want term 1 and vote for node 2", rd.HardState)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgVote {
		t.Fatalf("pending message was lost: %+v", rd.Messages)
	}
}

func TestAdvancePreservesHardStateAndMessagesCreatedAfterReady(t *testing.T) {
	n := newReadyTestNode()

	n.setHardState(1, 2)
	n.send(Message{Type: MsgVote, To: 2, Term: 1})

	first, ok := n.Ready()
	if !ok || first.HardState == nil {
		t.Fatal("expected first Ready to contain HardState")
	}

	// Simulate new work arriving before the previous batch is advanced.
	n.setHardState(2, 3)
	n.send(Message{Type: MsgVoteResp, To: 3, Term: 2})

	n.Advance()

	second, ok := n.Ready()
	if !ok {
		t.Fatal("second Ready returned no output")
	}
	if second.HardState == nil {
		t.Fatal("new HardState was lost by Advance")
	}
	if *second.HardState != (HardState{Term: 2, VotedFor: 3}) {
		t.Fatalf("second HardState = %+v, want term 2 and vote for node 3", second.HardState)
	}
	if len(second.Messages) != 1 || second.Messages[0].Type != MsgVoteResp {
		t.Fatalf("new message was lost or old message was repeated: %+v", second.Messages)
	}
}

func TestAdvancePreservesEntriesAppendedAfterReady(t *testing.T) {
	n := newReadyTestNode()

	n.appendLocal(Entry{Index: 1, Term: 1, Data: []byte("first")})

	first, ok := n.Ready()
	if !ok || len(first.Entries) != 1 {
		t.Fatalf("first Ready = %+v, want entry 1", first)
	}

	// Append another entry after Ready but before Advance.
	n.appendLocal(Entry{Index: 2, Term: 1, Data: []byte("second")})

	n.Advance()

	second, ok := n.Ready()
	if !ok {
		t.Fatal("second Ready returned no output")
	}
	if len(second.Entries) != 1 {
		t.Fatalf("second Ready entries = %+v, want only entry 2", second.Entries)
	}
	if second.Entries[0].Index != 2 || string(second.Entries[0].Data) != "second" {
		t.Fatalf("second Ready entry = %+v, want index 2 with data %q", second.Entries[0], "second")
	}
}

func TestReadyIsIdempotentUntilAdvance(t *testing.T) {
	n := newReadyTestNode()
	n.setHardState(1, 2)
	n.send(Message{Type: MsgVote, To: 2, Term: 1})

	first, ok := n.Ready()
	if !ok {
		t.Fatal("first Ready returned no output")
	}

	second, ok := n.Ready()
	if !ok {
		t.Fatal("second Ready returned no output")
	}

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("Ready batches differ before Advance:\nfirst:  %+v\nsecond: %+v", first, second)
	}
}

func TestReadyMessagesDoNotShareInternalQueue(t *testing.T) {
	n := newReadyTestNode()
	n.send(Message{Type: MsgVote, To: 2, Term: 1})

	first, ok := n.Ready()
	if !ok || len(first.Messages) != 1 {
		t.Fatal("expected one message in first Ready")
	}

	// Mutating the returned slice must not mutate the node's queue.
	first.Messages[0].To = 3

	second, ok := n.Ready()
	if !ok || len(second.Messages) != 1 {
		t.Fatal("expected one message in second Ready")
	}
	if second.Messages[0].To != 2 {
		t.Fatalf("internal message was mutated: To = %d, want 2", second.Messages[0].To)
	}
}
