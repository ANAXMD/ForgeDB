package raft

import "testing"

func newStepTestNode() *raftNode {
	return New(Config{
		ID:             1,
		Peers:          []NodeID{1, 2},
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		Rand:           fixedTestRand{},
	}, Persisted{}).(*raftNode)
}

func TestStepRejectsUnknownSender(t *testing.T) {
	n := newStepTestNode()
	initialTerm := n.hs.Term

	n.Step(Message{
		From: 99,
		Term: 50,
		Type: MsgVote,
	})

	if n.hs.Term != initialTerm {
		t.Fatalf("term changed: got %d, want %d", n.hs.Term, initialTerm)
	}
	if len(n.msgs) != 0 {
		t.Fatalf("unexpected messages: %v", n.msgs)
	}
}

func TestStepRejectsSelfMessage(t *testing.T) {
	n := newStepTestNode()
	initialTerm := n.hs.Term

	n.Step(Message{
		From: 1,
		Term: 50,
		Type: MsgVote,
	})

	if n.hs.Term != initialTerm {
		t.Fatalf("term changed: got %d, want %d", n.hs.Term, initialTerm)
	}
	if len(n.msgs) != 0 {
		t.Fatalf("unexpected messages: %v", n.msgs)
	}
}

func TestStepRejectsMalformedAppendEntries(t *testing.T) {
	n := newStepTestNode()
	initialTerm := n.hs.Term

	n.Step(Message{
		From:         2,
		Term:         50,
		Type:         MsgApp,
		PrevLogIndex: 0,
		Entries: []Entry{
			{Index: 2, Term: 1},
		},
	})

	if n.hs.Term != initialTerm {
		t.Fatalf("term changed: got %d, want %d", n.hs.Term, initialTerm)
	}
	if len(n.log) != 0 {
		t.Fatalf("log changed: got %d entries, want 0", len(n.log))
	}
	if len(n.msgs) != 0 {
		t.Fatalf("unexpected messages: %v", n.msgs)
	}
}
