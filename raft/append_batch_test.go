package raft

import "testing"

func newBatchTestNode(limit int) *raftNode {
	return New(Config{
		ID:             1,
		Peers:          []NodeID{1, 2, 3},
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		MaxAppendBytes: limit,
		Rand:           fixedTestRand{},
	}, Persisted{}).(*raftNode)
}

func TestBatchLenRespectsByteLimit(t *testing.T) {
	n := newBatchTestNode(50)

	for i := 1; i <= 4; i++ {
		n.appendLocal(Entry{
			Term:  1,
			Index: Index(i),
			Data:  []byte("x"),
		})
	}

	if got := n.batchLen(1); got != 2 {
		t.Fatalf("batchLen = %d, want 2", got)
	}
}

func TestBatchLenSendsOversizedEntryAlone(t *testing.T) {
	n := newBatchTestNode(50)

	n.appendLocal(Entry{
		Term:  1,
		Index: 1,
		Data:  make([]byte, 100),
	})
	n.appendLocal(Entry{
		Term:  1,
		Index: 2,
		Data:  []byte("small"),
	})

	if got := n.batchLen(1); got != 1 {
		t.Fatalf("batchLen = %d, want 1", got)
	}
}

func TestBatchLenCountsEmptyEntryOverhead(t *testing.T) {
	n := newBatchTestNode(48)

	for i := 1; i <= 3; i++ {
		n.appendLocal(Entry{
			Term:  1,
			Index: Index(i),
		})
	}

	if got := n.batchLen(1); got != 2 {
		t.Fatalf("batchLen = %d, want 2", got)
	}
}

func TestMaxAppendBytesDefaultsToFourMiB(t *testing.T) {
	n := newBatchTestNode(0)

	if n.cfg.MaxAppendBytes != 4<<20 {
		t.Fatalf("MaxAppendBytes = %d, want %d",
			n.cfg.MaxAppendBytes, 4<<20)
	}
}

func TestSuccessfulAppendAckSendsNextBatch(t *testing.T) {
	n := newBatchTestNode(50)

	for i := 1; i <= 4; i++ {
		n.appendLocal(Entry{
			Term:  1,
			Index: Index(i),
			Data:  []byte("x"),
		})
	}

	n.role = Leader
	n.hs.Term = 1
	n.matchIndex = map[NodeID]Index{
		1: 0,
		2: 0,
		3: 0,
	}
	n.nextIndex = map[NodeID]Index{
		2: 1,
		3: 1,
	}

	n.sendAppend(2)
	n.msgs = nil

	n.Step(Message{
		From:       2,
		Type:       MsgAppResp,
		Term:       1,
		MatchIndex: 2,
	})

	if len(n.msgs) != 1 {
		t.Fatalf("queued messages = %d, want 1", len(n.msgs))
	}

	msg := n.msgs[0]
	if msg.Type != MsgApp {
		t.Fatalf("message type = %v, want MsgApp", msg.Type)
	}

	if len(msg.Entries) != 2 {
		t.Fatalf("next batch has %d entries, want 2", len(msg.Entries))
	}

	if msg.Entries[0].Index != 3 || msg.Entries[1].Index != 4 {
		t.Fatalf("next batch indexes = %v, want [3 4]",
			[]Index{msg.Entries[0].Index, msg.Entries[1].Index})
	}
}

func TestNewRejectsNegativeMaxAppendBytes(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New() did not reject negative MaxAppendBytes")
		}
	}()

	New(Config{
		ID:             1,
		Peers:          []NodeID{1},
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		MaxAppendBytes: -1,
		Rand:           fixedTestRand{},
	}, Persisted{})
}
