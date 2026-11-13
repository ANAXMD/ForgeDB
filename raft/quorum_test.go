package raft

import "testing"

func newQuorumTestNode() *raftNode {
	return New(Config{
		ID:             1,
		Peers:          []NodeID{1, 2, 3},
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		PreVote:        true,
		CheckQuorum:    true,
		Rand:           fixedTestRand{},
	}, Persisted{}).(*raftNode)
}

func TestLeaderStepsDownWithoutQuorum(t *testing.T) {
	n := newQuorumTestNode()
	n.role = Leader
	n.leader = 1
	n.recentActive = make(map[NodeID]bool)
	n.electionElapsed = n.electionTimeout - 1

	n.Tick()

	if n.role != Follower {
		t.Fatalf("role = %v, want Follower after losing quorum", n.role)
	}
}

func TestLeaderStaysLeaderWithQuorum(t *testing.T) {
	n := newQuorumTestNode()
	n.role = Leader
	n.leader = 1
	n.recentActive = map[NodeID]bool{2: true}
	n.electionElapsed = n.electionTimeout - 1

	n.Tick()

	if n.role != Leader {
		t.Fatalf("role = %v, want Leader with quorum", n.role)
	}
	if n.recentActive[2] {
		t.Fatal("peer activity was not reset after quorum check")
	}
}

func TestLeaderLeaseRejectsHigherTermVote(t *testing.T) {
	n := newQuorumTestNode()
	n.role = Leader
	n.leader = 1
	n.hs.Term = 5

	n.Step(Message{
		From: 3,
		To:   1,
		Type: MsgVote,
		Term: 6,
	})

	if n.hs.Term != 5 {
		t.Fatalf("term = %d, want 5 while leader lease is active", n.hs.Term)
	}
	if n.role != Leader {
		t.Fatalf("role = %v, want Leader", n.role)
	}
}

func TestFollowerLeaseRejectsHigherTermVote(t *testing.T) {
	n := newQuorumTestNode()
	n.leader = 2
	n.hs.Term = 5
	n.electionElapsed = 0

	n.Step(Message{
		From: 3,
		To:   1,
		Type: MsgVote,
		Term: 6,
	})

	if n.hs.Term != 5 {
		t.Fatalf("term = %d, want 5 while follower lease is active", n.hs.Term)
	}
	if n.leader != 2 {
		t.Fatalf("leader = %d, want 2", n.leader)
	}
}

func TestPreVoteResponseEchoesRequestedTerm(t *testing.T) {
	n := newQuorumTestNode()
	n.hs.Term = 5

	n.Step(Message{
		From:         2,
		To:           1,
		Type:         MsgPreVote,
		Term:         6,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if n.hs.Term != 5 {
		t.Fatalf("responder term = %d, want 5", n.hs.Term)
	}
	if len(n.msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(n.msgs))
	}

	resp := n.msgs[0]
	if resp.Type != MsgPreVoteResp || resp.Term != 6 || resp.Reject {
		t.Fatalf("unexpected PreVote response: %+v", resp)
	}
}

func TestRejectedHigherTermPreVoteResponseUpdatesTerm(t *testing.T) {
	n := newQuorumTestNode()
	n.role = PreCandidate
	n.hs.Term = 5

	n.Step(Message{
		From:   2,
		To:     1,
		Type:   MsgPreVoteResp,
		Term:   6,
		Reject: true,
	})

	if n.hs.Term != 6 {
		t.Fatalf("term = %d, want 6", n.hs.Term)
	}
	if n.role != Follower {
		t.Fatalf("role = %v, want Follower", n.role)
	}
}
