package raft

// raftNode is the skeleton you will fill in. The plumbing (log storage,
// Ready/Advance bookkeeping, restore-from-disk) is done and tested through
// the simulator; the Raft *logic* is marked TODO(Mx) per milestone.
type raftNode struct {
	cfg Config

	// Persistent state (mirrored to disk through Ready).
	hs  HardState
	log []Entry // log[0] has Index 1

	// Volatile state.
	role    Role
	leader  NodeID
	commit  Index
	applied Index

	electionElapsed  int
	electionTimeout  int // randomized
	heartbeatElapsed int

	// TODO(M2): votes map[NodeID]bool, preVotes map[NodeID]bool
	// TODO(M3): leader-only nextIndex / matchIndex per peer

	// Ready bookkeeping.
	hsDirty   bool
	dirtyFrom Index // lowest log index modified since the last Advance (0 = clean)
	msgs      []Message
	readyAt   Index // commit index captured by the last Ready(), applied on Advance
}

// New restores a node from persisted state. Always starts as a Follower.
func New(cfg Config, p Persisted) Node {
	n := &raftNode{cfg: cfg, hs: p.HardState, log: append([]Entry(nil), p.Log...)}
	n.becomeFollower(n.hs.Term, 0)
	return n
}

// ---------- plumbing (done) ----------

func (n *raftNode) lastIndex() Index { return Index(len(n.log)) }

func (n *raftNode) lastTerm() Term {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

// termAt returns the term at index i; (0, true) for i == 0; ok=false if i is beyond the log.
func (n *raftNode) termAt(i Index) (Term, bool) {
	if i == 0 {
		return 0, true
	}
	if i > n.lastIndex() {
		return 0, false
	}
	return n.log[i-1].Term, true
}

// appendLocal replaces the log from ents[0].Index onward and marks it dirty so
// the next Ready() asks the driver to persist it. Panics on a gap, because a
// gap is always a bug in the caller.
func (n *raftNode) appendLocal(ents ...Entry) {
	if len(ents) == 0 {
		return
	}
	first := ents[0].Index
	if first == 0 || first > n.lastIndex()+1 {
		panic("raft: log gap in appendLocal")
	}
	if first <= n.commit {
		panic("raft: attempt to overwrite committed entries") // safety invariant!
	}
	n.log = append(n.log[:first-1], ents...)
	if n.dirtyFrom == 0 || first < n.dirtyFrom {
		n.dirtyFrom = first
	}
}

func (n *raftNode) setHardState(term Term, votedFor NodeID) {
	if n.hs.Term == term && n.hs.VotedFor == votedFor {
		return
	}
	n.hs = HardState{Term: term, VotedFor: votedFor}
	n.hsDirty = true
}

func (n *raftNode) send(m Message) {
	m.From = n.cfg.ID
	n.msgs = append(n.msgs, m)
}

func (n *raftNode) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTicks + n.cfg.Rand.Intn(n.cfg.ElectionTicks)
}

func (n *raftNode) becomeFollower(term Term, leader NodeID) {
	if term > n.hs.Term {
		n.setHardState(term, 0) // new term => forget our vote
	}
	n.role = Follower
	n.leader = leader
	n.heartbeatElapsed = 0
	n.resetElectionTimer()
}

func (n *raftNode) Status() Status {
	return Status{ID: n.cfg.ID, Term: n.hs.Term, Role: n.role, Leader: n.leader,
		Commit: n.commit, Applied: n.applied, LastIndex: n.lastIndex()}
}

func (n *raftNode) Log() []Entry { return append([]Entry(nil), n.log...) }

func (n *raftNode) Ready() (Ready, bool) {
	var rd Ready
	if n.hsDirty {
		hs := n.hs
		rd.HardState = &hs
	}
	if n.dirtyFrom != 0 {
		rd.Entries = append([]Entry(nil), n.log[n.dirtyFrom-1:]...)
	}
	rd.Messages = n.msgs
	if n.commit > n.applied {
		rd.Committed = append([]Entry(nil), n.log[n.applied:n.commit]...)
		n.readyAt = n.commit
	}
	if rd.Empty() {
		return Ready{}, false
	}
	return rd, true
}

func (n *raftNode) Advance() {
	n.hsDirty = false
	n.dirtyFrom = 0
	n.msgs = nil
	if n.readyAt > n.applied {
		n.applied = n.readyAt
	}
}

// ---------- the actual Raft (YOUR WORK) ----------

// Tick advances logical time by one unit.
func (n *raftNode) Tick() {
	switch n.role {
	case Follower, PreCandidate, Candidate:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			// TODO(M2): start an election (PreVote first if cfg.PreVote):
			//   become (Pre)Candidate, bump term, vote for self, persist,
			//   send MsgVote to every peer, reset the election timer.
			n.resetElectionTimer() // placeholder so the skeleton doesn't spin
		}
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			// TODO(M2/M3): broadcast heartbeats / AppendEntries.
			n.heartbeatElapsed = 0
		}
		// TODO(M5): if cfg.CheckQuorum, step down when no quorum was heard from.
	}
}

// Step processes one incoming message.
func (n *raftNode) Step(m Message) {
	// TODO(M2): term handling first, for EVERY message type:
	//   m.Term > ours  => becomeFollower(m.Term, ...)
	//   m.Term < ours  => reply with our term (or ignore) and return
	// then dispatch on m.Type:
	//   MsgVote/MsgVoteResp           (M2, incl. "candidate's log at least as up-to-date")
	//   MsgApp/MsgAppResp/Heartbeat   (M3, incl. prevLogIndex/prevLogTerm check,
	//                                  conflict truncation, commit advance)
	//   MsgPreVote/MsgPreVoteResp     (M5)
	_ = m
}

// Propose appends a client command if we are the leader.
func (n *raftNode) Propose(data []byte) bool {
	if n.role != Leader {
		return false
	}
	// TODO(M3): n.appendLocal(Entry{Term: n.hs.Term, Index: n.lastIndex()+1, Data: data});
	//           then replicate. Remember: a leader may only advance commit by
	//           counting replicas for entries of ITS OWN term (paper Figure 8).
	_ = data
	return false
}
