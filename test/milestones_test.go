// Milestone tests. These are RED on purpose: they run against the real
// raft.New and fail until you implement each milestone. Work top to bottom.
//
//	go test ./test -run M2 -v
//	go test ./test -seed=48213 -run M2 -v     # replay one exact run
package test

import (
	"flag"
	"testing"

	"github.com/ANAXMD/raft-lab/raft"
	"github.com/ANAXMD/raft-lab/sim"
)

var seedFlag = flag.Int64("seed", 0, "run a single seed (0 = sweep a range)")

// seeds returns the seeds to sweep. Sweeping many seeds is the whole point:
// each one is a different deterministic schedule of delays, drops and crashes.
func seeds(n int) []int64 {
	if *seedFlag != 0 {
		return []int64{*seedFlag}
	}
	out := make([]int64, n)
	for i := range out {
		out[i] = int64(i + 1)
	}
	return out
}

// M2: a healthy 5-node cluster elects exactly one leader.
func TestM2_ElectsOneLeader(t *testing.T) {
	for _, seed := range seeds(50) {
		s := sim.New(sim.Config{Seed: seed, N: 5})
		if !s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok }) {
			t.Fatalf("seed %d: no leader within 5 virtual seconds", seed)
		}
		s.Run(2_000) // stay stable: leadership must not flap in a healthy cluster
		s.MustBeSafe(t)
		lead, ok := s.Leader()
		if !ok {
			t.Fatalf("seed %d: leader disappeared in a healthy cluster", seed)
		}
		for _, id := range s.IDs() {
			if st := s.Node(id).Status(); st.Term != s.Node(lead).Status().Term {
				t.Fatalf("seed %d: node %d is in term %d, leader %d in term %d", seed, id, st.Term, lead, s.Node(lead).Status().Term)
			}
		}
	}
}

// M2: kill the leader, the other four elect a new one.
func TestM2_FailoverAfterLeaderCrash(t *testing.T) {
	for _, seed := range seeds(50) {
		s := sim.New(sim.Config{Seed: seed, N: 5})
		if !s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok }) {
			t.Fatalf("seed %d: no initial leader", seed)
		}
		old, _ := s.Leader()
		s.Crash(old)
		if !s.RunUntil(5_000, func() bool { l, ok := s.Leader(); return ok && l != old }) {
			t.Fatalf("seed %d: no new leader after crashing %d", seed, old)
		}
		s.MustBeSafe(t)
	}
}

// M3: 100 commands, flaky network, all live nodes converge on the committed prefix.
func TestM3_ReplicatesUnderFaults(t *testing.T) {
	for _, seed := range seeds(30) {
		s := sim.New(sim.Config{Seed: seed, N: 5, DropProb: 0.1, DupProb: 0.05})
		s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok })
		proposed := 0
		deadline := s.Now() + 60_000 // never loop forever: bound by virtual time
		for proposed < 100 {
			if s.Now() > deadline {
				t.Fatalf("seed %d: could only propose %d of 100 commands in 60 virtual seconds", seed, proposed)
			}
			if l, ok := s.Leader(); ok && s.Propose(l, []byte{byte(proposed)}) {
				proposed++
			}
			s.Run(20)
		}
		s.Heal()
		s.Run(5_000)
		s.MustBeSafe(t)
		var want int
		for _, id := range s.IDs() {
			if n := len(s.Applied(id)); n > want {
				want = n
			}
		}
		if want < 100 {
			t.Fatalf("seed %d: only %d of 100 commands were committed", seed, want)
		}
		for _, id := range s.IDs() {
			if got := len(s.Applied(id)); got != want {
				t.Fatalf("seed %d: node %d applied %d entries, others %d", seed, id, got, want)
			}
		}
	}
}

// ---- later milestones: write these as you reach them ----

func TestM3_Figure8(t *testing.T) {
	t.Skip("TODO(M3): reproduce paper Figure 8 and assert committed entries survive")
}
func TestM4_CrashRecoveryKeepsCommitted(t *testing.T) {
	t.Skip("TODO(M4): random Crash/Restart for 60 virtual seconds, MustBeSafe at the end")
}
func TestM4_SkipSyncIsCaught(t *testing.T) {
	t.Skip("TODO(M4): with SkipSync=true the fuzz run MUST report a violation; proves the harness has teeth")
}
func TestM5_PreVoteStopsDisruption(t *testing.T) {
	t.Skip("TODO(M5): isolated node rejoins without bumping the cluster's term")
}
func TestM6_LinearizableKV(t *testing.T) {
	t.Skip("TODO(M6): record client history, check with github.com/anishathalye/porcupine")
}

var _ = raft.Follower // keep the import until you use raft types in later tests
