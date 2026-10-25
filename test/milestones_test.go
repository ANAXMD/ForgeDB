// Milestone tests. These are RED on purpose: they run against the real
// raft.New and fail until you implement each milestone. Work top to bottom.
//
//	go test ./test -run M2 -v
//	go test ./test -seed=48213 -run M2 -v     # replay one exact run
package test

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/anishathalye/porcupine"

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

// M3: reproduce paper Figure 8 and assert committed entries survive.
func TestM3_Figure8(t *testing.T) {
	for _, seed := range seeds(30) {
		s := sim.New(sim.Config{Seed: seed, N: 5})

		// Force the Figure 8 term overlaps:
		// 1. Leader 1 proposes an entry, partially replicates it, and crashes.
		// 2. Leader 2 is elected, proposes a new entry, partially replicates, and crashes.
		// 3. Leader 1 is restarted and tries to finish replicating its old stranded entries.
		for i := 0; i < 5; i++ {
			s.RunUntil(2_000, func() bool { _, ok := s.Leader(); return ok })
			l1, ok := s.Leader()
			if !ok {
				continue
			}

			s.Propose(l1, []byte{1})
			s.Run(2) // 2 ticks: enough to send MsgApp, but NOT enough to get a quorum of responses
			s.Crash(l1)

			s.RunUntil(2_000, func() bool { _, ok := s.Leader(); return ok })
			l2, ok := s.Leader()
			if !ok {
				continue
			}

			s.Propose(l2, []byte{2})
			s.Run(2)
			s.Crash(l2)

			s.Restart(l1) // L1 comes back with its old uncommitted entries
		}

		// Resurrect everyone, heal the network, and allow the cluster to fully converge.
		for _, id := range s.IDs() {
			s.Restart(id)
		}
		s.Heal()
		s.Run(5_000)

		// If your Figure 8 commit rule (in advanceCommit) or the No-Op entry fix is missing,
		// the simulator's safety checker will panic right here because a committed entry was lost.
		s.MustBeSafe(t)
	}
}

// M4: random Crash/Restart for 60 virtual seconds, MustBeSafe at the end
func TestM4_CrashRecoveryKeepsCommitted(t *testing.T) {
	for _, seed := range seeds(30) {
		s := sim.New(sim.Config{Seed: seed, N: 5, DropProb: 0.1})
		s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok })

		proposed := 0
		// Propose commands while constantly crashing and restarting the leader
		for i := 0; i < 100 && proposed < 30; i++ {
			if l, ok := s.Leader(); ok && s.Propose(l, []byte{byte(proposed)}) {
				proposed++
			}
			s.Run(10)

			if l, ok := s.Leader(); ok {
				s.Crash(l)
				s.Run(20)
				s.Restart(l) // Node must read HardState and Log from disk here
			}
		}

		s.Heal()
		s.Run(5_000)
		s.MustBeSafe(t)
	}
}

type mockT struct {
	failed bool
	msg    string
}

func (m *mockT) Helper() {}
func (m *mockT) Fatalf(format string, args ...any) {
	m.failed = true
	m.msg = fmt.Sprintf(format, args...)
}

// M4: with SkipSync=true the fuzz run MUST report a violation; proves the harness has teeth
func TestM4_SkipSyncIsCaught(t *testing.T) {
	var mock mockT
	defer func() {
		if r := recover(); r != nil {
			// Caught a panic
			return
		}
		if !mock.failed {
			t.Fatal("expected a safety violation due to SkipSync=true, but cluster reported no violations")
		}
	}()

	s := sim.New(sim.Config{Seed: 1, N: 5, SkipSync: true})
	s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok })

	// 1. Force a commit and GUARANTEE it is applied before crashing
	l, ok := s.Leader()
	if !ok {
		t.Fatal("no leader elected")
	}
	s.Propose(l, []byte("doom"))
	if !s.RunUntil(5_000, func() bool { return len(s.Applied(l)) >= 2 }) {
		t.Fatal("failed to apply initial entry")
	}

	// 2. Wipe out the entire cluster so they all suffer amnesia.
	// Running 50ms while down drains all in-flight messages so nodes don't receive pre-crash state.
	for _, id := range s.IDs() {
		s.Crash(id)
	}
	s.Run(50)
	for _, id := range s.IDs() {
		s.Restart(id)
	}

	// 3. Elect a new leader from the amnesiac nodes
	s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok })
	l2, ok := s.Leader()
	if !ok {
		t.Fatal("no leader elected after restart")
	}

	// 4. Force the new leader to overwrite the exact same log index.
	s.Propose(l2, []byte("corrupted"))
	s.RunUntil(5_000, func() bool { return len(s.Applied(l2)) >= 2 })

	// 5. Check with MustBeSafe using mockT to catch the safety violation
	s.MustBeSafe(&mock)
}

func TestM5_PreVoteStopsDisruption(t *testing.T) {
	for _, seed := range seeds(30) {
		s := sim.New(sim.Config{Seed: seed, N: 5, PreVote: true})
		if !s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok }) {
			t.Fatalf("seed %d: no leader elected within 5 virtual seconds", seed)
		}
		leader, ok := s.Leader()
		if !ok {
			t.Fatalf("seed %d: leader disappeared", seed)
		}
		initialTerm := s.Node(leader).Status().Term

		// Pick a follower to isolate
		var follower raft.NodeID
		for _, id := range s.IDs() {
			if id != leader {
				follower = id
				break
			}
		}

		// Completely isolate one follower from the rest of the network
		s.Isolate(follower)

		// Let the cluster run for at least 500 virtual ticks (10ms per tick)
		s.Run(500 * 10)

		// Heal the partition / reconnect the node and run for another 100 ticks
		s.Heal()
		s.Run(100 * 10)

		s.MustBeSafe(t)

		currentLeader, ok := s.Leader()
		if !ok || currentLeader != leader {
			t.Fatalf("seed %d: leader stepped down (got %v, want %v)", seed, currentLeader, leader)
		}
		for _, id := range s.IDs() {
			if st := s.Node(id).Status(); st.Term != initialTerm {
				t.Fatalf("seed %d: node %d is in term %d, expected initial term %d", seed, id, st.Term, initialTerm)
			}
		}
	}
}
type kvOpKind uint8

const (
	opGet kvOpKind = iota
	opPut
)

type kvInput struct {
	Op    kvOpKind
	Key   string
	Value string
}

type kvOutput struct {
	Value string
	OK    bool
}

type kvCommand struct {
	ReqID uint64
	Op    kvOpKind
	Key   string
	Value string
}

type clientReq struct {
	clientID   int
	targetTime int64
	input      kvInput
	respCh     chan clientResp
}

type clientResp struct {
	output     kvOutput
	callTime   int64
	returnTime int64
	ok         bool
}

type pendingOp struct {
	req      clientReq
	callTime int64
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := make(map[string][]porcupine.Operation)
		for _, op := range history {
			inp := op.Input.(kvInput)
			m[inp.Key] = append(m[inp.Key], op)
		}
		var partitions [][]porcupine.Operation
		for _, ops := range m {
			partitions = append(partitions, ops)
		}
		return partitions
	},
	Init: func() any {
		return ""
	},
	Step: func(state any, input any, output any) (bool, any) {
		val := state.(string)
		inp := input.(kvInput)
		out := output.(kvOutput)
		if inp.Op == opPut {
			return true, inp.Value
		}
		return out.Value == val, val
	},
	Equal: func(s1, s2 any) bool {
		return s1.(string) == s2.(string)
	},
	DescribeOperation: func(in, out any) string {
		inp := in.(kvInput)
		outp := out.(kvOutput)
		if inp.Op == opPut {
			return fmt.Sprintf("Put('%s', '%s')", inp.Key, inp.Value)
		}
		return fmt.Sprintf("Get('%s') -> '%s'", inp.Key, outp.Value)
	},
	DescribeState: func(st any) string {
		return fmt.Sprintf("'%s'", st.(string))
	},
}

func TestM6_LinearizableKV(t *testing.T) {
	s := sim.New(sim.Config{Seed: 42, N: 5})
	if !s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok }) {
		t.Fatal("no leader elected")
	}

	const numClients = 4
	reqCh := make(chan clientReq, numClients*2)
	done := make(chan struct{})

	var historyMu sync.Mutex
	var history []porcupine.Operation

	var wg sync.WaitGroup
	keys := []string{"k1", "k2", "k3"}

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(clientID + 12345)))
			var opCount int
			var nextTarget int64
			for {
				select {
				case <-done:
					return
				default:
				}

				key := keys[rng.Intn(len(keys))]
				var inp kvInput
				if rng.Intn(2) == 0 {
					opCount++
					inp = kvInput{Op: opPut, Key: key, Value: fmt.Sprintf("v-%d-%d", clientID, opCount)}
				} else {
					inp = kvInput{Op: opGet, Key: key}
				}

				respCh := make(chan clientResp, 1)
				select {
				case reqCh <- clientReq{clientID: clientID, targetTime: nextTarget, input: inp, respCh: respCh}:
				case <-done:
					return
				}

				select {
				case resp := <-respCh:
					if resp.ok {
						historyMu.Lock()
						history = append(history, porcupine.Operation{
							ClientId: clientID,
							Input:    inp,
							Call:     resp.callTime,
							Output:   resp.output,
							Return:   resp.returnTime,
						})
						historyMu.Unlock()
						// Pace next request: 200 to 500 virtual ms later
						nextTarget = resp.returnTime + int64(rng.Intn(300)+200)
					}
				case <-done:
					return
				}
			}
		}(c)
	}

	deadline := s.Now() + 60_000 // 60 virtual seconds
	kvState := make(map[string]string)
	appliedCount := make(map[raft.NodeID]int)
	pending := make(map[uint64]pendingOp)
	var waiting []clientReq
	var reqID uint64

	for s.Now() < deadline {
		// 1. Ingest any incoming requests into waiting list
	drain:
		for {
			select {
			case req := <-reqCh:
				waiting = append(waiting, req)
			default:
				break drain
			}
		}

		// 2. Dispatch any requests whose targetTime has arrived
		var remaining []clientReq
		l, hasLeader := s.Leader()
		for _, req := range waiting {
			if s.Now() >= req.targetTime && hasLeader {
				reqID++
				cmd := kvCommand{
					ReqID: reqID,
					Op:    req.input.Op,
					Key:   req.input.Key,
					Value: req.input.Value,
				}
				bytes, err := json.Marshal(cmd)
				if err != nil {
					t.Fatalf("json marshal error: %v", err)
				}
				callTime := s.Now()
				if s.Propose(l, bytes) {
					pending[reqID] = pendingOp{
						req:      req,
						callTime: callTime,
					}
				} else {
					req.respCh <- clientResp{ok: false}
				}
			} else {
				remaining = append(remaining, req)
			}
		}
		waiting = remaining

		// 3. Advance simulation
		s.Run(20)

		// 4. Process newly applied entries on the leader
		if l, ok := s.Leader(); ok {
			applied := s.Applied(l)
			idx := appliedCount[l]
			for idx < len(applied) {
				entry := applied[idx]
				idx++
				if len(entry.Data) == 0 {
					continue
				}
				var cmd kvCommand
				if err := json.Unmarshal(entry.Data, &cmd); err != nil {
					continue
				}
				var out kvOutput
				if cmd.Op == opPut {
					kvState[cmd.Key] = cmd.Value
					out = kvOutput{Value: cmd.Value, OK: true}
				} else {
					out = kvOutput{Value: kvState[cmd.Key], OK: true}
				}

				if p, exists := pending[cmd.ReqID]; exists {
					delete(pending, cmd.ReqID)
					p.req.respCh <- clientResp{
						callTime:   p.callTime,
						returnTime: s.Now(),
						output:     out,
						ok:         true,
					}
				}
			}
			appliedCount[l] = idx
		}

		runtime.Gosched()
	}

	// Wait up to 5 virtual seconds for any pending operations to finish committing
	s.RunUntil(5_000, func() bool { return len(pending) == 0 })

	// Signal clients to finish and wait for all goroutines
	close(done)
	wg.Wait()

	s.MustBeSafe(t)

	if len(history) == 0 {
		t.Fatal("no operations were recorded in history")
	}

	t.Logf("Checking linearizability of %d operations with Porcupine...", len(history))
	if !porcupine.CheckOperations(kvModel, history) {
		t.Fatalf("history of %d operations is NOT linearizable", len(history))
	}
}

var _ = raft.Follower // keep the import until you use raft types in later tests
