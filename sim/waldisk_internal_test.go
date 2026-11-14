package sim

import (
	"strings"
	"testing"

	"github.com/ANAXMD/ForgeDB/raft"
)

// scenario crashes and restarts the leader repeatedly while proposing, then
// heals and settles. It uses only the public simulator API.
func scenario(cfg Config) *Sim {
	s := New(cfg)
	s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok })
	proposed := 0
	for i := 0; i < 100 && proposed < 30; i++ {
		if l, ok := s.Leader(); ok && s.Propose(l, []byte{byte(proposed)}) {
			proposed++
		}
		s.Run(10)
		if l, ok := s.Leader(); ok {
			s.Crash(l)
			s.Run(20)
			s.Restart(l)
		}
	}
	s.Heal()
	s.Run(5_000)
	return s
}

// The WAL is the default disk: every host runs on the real persistence layer.
func TestSimDefaultsToWALDisk(t *testing.T) {
	s := New(Config{Seed: 1, N: 3})
	for id, h := range s.hosts {
		if _, ok := h.disk.(*WALDisk); !ok {
			t.Fatalf("node %d uses %T, want *WALDisk", id, h.disk)
		}
	}
	f := New(Config{Seed: 1, N: 3, FakeDisk: true})
	for id, h := range f.hosts {
		if _, ok := h.disk.(*Disk); !ok {
			t.Fatalf("FakeDisk: node %d uses %T, want *Disk", id, h.disk)
		}
	}
}

// The WAL disk must be indistinguishable from the fake disk for a correct node:
// same seed, same crashes, identical trace and identical applied logs on every
// node, and both runs pass the safety checker. A difference means the WAL
// returned a different state after a restart.
func TestWALDiskMatchesFakeDisk(t *testing.T) {
	for seed := int64(1); seed <= 30; seed++ {
		cfg := Config{Seed: seed, N: 5, DropProb: 0.1, PreVote: seed%2 == 0, CheckQuorum: seed%3 == 0}
		real := scenario(cfg)
		cfg.FakeDisk = true
		fake := scenario(cfg)
		real.MustBeSafe(t)
		fake.MustBeSafe(t)
		if real.TraceHash() != fake.TraceHash() {
			t.Fatalf("seed %d: trace with the WAL (%d) differs from the trace with the fake disk (%d)", seed, real.TraceHash(), fake.TraceHash())
		}
		applied := 0
		for _, id := range real.IDs() {
			a, b := real.Applied(id), fake.Applied(id)
			if len(a) != len(b) {
				t.Fatalf("seed %d node %d: applied %d entries with the WAL, %d with the fake disk", seed, id, len(a), len(b))
			}
			for i := range a {
				if a[i].Term != b[i].Term || a[i].Index != b[i].Index || string(a[i].Data) != string(b[i].Data) {
					t.Fatalf("seed %d node %d: applied entry %d differs: WAL %+v, fake %+v", seed, id, i, a[i], b[i])
				}
			}
			applied += len(a)
		}
		if applied == 0 {
			t.Fatalf("seed %d: nothing was applied, the comparison proves nothing", seed)
		}
	}
}

// Everything unsynced is lost on a crash; everything synced survives, and is
// read back through WAL recovery (not from a Go struct).
func TestWALDiskDurability(t *testing.T) {
	d := NewWALDisk()
	d.Save(&raft.HardState{Term: 2, VotedFor: 3}, []raft.Entry{{Term: 2, Index: 1, Data: []byte("kept")}})
	d.Sync()
	d.Save(&raft.HardState{Term: 3}, []raft.Entry{{Term: 3, Index: 2, Data: []byte("lost")}})
	d.Crash()
	got := d.Load()
	if got.HardState != (raft.HardState{Term: 2, VotedFor: 3}) || len(got.Log) != 1 || string(got.Log[0].Data) != "kept" {
		t.Fatalf("after crash: %+v", got)
	}
}

// Cold start: every node loses power at once and restarts from its WAL bytes.
// A committed entry must still be in the log of the new leader.
func TestColdStartFromWALKeepsCommittedEntry(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Config{Seed: seed, N: 5})
		s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok })
		l, _ := s.Leader()
		s.Propose(l, []byte("committed"))
		if !s.RunUntil(5_000, func() bool { return len(s.Applied(l)) >= 2 }) {
			t.Fatalf("seed %d: entry not applied", seed)
		}
		for _, id := range s.IDs() {
			s.Crash(id)
		}
		s.Run(50)
		for _, id := range s.IDs() {
			s.Restart(id)
		}
		if !s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok }) {
			t.Fatalf("seed %d: no leader after cold start", seed)
		}
		l2, _ := s.Leader()
		s.Propose(l2, []byte("next"))
		s.RunUntil(5_000, func() bool { return len(s.Applied(l2)) >= 3 })
		s.MustBeSafe(t)
		found := false
		for _, e := range s.Applied(l2) {
			if string(e.Data) == "committed" {
				found = true
			}
		}
		if !found {
			t.Fatalf("seed %d: committed entry lost across a full-cluster restart", seed)
		}
	}
}

// A restart really goes through WAL recovery: damage in the middle of a node's
// log makes the restart refuse to start instead of silently losing entries.
func TestRestartRefusesToStartOnCorruptWAL(t *testing.T) {
	s := New(Config{Seed: 7, N: 3})
	s.RunUntil(5_000, func() bool { _, ok := s.Leader(); return ok })
	l, _ := s.Leader()
	for i := 0; i < 5; i++ {
		s.Propose(l, []byte{byte(i)})
		s.Run(100)
	}
	victim := raft.NodeID(1)
	if victim == l {
		victim = 2
	}
	s.Crash(victim)
	d := s.hosts[victim].disk.(*WALDisk)
	data := d.fs.Bytes()
	if len(data) < 60 {
		t.Fatalf("WAL unexpectedly small: %d bytes", len(data))
	}
	data[len(data)/4] ^= 0xFF // inside an early record, with records after it
	d.fs.Truncate(0)
	d.fs.Seek(0, 0)
	d.fs.Write(data)
	d.fs.Sync()

	defer func() {
		r := recover()
		msg, _ := r.(string)
		if !strings.Contains(msg, "WAL recovery failed") {
			t.Fatalf("expected the restart to refuse to start on a corrupt WAL, got %v", r)
		}
	}()
	s.Restart(victim)
	t.Fatal("restart succeeded on a corrupt WAL")
}
