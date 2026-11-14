package wal

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/ANAXMD/ForgeDB/raft"
)

// crashFS models a power cut in the middle of Sync: the bytes are written, but
// the fsync "never returns". The test then calls MemFS.Crash with some number of
// the unsynced bytes kept (a torn write).
type crashFS struct {
	*MemFS
	crashOnSync bool
}

var errPowerCut = errors.New("power cut during sync")

func (c *crashFS) Sync() error {
	if c.crashOnSync {
		return errPowerCut
	}
	return c.MemFS.Sync()
}

// Crash property (every crash point, with a torn last write): for random
// sequences of Save and Sync calls, a crash in the middle of a Sync that keeps
// any number of bytes of what was being written must recover exactly the state
// after (the records synced before) plus (the whole records that fit in the kept
// bytes). Nothing synced is ever lost and nothing is invented.
func TestCrashPropertyTornSyncRecoversExactPrefix(t *testing.T) {
	for seed := int64(1); seed <= 300; seed++ {
		rng := rand.New(rand.NewSource(seed))
		fs := &crashFS{MemFS: NewMemFS()}
		w, err := Open(fs)
		if err != nil {
			t.Fatal(err)
		}

		model := raft.Persisted{} // state if every record so far were durable
		states := []raft.Persisted{clonePersisted(model)}
		durableCount := 0 // records durable before the crashing sync
		var sizes []int   // encoded size of each record, in order

		step := func() (raft.HardState, []raft.Entry) {
			hs := raft.HardState{Term: raft.Term(1 + rng.Intn(5)), VotedFor: raft.NodeID(rng.Intn(4))}
			first := 1 + rng.Intn(len(model.Log)+1) // may overwrite a suffix
			n := 1 + rng.Intn(3)
			var es []raft.Entry
			for i := 0; i < n; i++ {
				data := make([]byte, rng.Intn(20))
				rng.Read(data)
				es = append(es, raft.Entry{Term: hs.Term, Index: raft.Index(first + i), Data: data})
			}
			return hs, es
		}
		save := func() {
			hs, es := step()
			if err := w.Save(&hs, es); err != nil {
				t.Fatalf("seed %d: Save: %v", seed, err)
			}
			if err := applyOp(&model, pendingOp{hs: &hs, ents: es}); err != nil {
				t.Fatalf("seed %d: model: %v", seed, err)
			}
			payload, _ := encodeBatch(&hs, es)
			rec, _ := encodeRecord(recordBatch, payload)
			sizes = append(sizes, len(rec))
			states = append(states, clonePersisted(model))
		}

		// Some rounds of Save...Sync that complete normally.
		for round := rng.Intn(4); round > 0; round-- {
			for k := 1 + rng.Intn(3); k > 0; k-- {
				save()
			}
			if err := w.Sync(); err != nil {
				t.Fatalf("seed %d: Sync: %v", seed, err)
			}
			durableCount = len(sizes)
		}
		// A final batch whose Sync loses power part-way.
		batch := 1 + rng.Intn(3)
		for k := 0; k < batch; k++ {
			save()
		}
		fs.crashOnSync = true
		if err := w.Sync(); !errors.Is(err, errPowerCut) {
			t.Fatalf("seed %d: expected the power cut, got %v", seed, err)
		}
		unsynced := fs.MemFS.Unsynced()
		total := 0
		for _, s := range sizes[durableCount:] {
			total += s
		}
		if unsynced != total {
			t.Fatalf("seed %d: %d unsynced bytes, expected %d", seed, unsynced, total)
		}

		// Crash with every possible number of surviving bytes (seed 1..300 each
		// try all of them for small batches).
		base := fs.MemFS.Bytes()
		durable := append([]byte(nil), fs.MemFS.durable...)
		for keep := 0; keep <= unsynced; keep++ {
			m := NewMemFS()
			m.cur = append([]byte(nil), base...)
			m.durable = append([]byte(nil), durable...)
			m.Crash(keep)

			whole, used := 0, 0
			for _, s := range sizes[durableCount:] {
				if used+s > keep {
					break
				}
				used += s
				whole++
			}
			rec, err := Open(m)
			if err != nil {
				t.Fatalf("seed %d keep %d/%d: recovery failed: %v", seed, keep, unsynced, err)
			}
			want := states[durableCount+whole]
			if got := rec.Load(); !samePersisted(got, want) {
				t.Fatalf("seed %d keep %d/%d: recovered %d entries %+v, want the state after %d records (%d entries %+v)",
					seed, keep, unsynced, len(got.Log), got.HardState, durableCount+whole, len(want.Log), want.HardState)
			}
		}
	}
}
