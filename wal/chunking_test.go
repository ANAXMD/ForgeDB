package wal

import (
	"bytes"
	"math/rand"
	"os"
	"testing"

	"github.com/ANAXMD/ForgeDB/raft"
)

// withChunkLimit forces tiny records so that every Save is split into many.
func withChunkLimit(t *testing.T, n int) {
	t.Helper()
	old := chunkLimit
	chunkLimit = n
	t.Cleanup(func() { chunkLimit = old })
}

type zeroRand struct{}

func (zeroRand) Intn(int) int { return 0 }

// A recovered state must be one Raft can start from. raft.New validates it (log
// starts at 1, no gaps, terms never decrease, no entry term above the persisted
// term) and panics otherwise.
func mustStartRaft(t *testing.T, p raft.Persisted, what string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s: raft.New rejected the recovered state: %v", what, r)
		}
	}()
	raft.New(raft.Config{ID: 1, Peers: []raft.NodeID{1, 2, 3}, ElectionTicks: 10, HeartbeatTicks: 1, Rand: zeroRand{}}, p)
}

func sizedEntries(term raft.Term, first raft.Index, n, size int, fill byte) []raft.Entry {
	var es []raft.Entry
	for i := 0; i < n; i++ {
		es = append(es, raft.Entry{Term: term, Index: first + raft.Index(i), Data: bytes.Repeat([]byte{fill + byte(i)}, size)})
	}
	return es
}

// Splitting contract: the hard state is in the first chunk, entries are never
// cut, they stay in index order, every chunk except the last is marked "more",
// no chunk exceeds the limit, and nothing is lost or invented.
func TestChunksAreBoundedOrderedAndLossless(t *testing.T) {
	withChunkLimit(t, 100)
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 500; iter++ {
		var hs *raft.HardState
		if rng.Intn(2) == 0 {
			hs = &raft.HardState{Term: 5, VotedFor: 2}
		}
		var es []raft.Entry
		for i, n := 0, rng.Intn(12); i < n; i++ {
			d := make([]byte, rng.Intn(50))
			rng.Read(d)
			es = append(es, raft.Entry{Term: 5, Index: raft.Index(10 + i), Data: d})
		}
		if hs == nil && len(es) == 0 {
			continue
		}
		chunks, err := encodeBatchChunks(hs, es)
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}
		var got []raft.Entry
		var gotHS *raft.HardState
		for i, c := range chunks {
			if len(c) > chunkLimit {
				t.Fatalf("iter %d: chunk %d is %d bytes, limit %d", iter, i, len(c), chunkLimit)
			}
			op, more, err := decodeBatch(c)
			if err != nil {
				t.Fatalf("iter %d chunk %d: %v", iter, i, err)
			}
			if more != (i < len(chunks)-1) {
				t.Fatalf("iter %d: chunk %d of %d has more=%v", iter, i, len(chunks), more)
			}
			if op.hs != nil {
				if i != 0 {
					t.Fatalf("iter %d: hard state in chunk %d, must be only in the first", iter, i)
				}
				gotHS = op.hs
			}
			if op.hs == nil && len(op.ents) == 0 {
				t.Fatalf("iter %d: chunk %d is empty", iter, i)
			}
			got = append(got, op.ents...)
		}
		if (hs == nil) != (gotHS == nil) || (hs != nil && *hs != *gotHS) {
			t.Fatalf("iter %d: hard state changed", iter)
		}
		if len(got) != len(es) {
			t.Fatalf("iter %d: %d entries out, %d in", iter, len(got), len(es))
		}
		for i := range es {
			if got[i].Index != es[i].Index || got[i].Term != es[i].Term || !bytes.Equal(got[i].Data, es[i].Data) {
				t.Fatalf("iter %d: entry %d changed", iter, i)
			}
		}
	}
}

// buildSplitLog writes three synced Saves: a small one, a Save split into many
// records, and a small one after it. It returns the file, the offsets where each
// Save starts, and the model state after each Save.
func buildSplitLog(t *testing.T) (data []byte, saveStart []int, states []raft.Persisted, recordStarts []int) {
	t.Helper()
	fs := NewMemFS()
	w, err := Open(fs)
	if err != nil {
		t.Fatal(err)
	}
	type save struct {
		hs raft.HardState
		es []raft.Entry
	}
	saves := []save{
		{raft.HardState{Term: 1, VotedFor: 1}, sizedEntries(1, 1, 2, 10, 'a')},
		{raft.HardState{Term: 3, VotedFor: 2}, sizedEntries(3, 3, 8, 40, 'k')}, // many chunks
		{raft.HardState{Term: 3, VotedFor: 2}, sizedEntries(3, 11, 2, 10, 'z')},
	}
	model := raft.Persisted{Log: []raft.Entry{}}
	states = append(states, clonePersisted(model))
	for _, s := range saves {
		saveStart = append(saveStart, len(fs.Bytes()))
		hs := s.hs
		if err := w.Save(&hs, s.es); err != nil {
			t.Fatal(err)
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		model.HardState = s.hs
		model.Log = append(model.Log, s.es...)
		states = append(states, clonePersisted(model))
	}
	data = fs.Bytes()
	for off := 0; off < len(data); {
		recordStarts = append(recordStarts, off)
		_, length, _, ok := parseHeader(data[off : off+recordHeaderSize])
		if !ok {
			t.Fatal("test setup: bad header")
		}
		off += recordHeaderSize + int(length)
	}
	return
}

// A Save split across records is all-or-nothing. Cutting the file at EVERY byte
// offset must recover the state after the last Save that is completely present:
// never part of a split Save. Each recovered state must also be one Raft can
// start from, and a cut inside a Save truncates back to where that Save began.
func TestSplitSaveIsAllOrNothingAtEveryCutPoint(t *testing.T) {
	withChunkLimit(t, 100)
	data, saveStart, states, recordStarts := buildSplitLog(t)
	groupRecords := 0
	for _, r := range recordStarts {
		if r >= saveStart[1] && r < saveStart[2] {
			groupRecords++
		}
	}
	if groupRecords < 6 {
		t.Fatalf("test setup: the middle Save was split into only %d records", groupRecords)
	}
	saveEnd := append(append([]int(nil), saveStart[1:]...), len(data))

	for cut := 0; cut <= len(data); cut++ {
		m := NewMemFS()
		m.cur = append([]byte(nil), data[:cut]...)
		m.durable = append([]byte(nil), data[:cut]...)
		w, err := Open(m)
		if err != nil {
			t.Fatalf("cut %d: recovery failed: %v", cut, err)
		}
		complete := 0
		for complete < len(saveEnd) && saveEnd[complete] <= cut {
			complete++
		}
		want := states[complete]
		if got := w.Load(); !samePersisted(got, want) {
			t.Fatalf("cut %d (inside save %d): recovered %d entries %+v, want the state after %d whole saves (%d entries %+v): a split Save was partly applied",
				cut, complete, len(got.Log), got.HardState, complete, len(want.Log), want.HardState)
		}
		wantSize := 0
		if complete > 0 {
			wantSize = saveEnd[complete-1]
		}
		if int(len(m.Bytes())) != wantSize {
			t.Fatalf("cut %d: file is %d bytes after recovery, want truncation to the start of the unfinished Save at %d", cut, len(m.Bytes()), wantSize)
		}
		mustStartRaft(t, w.Load(), "cut "+itoa(cut))
	}
}

// Damage to any byte of a split Save that is followed by more data must refuse to
// start and leave the file untouched, like any other record in the middle.
func TestDamageInsideASplitSaveFailsStop(t *testing.T) {
	withChunkLimit(t, 100)
	data, saveStart, _, _ := buildSplitLog(t)
	dir := t.TempDir()
	for pos := saveStart[1]; pos < saveStart[2]; pos++ {
		d := append([]byte(nil), data...)
		d[pos] ^= 0xFF
		path := dir + "/split.wal"
		if err := writeFileForTest(path, d); err != nil {
			t.Fatal(err)
		}
		mustFailStop(t, path, "flip byte "+itoa(pos)+" of a split Save")
	}
}

// Crash property with split Saves: random Saves of random size, a power cut in
// the middle of Sync with every possible number of bytes surviving. Recovery
// yields the synced Saves plus only the WHOLE Saves that fit, and the state is
// always startable by Raft.
func TestCrashPropertyWithSplitSaves(t *testing.T) {
	withChunkLimit(t, 90)
	for seed := int64(1); seed <= 150; seed++ {
		rng := rand.New(rand.NewSource(seed))
		fs := &crashFS{MemFS: NewMemFS()}
		w, err := Open(fs)
		if err != nil {
			t.Fatal(err)
		}
		model := raft.Persisted{Log: []raft.Entry{}}
		states := []raft.Persisted{clonePersisted(model)}
		var saveBytes []int // bytes each Save adds to the file (all its records)
		term := raft.Term(1)

		save := func() {
			if rng.Intn(3) == 0 {
				term++
			}
			hs := raft.HardState{Term: term, VotedFor: raft.NodeID(rng.Intn(4))}
			first := 1 + rng.Intn(len(model.Log)+1)
			var es []raft.Entry
			for i, n := 0, 1+rng.Intn(6); i < n; i++ {
				d := make([]byte, rng.Intn(40))
				rng.Read(d)
				es = append(es, raft.Entry{Term: term, Index: raft.Index(first + i), Data: d})
			}
			if err := w.Save(&hs, es); err != nil {
				t.Fatalf("seed %d: Save: %v", seed, err)
			}
			if err := applyOp(&model, pendingOp{hs: &hs, ents: es}); err != nil {
				t.Fatalf("seed %d: model: %v", seed, err)
			}
			chunks, _ := encodeBatchChunks(&hs, es)
			n := 0
			for _, c := range chunks {
				r, _ := encodeRecord(recordBatch, c)
				n += len(r)
			}
			saveBytes = append(saveBytes, n)
			states = append(states, clonePersisted(model))
		}

		durable := 0
		for round := rng.Intn(3); round > 0; round-- {
			for k := 1 + rng.Intn(3); k > 0; k-- {
				save()
			}
			if err := w.Sync(); err != nil {
				t.Fatalf("seed %d: Sync: %v", seed, err)
			}
			durable = len(saveBytes)
		}
		for k := 1 + rng.Intn(3); k > 0; k-- {
			save()
		}
		fs.crashOnSync = true
		if err := w.Sync(); err == nil {
			t.Fatalf("seed %d: expected a power cut", seed)
		}
		unsynced := fs.MemFS.Unsynced()
		base, dur := fs.MemFS.Bytes(), append([]byte(nil), fs.MemFS.durable...)

		for keep := 0; keep <= unsynced; keep++ {
			m := NewMemFS()
			m.cur, m.durable = append([]byte(nil), base...), append([]byte(nil), dur...)
			m.Crash(keep)
			whole, used := 0, 0
			for _, n := range saveBytes[durable:] {
				if used+n > keep {
					break
				}
				used += n
				whole++
			}
			rec, err := Open(m)
			if err != nil {
				t.Fatalf("seed %d keep %d/%d: recovery failed: %v", seed, keep, unsynced, err)
			}
			want := states[durable+whole]
			if got := rec.Load(); !samePersisted(got, want) {
				t.Fatalf("seed %d keep %d/%d: recovered %d entries, want the state after %d whole Saves (%d entries)",
					seed, keep, unsynced, len(got.Log), durable+whole, len(want.Log))
			}
			mustStartRaft(t, rec.Load(), "seed "+itoa(int(seed))+" keep "+itoa(keep))
		}
	}
}

func writeFileForTest(path string, b []byte) error { return os.WriteFile(path, b, 0600) }
