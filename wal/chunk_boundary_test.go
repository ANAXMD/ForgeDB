package wal

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ANAXMD/ForgeDB/raft"
)

// Sizes at the chunk limit. A chunk holding one entry is flags(1) + count(4) +
// entry header(20) + data; a hard state adds 12 more bytes to the chunk it is in.
const (
	boundaryLimit  = 200
	soloOverhead   = 1 + 4 + entryHeaderSize // 25
	withHSOverhead = soloOverhead + 12       // 37
)

func oneEntry(size int) []raft.Entry { return sizedEntries(1, 1, 1, size, 'x') }

func chunkShape(t *testing.T, chunks [][]byte) (hsFirst bool, entsPerChunk []int) {
	t.Helper()
	for i, c := range chunks {
		if len(c) > chunkLimit {
			t.Fatalf("chunk %d is %d bytes, limit %d", i, len(c), chunkLimit)
		}
		op, more, err := decodeBatch(c)
		if err != nil {
			t.Fatal(err)
		}
		if more != (i < len(chunks)-1) {
			t.Fatalf("chunk %d: more=%v", i, more)
		}
		if op.hs != nil {
			if i != 0 {
				t.Fatalf("hard state in chunk %d", i)
			}
			hsFirst = true
		}
		entsPerChunk = append(entsPerChunk, len(op.ents))
	}
	return
}

// An entry is accepted exactly when it fits in a record of its own. Without a hard
// state that is limit-25 bytes of data; one byte more is ErrEntryTooLarge.
func TestEntrySizeBoundaryWithoutHardState(t *testing.T) {
	withChunkLimit(t, boundaryLimit)
	max := boundaryLimit - soloOverhead

	chunks, err := encodeBatchChunks(nil, oneEntry(max))
	if err != nil {
		t.Fatalf("entry that fits exactly was rejected: %v", err)
	}
	if len(chunks) != 1 || len(chunks[0]) != boundaryLimit {
		t.Fatalf("want one chunk of exactly %d bytes, got %d chunks (first %d bytes)", boundaryLimit, len(chunks), len(chunks[0]))
	}
	if _, err := encodeBatchChunks(nil, oneEntry(max+1)); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("one byte over: err = %v, want ErrEntryTooLarge", err)
	}
}

// With a hard state, an entry that fits alongside it (limit-37 bytes) goes in the
// same single record. An entry between limit-36 and limit-25 bytes fits only on
// its own: it must be ACCEPTED, with the hard state in a record of its own ahead
// of it. The hard state never makes an entry too large.
func TestEntrySizeBoundaryWithHardState(t *testing.T) {
	withChunkLimit(t, boundaryLimit)
	hs := &raft.HardState{Term: 1, VotedFor: 2}

	shared := boundaryLimit - withHSOverhead
	chunks, err := encodeBatchChunks(hs, oneEntry(shared))
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || len(chunks[0]) != boundaryLimit {
		t.Fatalf("fits exactly with hard state: want 1 chunk of %d bytes, got %d chunks", boundaryLimit, len(chunks))
	}

	for _, size := range []int{shared + 1, boundaryLimit - soloOverhead} {
		chunks, err := encodeBatchChunks(hs, oneEntry(size))
		if err != nil {
			t.Fatalf("%d-byte entry fits a record without the hard state but was rejected: %v", size, err)
		}
		hsFirst, per := chunkShape(t, chunks)
		if !hsFirst || len(per) != 2 || per[0] != 0 || per[1] != 1 {
			t.Fatalf("%d-byte entry: want [hard state only][entry], got hsFirst=%v entries per chunk %v", size, hsFirst, per)
		}
	}

	if _, err := encodeBatchChunks(hs, oneEntry(boundaryLimit-soloOverhead+1)); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("one byte over the solo limit: err = %v, want ErrEntryTooLarge", err)
	}
}

// The case the old global check got wrong: with a hard state present, an entry
// that only fits in a hard-state-free chunk, sitting behind a small first entry.
func TestLargeEntryInLaterChunkIsAcceptedWithHardState(t *testing.T) {
	withChunkLimit(t, boundaryLimit)
	hs := &raft.HardState{Term: 1, VotedFor: 2}
	big := boundaryLimit - soloOverhead // exact fit without hard state; too big for the old check
	es := []raft.Entry{
		{Term: 1, Index: 1, Data: bytes.Repeat([]byte{'a'}, 10)},
		{Term: 1, Index: 2, Data: bytes.Repeat([]byte{'b'}, big)},
		{Term: 1, Index: 3, Data: bytes.Repeat([]byte{'c'}, 5)},
	}
	chunks, err := encodeBatchChunks(hs, es)
	if err != nil {
		t.Fatalf("rejected: %v", err)
	}
	hsFirst, per := chunkShape(t, chunks)
	if !hsFirst || len(per) != 3 || per[0] != 1 || per[1] != 1 || per[2] != 1 {
		t.Fatalf("unexpected shape: hsFirst=%v per=%v", hsFirst, per)
	}

	// And it survives a real Save, crash and recovery.
	m := NewMemFS()
	w, err := Open(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Save(hs, es); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w2, err := Open(m)
	if err != nil {
		t.Fatal(err)
	}
	got := w2.Load()
	if got.HardState != *hs || len(got.Log) != 3 || !bytes.Equal(got.Log[1].Data, es[1].Data) {
		t.Fatalf("recovered state differs: %+v", got)
	}
	mustStartRaft(t, got, "boundary save")
}

// Same boundaries through the real Save path, with the production limit.
func TestSaveExactFitAtProductionLimit(t *testing.T) {
	max := maxRecordSize - soloOverhead
	m := NewMemFS()
	w, err := Open(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Save(&raft.HardState{Term: 1, VotedFor: 1}, oneEntry(max)); err != nil {
		t.Fatalf("entry that fits a record exactly: %v", err)
	}
	if err := w.Save(nil, sizedEntries(1, 2, 1, max+1, 'y')); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("one byte over: %v", err)
	}
}

// ---- recovery enforces "hard state first, only first" ----

func groupFile(t *testing.T, recs ...[]byte) []byte {
	t.Helper()
	var out []byte
	for _, p := range recs {
		r, err := encodeRecord(recordBatch, p)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, r...)
	}
	return out
}

func chunkOf(t *testing.T, hs *raft.HardState, es []raft.Entry, more bool) []byte {
	t.Helper()
	c, err := encodeChunk(hs, es, more)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Every record has a valid checksum, so only the structure can be wrong: a later
// record of an unfinished Save carries a hard state. The encoder never writes
// that; recovery must refuse to start and leave the file alone, not apply it or
// replace the group's hard state.
func TestRecoveryRejectsHardStateAfterFirstRecordOfAGroup(t *testing.T) {
	hs1 := &raft.HardState{Term: 1, VotedFor: 1}
	hs2 := &raft.HardState{Term: 1, VotedFor: 2}
	e := func(i raft.Index) []raft.Entry { return []raft.Entry{{Term: 1, Index: i, Data: []byte("d")}} }

	cases := map[string][]byte{
		"hard state again in the final record": groupFile(t,
			chunkOf(t, hs1, e(1), true), chunkOf(t, hs2, e(2), false)),
		"hard state again in a middle record": groupFile(t,
			chunkOf(t, hs1, e(1), true), chunkOf(t, hs2, e(2), true), chunkOf(t, nil, e(3), false)),
		"hard state only in a later record": groupFile(t,
			chunkOf(t, nil, e(1), true), chunkOf(t, hs2, e(2), false)),
		"same hard state repeated": groupFile(t,
			chunkOf(t, hs1, e(1), true), chunkOf(t, hs1, e(2), false)),
		"hard-state-only record after the first": groupFile(t,
			chunkOf(t, hs1, e(1), true), chunkOf(t, hs2, nil, false)),
		"malformed group inside a longer log": append(
			groupFile(t, chunkOf(t, hs1, e(1), false)),
			groupFile(t, chunkOf(t, nil, e(2), true), chunkOf(t, hs2, e(3), false), chunkOf(t, nil, e(4), false))...),
	}
	dir := t.TempDir()
	for name, data := range cases {
		path := dir + "/malformed.wal"
		if err := writeFileForTest(path, data); err != nil {
			t.Fatal(err)
		}
		mustFailStop(t, path, name)
	}
}

// Control: the shapes the encoder does produce still recover. A group whose first
// record is the hard state alone, then entries, is valid.
func TestRecoveryAcceptsWellFormedGroups(t *testing.T) {
	hs := &raft.HardState{Term: 1, VotedFor: 1}
	e := func(i raft.Index) []raft.Entry { return []raft.Entry{{Term: 1, Index: i, Data: []byte("d")}} }
	cases := map[string][]byte{
		"hard state alone, then entries": groupFile(t,
			chunkOf(t, hs, nil, true), chunkOf(t, nil, e(1), true), chunkOf(t, nil, e(2), false)),
		"hard state with first entry": groupFile(t,
			chunkOf(t, hs, e(1), true), chunkOf(t, nil, e(2), false)),
	}
	dir := t.TempDir()
	for name, data := range cases {
		path := dir + "/ok.wal"
		if err := writeFileForTest(path, data); err != nil {
			t.Fatal(err)
		}
		w, err := OpenPath(path)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got := w.Load()
		w.Close()
		if got.HardState != *hs || len(got.Log) == 0 {
			t.Fatalf("%s: recovered %+v", name, got)
		}
	}
}
