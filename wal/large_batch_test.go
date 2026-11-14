package wal

// A single Raft Ready can carry more entry bytes than one WAL record may hold
// (maxRecordSize). Save must still persist it, and a Save must stay all-or-nothing:
// after any crash the recovered log contains either the whole Save or none of it,
// because a half-persisted Ready is a state the node was never in.

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ANAXMD/ForgeDB/raft"
)

func bigEntries(first raft.Index, n, size int, fill byte) []raft.Entry {
	var es []raft.Entry
	for i := 0; i < n; i++ {
		es = append(es, raft.Entry{Term: 2, Index: first + raft.Index(i), Data: bytes.Repeat([]byte{fill + byte(i)}, size)})
	}
	return es
}

// Three 6 MiB entries are one 18 MiB Ready; each entry is individually legal.
// Before splitting, Save failed with "record exceeds maximum size".
func TestLargeReadyIsSavedAndRecovered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	hs := raft.HardState{Term: 2, VotedFor: 1}
	es := bigEntries(1, 3, 6<<20, 'a')
	if err := w.Save(&hs, es); err != nil {
		t.Fatalf("Save of an 18 MiB Ready failed: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenPath(path)
	if err != nil {
		t.Fatalf("recovery of an 18 MiB Ready failed: %v", err)
	}
	defer r.Close()
	got := r.Load()
	if got.HardState != hs || len(got.Log) != 3 {
		t.Fatalf("recovered %+v with %d entries", got.HardState, len(got.Log))
	}
	for i, e := range got.Log {
		if e.Index != es[i].Index || e.Term != es[i].Term || !bytes.Equal(e.Data, es[i].Data) {
			t.Fatalf("entry %d differs after recovery", i)
		}
	}
}

// An entry that cannot fit in any record must be refused explicitly, leaving
// the WAL usable and its state unchanged.
func TestEntryTooLargeForARecordIsRejectedAndWALStaysUsable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Save(nil, []raft.Entry{{Term: 1, Index: 1, Data: []byte("ok")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	err = w.Save(nil, []raft.Entry{{Term: 1, Index: 2, Data: make([]byte, maxRecordSize+1)}})
	if !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("Save of an oversized entry: got %v, want ErrEntryTooLarge", err)
	}
	// Nothing was queued and the WAL is not poisoned.
	if err := w.Save(nil, []raft.Entry{{Term: 1, Index: 2, Data: []byte("next")}}); err != nil {
		t.Fatalf("WAL unusable after a rejected Save: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := w.Load(); len(got.Log) != 2 || string(got.Log[1].Data) != "next" {
		t.Fatalf("state after rejected Save: %+v", got)
	}
	if fi, _ := os.Stat(path); fi.Size() > 200 {
		t.Fatalf("rejected Save wrote %d bytes", fi.Size())
	}
}
