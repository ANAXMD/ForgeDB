package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ANAXMD/ForgeDB/raft"
)

func TestSaveIsNotVisibleUntilSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	hs := raft.HardState{Term: 4, VotedFor: 2}
	entries := []raft.Entry{{Term: 4, Index: 1, Data: []byte("hello")}}

	if err := w.Save(&hs, entries); err != nil {
		t.Fatal(err)
	}
	if got := w.Load(); got.HardState != (raft.HardState{}) || len(got.Log) != 0 {
		t.Fatalf("unsynced state became visible: %+v", got)
	}

	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	got := w.Load()
	if got.HardState != hs || len(got.Log) != 1 || string(got.Log[0].Data) != "hello" {
		t.Fatalf("unexpected synced state: %+v", got)
	}
}

func TestReopenRecoversSyncedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}

	hs := raft.HardState{Term: 8, VotedFor: 1}
	entries := []raft.Entry{
		{Term: 7, Index: 1, Data: []byte("one")},
		{Term: 8, Index: 2, Data: []byte("two")},
	}
	if err := w.Save(&hs, entries); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	got := reopened.Load()
	if got.HardState != hs || len(got.Log) != 2 {
		t.Fatalf("unexpected recovered state: %+v", got)
	}
	for i, want := range entries {
		if got.Log[i].Index != want.Index || got.Log[i].Term != want.Term ||
			string(got.Log[i].Data) != string(want.Data) {
			t.Fatalf("entry %d: got %+v, want %+v", i, got.Log[i], want)
		}
	}
}

func TestRecoveryTruncatesTornFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Save(nil, []raft.Entry{{Term: 1, Index: 1, Data: []byte("safe")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{recordEntries, 0, 0, 0, 20, 1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	got := reopened.Load()
	if len(got.Log) != 1 || string(got.Log[0].Data) != "safe" {
		t.Fatalf("torn tail damaged recovered state: %+v", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(recordHeaderSize+1+4+8+8+4+4) {
		t.Fatalf("unexpected WAL size after truncation: %d", info.Size())
	}
}

func TestSaveReplacesLogSuffix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	first := []raft.Entry{
		{Term: 1, Index: 1, Data: []byte("one")},
		{Term: 1, Index: 2, Data: []byte("old-two")},
		{Term: 1, Index: 3, Data: []byte("old-three")},
	}
	if err := w.Save(nil, first); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	replacement := []raft.Entry{
		{Term: 2, Index: 2, Data: []byte("new-two")},
	}
	if err := w.Save(nil, replacement); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	got := w.Load()
	if len(got.Log) != 2 {
		t.Fatalf("got %d entries, want 2", len(got.Log))
	}
	if string(got.Log[0].Data) != "one" ||
		string(got.Log[1].Data) != "new-two" {
		t.Fatalf("unexpected replacement log: %+v", got.Log)
	}
}

func TestRecoveryRejectsCorruptionInMiddle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}

	entries := []raft.Entry{{Term: 1, Index: 1, Data: []byte("one")}}
	if err := w.Save(nil, entries); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Save(nil, []raft.Entry{
		{Term: 1, Index: 2, Data: []byte("two")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the first record's payload while leaving the file complete.
	if _, err := f.WriteAt([]byte{0xff}, recordHeaderSize+4+8+8+4); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPath(path)
	if err == nil {
		reopened.Close()
		t.Fatal("expected corruption error, got nil")
	}
}

type faultFS struct {
	FS
	writeErr error
	syncErr  error
}

func (f *faultFS) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.FS.Write(p)
}

func (f *faultFS) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.FS.Sync()
}

func TestWriteFailurePoisonsWALWithoutPublishingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	file, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}

	injected := &faultFS{
		FS:       file,
		writeErr: errors.New("injected write failure"),
	}
	w, err := Open(injected)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	hs := raft.HardState{Term: 5, VotedFor: 2}
	if err := w.Save(&hs, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err == nil {
		t.Fatal("expected injected write failure")
	}
	if got := w.Load(); got.HardState != (raft.HardState{}) {
		t.Fatalf("failed write published state: %+v", got)
	}
	if err := w.Save(&hs, nil); err == nil {
		t.Fatal("expected WAL to reject writes after failure")
	}
}

func TestSyncFailurePoisonsWALWithoutPublishingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	file, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}

	injected := &faultFS{
		FS:      file,
		syncErr: errors.New("injected sync failure"),
	}
	w, err := Open(injected)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	hs := raft.HardState{Term: 6, VotedFor: 3}
	if err := w.Save(&hs, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err == nil {
		t.Fatal("expected injected sync failure")
	}
	if got := w.Load(); got.HardState != (raft.HardState{}) {
		t.Fatalf("failed sync published state: %+v", got)
	}
	if err := w.Save(&hs, nil); err == nil {
		t.Fatal("expected WAL to reject writes after failure")
	}
}

func TestRecoveryDoesNotApplyPartialSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}

	hs := raft.HardState{Term: 9, VotedFor: 2}
	entries := []raft.Entry{{Term: 9, Index: 1, Data: []byte("command")}}

	if err := w.Save(&hs, entries); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash that leaves the combined save batch truncated.
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(recordHeaderSize + 12)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()

	got := recovered.Load()
	if got.HardState != (raft.HardState{}) || len(got.Log) != 0 {
		t.Fatalf("recovered partial save: %+v", got)
	}
}
