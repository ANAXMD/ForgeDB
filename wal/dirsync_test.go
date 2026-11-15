package wal

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ANAXMD/ForgeDB/raft"
)

// recordDirSync replaces syncDir for one test. calls collects the directories it
// was asked to sync, and for each call it checks the file already exists on disk
// (the directory is synced after creation, never before).
func recordDirSync(t *testing.T, path string, result error) *[]string {
	t.Helper()
	var calls []string
	old := syncDir
	syncDir = func(dir string) error {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("directory synced before the file existed: %v", err)
		}
		calls = append(calls, dir)
		return result
	}
	t.Cleanup(func() { syncDir = old })
	return &calls
}

func saveOne(t *testing.T, w *WAL) {
	t.Helper()
	if err := w.Save(&raft.HardState{Term: 1, VotedFor: 1}, []raft.Entry{{Term: 1, Index: 1, Data: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
}

// Creating a WAL file syncs its parent directory, once, after the file exists.
func TestCreatingWALSyncsParentDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node1")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "raft.wal")
	calls := recordDirSync(t, path, nil)

	w, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(*calls) != 1 || (*calls)[0] != dir {
		t.Fatalf("directory syncs = %v, want exactly [%s]", *calls, dir)
	}
	saveOne(t, w)
}

// A bare file name has the current directory as its parent.
func TestCreatingWALWithBareNameSyncsCurrentDirectory(t *testing.T) {
	t.Chdir(t.TempDir())
	calls := recordDirSync(t, "raft.wal", nil)
	w, err := OpenPath("raft.wal")
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	if len(*calls) != 1 || (*calls)[0] != "." {
		t.Fatalf("directory syncs = %v, want [.]", *calls)
	}
}

// Opening an existing WAL file does not sync the directory, however many times.
func TestOpeningExistingWALDoesNotSyncDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	w, err := OpenPath(path) // real directory sync on creation
	if err != nil {
		t.Fatal(err)
	}
	saveOne(t, w)
	w.Close()

	old := syncDir
	syncDir = func(dir string) error {
		t.Errorf("directory %s synced when opening an existing WAL", dir)
		return errors.New("must not be called")
	}
	t.Cleanup(func() { syncDir = old })

	for i := 0; i < 3; i++ {
		w, err := OpenPath(path)
		if err != nil {
			t.Fatalf("reopen %d: %v", i, err)
		}
		if got := w.Load(); len(got.Log) != 1 || got.HardState.Term != 1 {
			t.Fatalf("reopen %d: recovered %+v", i, got)
		}
		w.Close()
	}
}

// A directory-sync failure is returned, no WAL is handed back, the half-created
// file is removed, and a retry creates and syncs again instead of finding a file
// whose directory entry was never made durable.
func TestDirectorySyncFailureIsReturnedAndRetrySyncsAgain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	boom := errors.New("injected directory sync failure")
	calls := recordDirSync(t, path, boom)

	w, err := OpenPath(path)
	if err == nil {
		w.Close()
		t.Fatal("OpenPath succeeded although the directory sync failed")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("error %v does not wrap the directory sync error", err)
	}
	if w != nil {
		t.Fatal("a WAL was returned alongside the error")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("file left behind after failed directory sync (stat: %v)", statErr)
	}

	// Retry with a working sync: the file is created again and synced again.
	*calls = nil
	syncDir = func(dir string) error { *calls = append(*calls, dir); return nil }
	w, err = OpenPath(path)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	defer w.Close()
	if len(*calls) != 1 {
		t.Fatalf("retry synced the directory %d times, want 1", len(*calls))
	}
}

// If the half-created file cannot be removed either, both failures are reported.
// The sync hook swaps the file for a non-empty directory, which os.Remove refuses
// to delete, whoever runs the test.
func TestDirectorySyncFailureReportsRemovalFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	boom := errors.New("injected directory sync failure")
	old := syncDir
	syncDir = func(string) error {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(path, "child"), 0700); err != nil {
			t.Fatal(err)
		}
		return boom
	}
	t.Cleanup(func() { syncDir = old })

	w, err := OpenPath(path)
	if err == nil {
		w.Close()
		t.Fatal("OpenPath succeeded although the directory sync failed")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("error %v does not wrap the directory sync error", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "remove") {
		t.Fatalf("error %q does not mention that the file could not be removed", msg)
	}
}

// No directory sync is attempted when the file cannot even be created.
func TestNoDirectorySyncWhenCreationFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-dir", "raft.wal")
	old := syncDir
	syncDir = func(string) error { t.Error("syncDir called although creation failed"); return nil }
	t.Cleanup(func() { syncDir = old })
	if w, err := OpenPath(path); err == nil {
		w.Close()
		t.Fatal("expected an error for a missing parent directory")
	}
}

// The real directory sync works where it is supported and fails loudly where it
// is not; it never reports success for a platform it cannot serve.
func TestPlatformDirectorySyncBehaviour(t *testing.T) {
	for _, goos := range []string{"windows", "plan9", "js", "wasip1"} {
		err := syncDirectoryOn(goos, t.TempDir())
		if !errors.Is(err, ErrDirSyncUnsupported) {
			t.Errorf("%s: err = %v, want ErrDirSyncUnsupported", goos, err)
		}
	}
	for _, goos := range []string{"linux", "darwin", "freebsd"} {
		if runtime.GOOS == "windows" {
			break
		}
		if err := syncDirectoryOn(goos, t.TempDir()); err != nil {
			t.Errorf("%s: real directory sync failed: %v", goos, err)
		}
	}
	if runtime.GOOS != "windows" {
		if err := syncDirectoryOn("linux", filepath.Join(t.TempDir(), "nope")); err == nil {
			t.Error("syncing a missing directory must fail")
		}
	}
}

// On a platform without directory sync, creating a WAL fails with
// ErrDirSyncUnsupported and leaves no file; opening an existing WAL still works.
func TestUnsupportedPlatformRefusesToCreateButOpensExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	old := syncDir
	syncDir = func(dir string) error { return syncDirectoryOn("windows", dir) }
	t.Cleanup(func() { syncDir = old })

	if w, err := OpenPath(path); err == nil {
		w.Close()
		t.Fatal("created a WAL although its directory entry cannot be made durable")
	} else if !errors.Is(err, ErrDirSyncUnsupported) {
		t.Fatalf("err = %v, want ErrDirSyncUnsupported", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file left behind: %v", err)
	}

	// An existing file (made elsewhere, e.g. earlier on a supported setup).
	syncDir = old
	w, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	saveOne(t, w)
	w.Close()
	syncDir = func(dir string) error { return syncDirectoryOn("windows", dir) }
	w, err = OpenPath(path)
	if err != nil {
		t.Fatalf("existing WAL must open on an unsupported platform: %v", err)
	}
	w.Close()
}

// openHandles counts this process's open descriptors that point at path, or -1
// where /proc is unavailable.
func openHandles(t *testing.T, path string) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	n := 0
	for _, e := range ents {
		if target, err := os.Readlink("/proc/self/fd/" + e.Name()); err == nil && target == path {
			n++
		}
	}
	return n
}

// The hole the cleanup failure leaves: directory sync failed AND the file could
// not be removed, so a zero-length regular file stays behind. A later open must
// not trust it. It syncs the directory, fails while that sync fails, and returns
// a WAL only once the sync succeeds.
func TestLeftoverEmptyFileIsNotTrustedUntilDirectorySyncSucceeds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	boom := errors.New("injected directory sync failure")
	oldSync, oldRemove := syncDir, removeFile
	t.Cleanup(func() { syncDir, removeFile = oldSync, oldRemove })

	// 1. Creation: directory sync fails and so does the removal.
	syncDir = func(string) error { return boom }
	removeFile = func(string) error { return errors.New("injected removal failure") }
	if w, err := OpenPath(path); err == nil {
		w.Close()
		t.Fatal("creation succeeded although the directory sync failed")
	} else if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the directory sync error", err)
	}
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("test setup: the file should remain: %v", err)
	}
	if !fi.Mode().IsRegular() || fi.Size() != 0 {
		t.Fatalf("test setup: want a zero-length regular file, got mode %v size %d", fi.Mode(), fi.Size())
	}

	// 2. A later start: the file exists, so the directory is synced again. While
	// that still fails, no WAL is returned and the file is not touched.
	var calls []string
	syncDir = func(dir string) error { calls = append(calls, dir); return boom }
	if w, err := OpenPath(path); err == nil {
		w.Close()
		t.Fatal("an empty leftover file was accepted although its directory sync failed")
	} else if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the directory sync error", err)
	}
	if len(calls) != 1 || calls[0] != filepath.Dir(path) {
		t.Fatalf("directory syncs = %v, want one sync of %s", calls, filepath.Dir(path))
	}
	if fi, err := os.Lstat(path); err != nil || fi.Size() != 0 {
		t.Fatalf("failed open must leave the file alone: %v size %v", err, fi)
	}
	if n := openHandles(t, path); n > 0 {
		t.Fatalf("failed open leaked %d open handle(s) on the file", n)
	}

	// 3. Once the directory sync succeeds the file is returned and usable.
	calls = nil
	syncDir = func(dir string) error { calls = append(calls, dir); return nil }
	w, err := OpenPath(path)
	if err != nil {
		t.Fatalf("open after a successful directory sync: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("directory syncs = %v, want exactly one", calls)
	}
	saveOne(t, w)
	w.Close()

	// 4. Now it holds data, so it opens without any directory sync.
	syncDir = func(dir string) error { t.Errorf("directory %s synced for a non-empty WAL", dir); return boom }
	w, err = OpenPath(path)
	if err != nil {
		t.Fatalf("reopen of a non-empty WAL: %v", err)
	}
	w.Close()
}

// On a platform without directory sync an existing EMPTY file cannot be vouched
// for either, so it is refused the same way and left in place.
func TestUnsupportedPlatformRefusesEmptyExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	old := syncDir
	syncDir = func(dir string) error { return syncDirectoryOn("windows", dir) }
	t.Cleanup(func() { syncDir = old })

	if w, err := OpenPath(path); err == nil {
		w.Close()
		t.Fatal("accepted an empty file whose directory entry cannot be made durable")
	} else if !errors.Is(err, ErrDirSyncUnsupported) {
		t.Fatalf("err = %v, want ErrDirSyncUnsupported", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file must be left in place: %v", err)
	}
}
