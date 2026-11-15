package wal

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// FS is the small filesystem surface required by the WAL.
// Keeping it behind an interface lets tests simulate crashes and I/O failures.
type FS interface {
	io.Reader
	io.Writer
	io.Seeker
	Sync() error
	Truncate(size int64) error
	Close() error
}

// ErrDirSyncUnsupported is returned when a new WAL file is created on a platform
// where this package cannot make the file's directory entry durable. It is an
// error rather than a silent downgrade: the file's contents could be synced but
// its name could be lost in a crash. Opening an existing NON-EMPTY WAL file is
// unaffected; an empty one needs the same directory sync and fails the same way.
var ErrDirSyncUnsupported = errors.New("wal: syncing a directory is not supported on this platform")

// syncDir makes the directory entries of dir durable. It is a variable so tests
// can observe and fail it.
var syncDir = syncDirectory

// removeFile deletes a half-created WAL file; a variable so tests can fail it.
var removeFile = os.Remove

// openMu serializes OpenFile within the process. Creation is a sequence (create,
// sync the directory, and on failure remove the file) and the file is visible to
// other callers throughout it. Without the lock a second caller could open the
// half-created file, sync the directory successfully, and return a handle to a
// file the first caller then removes. It does not coordinate separate processes:
// two processes must not open the same WAL path at once in any case.
var openMu sync.Mutex

// OpenFile opens a WAL file, creating it if it does not exist.
//
// When the file is created, its parent directory is fsynced before OpenFile
// returns, so a crash after a successful return cannot lose the directory entry
// of a file whose contents were later synced. If the directory sync fails, the
// error is returned, the new file is removed so that a retry creates and syncs
// it again, and no FS is returned.
//
// Precondition: the parent directory must itself already exist durably. Syncing
// it persists the WAL's entry INSIDE that directory; it does not persist the
// directory's own entry in ITS parent. If the caller has just created the WAL
// directory (or any ancestor), the caller must sync that directory's parent
// (and so on up to a directory known to be durable), or a crash can still lose
// the whole directory and the WAL with it. OpenFile does not create or sync
// ancestors.
//
// An existing non-empty file is opened without touching its directory: a WAL
// only holds data once a Sync has succeeded, and that sync happens after the
// creating OpenFile succeeded. An existing zero-length file is different. It
// may be the leftover of a creation whose directory sync failed and whose
// cleanup also failed, so its entry cannot be assumed durable: the parent
// directory is synced before the file is returned, and if that fails the file
// is closed and the error returned.
func OpenFile(path string) (FS, error) {
	openMu.Lock()
	defer openMu.Unlock()

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, fs.ErrExist) {
		return openExisting(path)
	}
	if err != nil {
		return nil, err
	}

	if err := syncDir(filepath.Dir(path)); err != nil {
		closeErr := f.Close()
		rmErr := removeFile(path)
		err = fmt.Errorf("wal: sync directory after creating %s: %w", path, err)
		if rmErr != nil {
			err = errors.Join(err, fmt.Errorf("wal: remove %s after failed directory sync: %w", path, rmErr))
		}
		return nil, errors.Join(err, closeErr)
	}
	return f, nil
}

// openExisting opens an existing WAL file, syncing its directory first if the
// file is empty (see OpenFile).
func openExisting(path string) (FS, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if fi.Size() == 0 {
		if err := syncDir(filepath.Dir(path)); err != nil {
			closeErr := f.Close()
			return nil, errors.Join(fmt.Errorf("wal: sync directory of empty existing %s: %w", path, err), closeErr)
		}
	}
	return f, nil
}

// syncDirectory fsyncs the directory dir.
func syncDirectory(dir string) error { return syncDirectoryOn(runtime.GOOS, dir) }

// syncDirectoryOn is syncDirectory for a named platform, so the behaviour on
// platforms other than the one running the tests can be tested.
//
// Linux and the BSDs, including macOS, make directory entries durable with
// fsync on a directory handle. Windows has no equivalent that Go exposes (a
// directory handle cannot be flushed), and plan9, js and wasip1 have no usable
// directory sync either; they get ErrDirSyncUnsupported.
func syncDirectoryOn(goos, dir string) error {
	switch goos {
	case "windows", "plan9", "js", "wasip1":
		return fmt.Errorf("%w (%s)", ErrDirSyncUnsupported, goos)
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
