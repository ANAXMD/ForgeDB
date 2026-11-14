package wal

import (
	"io"
	"os"
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

// OpenFile opens or creates a WAL file.
func OpenFile(path string) (FS, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
}
