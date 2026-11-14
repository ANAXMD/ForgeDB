package wal

import (
	"errors"
	"io"
)

// MemFS is an in-memory FS with the one property that matters for crash
// testing: bytes written are NOT durable until Sync. Crash throws away
// everything since the last Sync, optionally keeping a prefix of the unsynced
// bytes (a torn write, such as a power cut in the middle of a multi-sector
// write). It is deterministic and used by tests and by the simulator.
type MemFS struct {
	cur     []byte // what a reader sees now (synced + unsynced)
	durable []byte // what survives a crash
	pos     int64
	closed  bool
}

// NewMemFS returns an empty MemFS.
func NewMemFS() *MemFS { return &MemFS{} }

var _ FS = (*MemFS)(nil)

func (m *MemFS) Read(p []byte) (int, error) {
	if m.closed {
		return 0, errors.New("memfs: closed")
	}
	if m.pos >= int64(len(m.cur)) {
		return 0, io.EOF
	}
	n := copy(p, m.cur[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *MemFS) Write(p []byte) (int, error) {
	if m.closed {
		return 0, errors.New("memfs: closed")
	}
	end := m.pos + int64(len(p))
	if end > int64(len(m.cur)) {
		m.cur = append(m.cur, make([]byte, end-int64(len(m.cur)))...)
	}
	copy(m.cur[m.pos:], p)
	m.pos = end
	return len(p), nil
}

func (m *MemFS) Seek(offset int64, whence int) (int64, error) {
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = m.pos
	case io.SeekEnd:
		base = int64(len(m.cur))
	default:
		return 0, errors.New("memfs: bad whence")
	}
	if base+offset < 0 {
		return 0, errors.New("memfs: negative position")
	}
	m.pos = base + offset
	return m.pos, nil
}

func (m *MemFS) Sync() error {
	if m.closed {
		return errors.New("memfs: closed")
	}
	m.durable = append([]byte(nil), m.cur...)
	return nil
}

func (m *MemFS) Truncate(size int64) error {
	if m.closed {
		return errors.New("memfs: closed")
	}
	if size < 0 {
		return errors.New("memfs: negative size")
	}
	if size <= int64(len(m.cur)) {
		m.cur = m.cur[:size]
	} else {
		m.cur = append(m.cur, make([]byte, size-int64(len(m.cur)))...)
	}
	return nil
}

func (m *MemFS) Close() error { m.closed = true; return nil }

// Unsynced returns how many bytes at the end of the file were written but not
// synced (only meaningful while the synced bytes are still a prefix of the file).
func (m *MemFS) Unsynced() int {
	if len(m.cur) >= len(m.durable) && string(m.cur[:len(m.durable)]) == string(m.durable) {
		return len(m.cur) - len(m.durable)
	}
	return 0
}

// Crash simulates a power cut. Everything unsynced is lost, except that the
// first keep bytes of the unsynced tail survive (a torn write); keep is clamped
// to the unsynced length. The file is usable again (a "rebooted" disk) and a
// fresh WAL can be opened on it.
func (m *MemFS) Crash(keep int) {
	if n := m.Unsynced(); keep > n {
		keep = n
	}
	if keep < 0 {
		keep = 0
	}
	survivor := append([]byte(nil), m.durable...)
	if keep > 0 {
		survivor = append(survivor, m.cur[len(m.durable):len(m.durable)+keep]...)
	}
	m.cur = survivor
	m.durable = append([]byte(nil), survivor...)
	m.pos = 0
	m.closed = false
}

// Bytes returns a copy of the current contents.
func (m *MemFS) Bytes() []byte { return append([]byte(nil), m.cur...) }
