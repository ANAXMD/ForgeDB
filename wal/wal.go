package wal

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/ANAXMD/ForgeDB/raft"
)

type pendingOp struct {
	hs   *raft.HardState
	ents []raft.Entry
}

// WAL stores Raft hard state and log entries in an append-only file.
type WAL struct {
	fs      FS
	state   raft.Persisted
	pending [][]byte
	ops     []pendingOp
	failed  error
}

// Open recovers an existing WAL or initializes an empty one.
func Open(fs FS) (*WAL, error) {
	w := &WAL{fs: fs}

	if err := w.recover(); err != nil {
		return nil, err
	}
	return w, nil
}

// OpenPath opens a WAL at path.
func OpenPath(path string) (*WAL, error) {
	fs, err := OpenFile(path)
	if err != nil {
		return nil, err
	}

	w, err := Open(fs)
	if err != nil {
		_ = fs.Close()
		return nil, err
	}
	return w, nil
}

// Save queues hard state and/or a log suffix replacement.
// Queued data is not durable until Sync succeeds.
func (w *WAL) Save(hs *raft.HardState, entries []raft.Entry) error {
	if w.failed != nil {
		return fmt.Errorf("wal: unusable after previous I/O failure: %w", w.failed)
	}

	if hs == nil && len(entries) == 0 {
		return nil
	}

	payload, err := encodeBatch(hs, entries)
	if err != nil {
		return err
	}
	record, err := encodeRecord(recordBatch, payload)
	if err != nil {
		return err
	}

	var op pendingOp
	if hs != nil {
		copyHS := *hs
		op.hs = &copyHS
	}
	op.ents = cloneEntries(entries)

	projected := clonePersisted(w.state)
	for _, old := range w.ops {
		if err := applyOp(&projected, old); err != nil {
			return err
		}
	}
	if err := applyOp(&projected, op); err != nil {
		return err
	}

	w.pending = append(w.pending, record)
	w.ops = append(w.ops, op)
	return nil
}

// Sync writes all queued records and makes them durable.
func (w *WAL) Sync() error {
	if w.failed != nil {
		return fmt.Errorf("wal: unusable after previous I/O failure: %w", w.failed)
	}
	if len(w.pending) == 0 {
		return nil
	}

	if _, err := w.fs.Seek(0, io.SeekEnd); err != nil {
		w.failed = err
		return err
	}

	for _, record := range w.pending {
		if err := writeFull(w.fs, record); err != nil {
			w.failed = err
			return err
		}
	}
	if err := w.fs.Sync(); err != nil {
		w.failed = err
		return err
	}

	for _, op := range w.ops {
		if err := applyOp(&w.state, op); err != nil {
			w.failed = err
			return err
		}
	}

	w.pending = nil
	w.ops = nil
	return nil
}

// Load returns a copy of the last successfully synced state.
func (w *WAL) Load() raft.Persisted {
	return clonePersisted(w.state)
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	return w.fs.Close()
}

// recover rebuilds the state from the file. Every condition has one outcome:
//
//	clean EOF at a record boundary                                 -> done
//	fewer than a full header remain                                -> torn tail: truncate
//	valid header, payload runs past EOF                            -> torn tail: truncate
//	valid header, payload checksum bad, record ends exactly at EOF -> torn tail: truncate
//	bad header, every remaining byte is zero                       -> zero-filled tail: truncate
//	bad header (checksum, type, length), non-zero bytes remain     -> corruption: refuse to start
//	valid header, payload checksum bad, more bytes follow          -> corruption: refuse to start
//	checksums fine but the content is invalid (gap, bad encoding)  -> corruption: refuse to start
//
// "Refuse to start" never modifies the file. A record is never skipped: once a
// record's extent or content cannot be trusted, nothing after it can be.
func (w *WAL) recover() error {
	if _, err := w.fs.Seek(0, io.SeekStart); err != nil {
		return err
	}
	data, err := io.ReadAll(w.fs)
	if err != nil {
		return err
	}

	off := 0
	for off < len(data) {
		rest := data[off:]
		if len(rest) < recordHeaderSize {
			return w.truncateTail(int64(off)) // partial header
		}
		kind, length, payloadCRC, ok := parseHeader(rest[:recordHeaderSize])
		if !ok {
			if allZero(rest) {
				return w.truncateTail(int64(off)) // zero-filled tail
			}
			return fmt.Errorf("wal: corruption at offset %d: %w: bad record header", off, errInvalidRecord)
		}
		end := off + recordHeaderSize + int(length)
		if end > len(data) {
			return w.truncateTail(int64(off)) // header is trustworthy, payload never finished
		}
		payload := data[off+recordHeaderSize : end]
		if crc32.Checksum(payload, castagnoliTable) != payloadCRC {
			if end == len(data) {
				return w.truncateTail(int64(off)) // damaged final record
			}
			return fmt.Errorf("wal: corruption at offset %d: %w: payload checksum mismatch before the end of the log", off, errInvalidRecord)
		}

		switch kind {
		case recordHardState:
			hs, err := decodeHardState(payload)
			if err != nil {
				return fmt.Errorf("wal: corruption at offset %d: %w", off, err)
			}
			w.state.HardState = hs

		case recordEntries:
			entries, err := decodeEntries(payload)
			if err != nil {
				return fmt.Errorf("wal: corruption at offset %d: %w", off, err)
			}
			if err := applyOp(&w.state, pendingOp{ents: entries}); err != nil {
				return fmt.Errorf("wal: invalid log at offset %d: %w", off, err)
			}

		case recordBatch:
			op, err := decodeBatch(payload)
			if err != nil {
				return fmt.Errorf("wal: corruption at offset %d: %w", off, err)
			}
			if err := applyOp(&w.state, op); err != nil {
				return fmt.Errorf("wal: invalid batch at offset %d: %w", off, err)
			}
		}
		off = end
	}

	_, err = w.fs.Seek(0, io.SeekEnd)
	return err
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func (w *WAL) truncateTail(offset int64) error {
	if err := w.fs.Truncate(offset); err != nil {
		return err
	}
	if err := w.fs.Sync(); err != nil {
		return err
	}
	_, err := w.fs.Seek(0, io.SeekEnd)
	return err
}

func applyOp(state *raft.Persisted, op pendingOp) error {
	if op.hs != nil {
		state.HardState = *op.hs
	}
	if len(op.ents) == 0 {
		return nil
	}

	entries := op.ents
	first := entries[0].Index
	if first == 0 || uint64(first) > uint64(len(state.Log))+1 {
		return fmt.Errorf("%w: log suffix starts at invalid index %d", errInvalidRecord, first)
	}

	for i, entry := range entries {
		expected := first + raft.Index(i)
		if entry.Index != expected {
			return fmt.Errorf("%w: expected entry index %d, got %d", errInvalidRecord, expected, entry.Index)
		}
	}

	state.Log = append(state.Log[:int(first)-1], cloneEntries(entries)...)
	return nil
}

func cloneEntries(entries []raft.Entry) []raft.Entry {
	out := make([]raft.Entry, len(entries))
	for i, entry := range entries {
		out[i] = entry
		out[i].Data = append([]byte(nil), entry.Data...)
	}
	return out
}

func clonePersisted(state raft.Persisted) raft.Persisted {
	return raft.Persisted{
		HardState: state.HardState,
		Log:       cloneEntries(state.Log),
	}
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("wal: zero-length write")
		}
		data = data[n:]
	}
	return nil
}
