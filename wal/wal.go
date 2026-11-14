package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
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

func (w *WAL) recover() error {
	if _, err := w.fs.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var offset int64
	for {
		start := offset
		header := make([]byte, recordHeaderSize)
		n, err := io.ReadFull(w.fs, header)

		if err == io.EOF && n == 0 {
			break
		}
		if err == io.ErrUnexpectedEOF || (err == io.EOF && n > 0) {
			return w.truncateTail(start)
		}
		if err != nil {
			return err
		}
		offset += recordHeaderSize

		length := binary.BigEndian.Uint32(header[1:5])
		if length > maxRecordSize {
			return fmt.Errorf("%w: record length %d exceeds limit", errInvalidRecord, length)
		}

		payload := make([]byte, int(length))
		n, err = io.ReadFull(w.fs, payload)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return w.truncateTail(start)
		}
		if err != nil {
			return err
		}
		offset += int64(n)

		kind, err := decodeRecord(header, payload)
		if err != nil {
			return fmt.Errorf("wal: corruption at offset %d: %w", start, err)
		}

		switch kind {
		case recordHardState:
			hs, err := decodeHardState(payload)
			if err != nil {
				return fmt.Errorf("wal: corruption at offset %d: %w", start, err)
			}
			w.state.HardState = hs

		case recordEntries:
			entries, err := decodeEntries(payload)
			if err != nil {
				return fmt.Errorf("wal: corruption at offset %d: %w", start, err)
			}
			if err := applyOp(&w.state, pendingOp{ents: entries}); err != nil {
				return fmt.Errorf("wal: invalid log at offset %d: %w", start, err)
			}

		case recordBatch:
			op, err := decodeBatch(payload)
			if err != nil {
				return fmt.Errorf("wal: corruption at offset %d: %w", start, err)
			}
			if err := applyOp(&w.state, op); err != nil {
				return fmt.Errorf("wal: invalid batch at offset %d: %w", start, err)
			}
		}
	}

	_, err := w.fs.Seek(0, io.SeekEnd)
	return err
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
