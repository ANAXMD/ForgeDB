package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ANAXMD/ForgeDB/raft"
)

func encodeHardState(hs raft.HardState) ([]byte, error) {
	var b bytes.Buffer
	if err := binary.Write(&b, binary.BigEndian, uint64(hs.Term)); err != nil {
		return nil, err
	}
	if err := binary.Write(&b, binary.BigEndian, uint32(hs.VotedFor)); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func decodeHardState(data []byte) (raft.HardState, error) {
	if len(data) != 12 {
		return raft.HardState{}, fmt.Errorf("%w: invalid hard state length", errInvalidRecord)
	}
	return raft.HardState{
		Term:     raft.Term(binary.BigEndian.Uint64(data[:8])),
		VotedFor: raft.NodeID(binary.BigEndian.Uint32(data[8:12])),
	}, nil
}

func encodeEntries(entries []raft.Entry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: empty entry batch", errInvalidRecord)
	}
	if uint64(len(entries)) > uint64(^uint32(0)) {
		return nil, errRecordTooBig
	}

	var b bytes.Buffer
	if err := binary.Write(&b, binary.BigEndian, uint32(len(entries))); err != nil {
		return nil, err
	}

	for _, e := range entries {
		if err := binary.Write(&b, binary.BigEndian, uint64(e.Term)); err != nil {
			return nil, err
		}
		if err := binary.Write(&b, binary.BigEndian, uint64(e.Index)); err != nil {
			return nil, err
		}
		if uint64(len(e.Data)) > uint64(maxRecordSize) {
			return nil, errRecordTooBig
		}
		if err := binary.Write(&b, binary.BigEndian, uint32(len(e.Data))); err != nil {
			return nil, err
		}
		if _, err := b.Write(e.Data); err != nil {
			return nil, err
		}
		if b.Len() > maxRecordSize {
			return nil, errRecordTooBig
		}
	}
	return b.Bytes(), nil
}

func decodeEntries(data []byte) ([]raft.Entry, error) {
	r := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("%w: missing entry count", errInvalidRecord)
	}
	if count == 0 {
		return nil, fmt.Errorf("%w: empty entry batch", errInvalidRecord)
	}

	// Every entry requires at least 20 bytes, excluding its data.
	if uint64(count) > uint64(r.Len())/20 {
		return nil, fmt.Errorf("%w: impossible entry count", errInvalidRecord)
	}

	entries := make([]raft.Entry, 0, count)
	for i := uint32(0); i < count; i++ {
		var term, index uint64
		var size uint32

		if err := binary.Read(r, binary.BigEndian, &term); err != nil {
			return nil, errInvalidRecord
		}
		if err := binary.Read(r, binary.BigEndian, &index); err != nil {
			return nil, errInvalidRecord
		}
		if err := binary.Read(r, binary.BigEndian, &size); err != nil {
			return nil, errInvalidRecord
		}
		if uint64(size) > uint64(r.Len()) {
			return nil, fmt.Errorf("%w: truncated entry data", errInvalidRecord)
		}

		command := make([]byte, size)
		if _, err := io.ReadFull(r, command); err != nil {
			return nil, errInvalidRecord
		}
		entries = append(entries, raft.Entry{
			Term:  raft.Term(term),
			Index: raft.Index(index),
			Data:  command,
		})
	}

	if r.Len() != 0 {
		return nil, fmt.Errorf("%w: trailing entry data", errInvalidRecord)
	}
	return entries, nil
}

// encodeBatch stores the hard state and entries from one Save call together.
func encodeBatch(hs *raft.HardState, entries []raft.Entry) ([]byte, error) {
	if hs == nil && len(entries) == 0 {
		return nil, fmt.Errorf("%w: empty save batch", errInvalidRecord)
	}

	var b bytes.Buffer
	var flags byte
	if hs != nil {
		flags |= 1
	}
	if len(entries) > 0 {
		flags |= 2
	}
	if err := b.WriteByte(flags); err != nil {
		return nil, err
	}

	if hs != nil {
		data, err := encodeHardState(*hs)
		if err != nil {
			return nil, err
		}
		_, _ = b.Write(data)
	}
	if len(entries) > 0 {
		data, err := encodeEntries(entries)
		if err != nil {
			return nil, err
		}
		if b.Len()+len(data) > maxRecordSize {
			return nil, errRecordTooBig
		}
		_, _ = b.Write(data)
	}
	return b.Bytes(), nil
}

func decodeBatch(data []byte) (pendingOp, error) {
	if len(data) < 1 {
		return pendingOp{}, errInvalidRecord
	}

	flags := data[0]
	if flags == 0 || flags&^byte(3) != 0 {
		return pendingOp{}, errInvalidRecord
	}
	data = data[1:]

	var op pendingOp
	if flags&1 != 0 {
		if len(data) < 12 {
			return pendingOp{}, errInvalidRecord
		}
		hs, err := decodeHardState(data[:12])
		if err != nil {
			return pendingOp{}, err
		}
		op.hs = &hs
		data = data[12:]
	}
	if flags&2 != 0 {
		entries, err := decodeEntries(data)
		if err != nil {
			return pendingOp{}, err
		}
		op.ents = entries
	} else if len(data) != 0 {
		return pendingOp{}, errInvalidRecord
	}
	return op, nil
}
