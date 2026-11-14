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

const (
	flagHardState byte = 1
	flagEntries   byte = 2
	// flagMore marks a record that is not the last of its Save: the following
	// record(s) belong to the same Save and the group is applied only when the
	// last one (without the flag) has been read.
	flagMore byte = 4

	entryHeaderSize = 20 // term u64, index u64, data length u32
)

// chunkLimit is the largest payload Save puts in one record. It is a variable
// only so that tests can force splitting without allocating 16 MiB.
var chunkLimit = maxRecordSize

// encodeBatch stores the hard state and entries from one Save call in ONE record
// payload.
func encodeBatch(hs *raft.HardState, entries []raft.Entry) ([]byte, error) {
	return encodeChunk(hs, entries, false)
}

func encodeChunk(hs *raft.HardState, entries []raft.Entry, more bool) ([]byte, error) {
	if hs == nil && len(entries) == 0 {
		return nil, fmt.Errorf("%w: empty save batch", errInvalidRecord)
	}

	var b bytes.Buffer
	var flags byte
	if hs != nil {
		flags |= flagHardState
	}
	if len(entries) > 0 {
		flags |= flagEntries
	}
	if more {
		flags |= flagMore
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

// encodeBatchChunks encodes one Save as one or more record payloads. The hard
// state goes in the first chunk, entries are split only between entries, in
// index order, and every chunk except the last carries flagMore. Recovery treats
// the chunks as one unit, so a Save stays all-or-nothing whatever its size.
//
// An entry is accepted whenever it fits in a record of its own (flags byte, entry
// count and entry header included); the hard state never makes an entry too large:
// if the first entry cannot share a chunk with the hard state, the hard state goes
// in a chunk by itself, ahead of the entries.
func encodeBatchChunks(hs *raft.HardState, entries []raft.Entry) ([][]byte, error) {
	if hs == nil && len(entries) == 0 {
		return nil, fmt.Errorf("%w: empty save batch", errInvalidRecord)
	}
	limit := chunkLimit

	const (
		chunkBase    = 1 + 4 // flags byte, entry count
		hardStateLen = 12    // term u64, voted-for u32
		onlyEntry    = chunkBase + entryHeaderSize
	)
	for _, e := range entries {
		if onlyEntry+len(e.Data) > limit {
			return nil, fmt.Errorf("%w: entry %d is %d bytes, at most %d fit in one record", ErrEntryTooLarge, e.Index, len(e.Data), limit-onlyEntry)
		}
	}

	if len(entries) == 0 {
		chunk, err := encodeChunk(hs, nil, false)
		return [][]byte{chunk}, err
	}

	var chunks [][]byte
	pendingHS := hs
	if hs != nil && onlyEntry+hardStateLen+len(entries[0].Data) > limit {
		// The first entry fits alone but not next to the hard state.
		chunk, err := encodeChunk(hs, nil, true)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
		pendingHS = nil
	}

	for i := 0; i < len(entries); {
		size := chunkBase
		h := pendingHS
		if h != nil {
			size += hardStateLen
			pendingHS = nil
		}
		j := i
		for j < len(entries) {
			c := entryHeaderSize + len(entries[j].Data)
			if j > i && size+c > limit {
				break
			}
			size += c
			j++
		}
		chunk, err := encodeChunk(h, entries[i:j], j < len(entries))
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
		i = j
	}
	return chunks, nil
}

// decodeBatch decodes one batch record. more reports flagMore: the record is
// not the last of its Save.
func decodeBatch(data []byte) (op pendingOp, more bool, err error) {
	if len(data) < 1 {
		return pendingOp{}, false, errInvalidRecord
	}

	flags := data[0]
	if flags&^(flagHardState|flagEntries|flagMore) != 0 || flags&(flagHardState|flagEntries) == 0 {
		return pendingOp{}, false, errInvalidRecord
	}
	more = flags&flagMore != 0
	data = data[1:]

	if flags&1 != 0 {
		if len(data) < 12 {
			return pendingOp{}, false, errInvalidRecord
		}
		hs, err := decodeHardState(data[:12])
		if err != nil {
			return pendingOp{}, false, err
		}
		op.hs = &hs
		data = data[12:]
	}
	if flags&2 != 0 {
		entries, err := decodeEntries(data)
		if err != nil {
			return pendingOp{}, false, err
		}
		op.ents = entries
	} else if len(data) != 0 {
		return pendingOp{}, false, errInvalidRecord
	}
	return op, more, nil
}
