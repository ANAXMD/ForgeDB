package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	recordHeaderSize = 9
	maxRecordSize    = 16 << 20 // 16 MiB

	recordHardState byte = 1
	recordEntries   byte = 2
	recordBatch     byte = 3
)

var (
	errInvalidRecord = errors.New("wal: invalid record")
	errRecordTooBig  = errors.New("wal: record exceeds maximum size")
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// encodeRecord creates a checksummed WAL record.
//
// Layout:
//   - 1 byte record type
//   - 4 bytes payload length (big endian)
//   - 4 bytes CRC32 checksum
//   - payload
func encodeRecord(kind byte, payload []byte) ([]byte, error) {
	if kind != recordHardState && kind != recordEntries && kind != recordBatch {
		return nil, errInvalidRecord
	}
	if len(payload) > maxRecordSize {
		return nil, errRecordTooBig
	}

	record := make([]byte, recordHeaderSize+len(payload))
	record[0] = kind
	binary.BigEndian.PutUint32(record[1:5], uint32(len(payload)))
	copy(record[recordHeaderSize:], payload)

	checksum := crc32.Checksum(record[:5], castagnoliTable)
	checksum = crc32.Update(checksum, castagnoliTable, payload)
	binary.BigEndian.PutUint32(record[5:9], checksum)

	return record, nil
}

func decodeRecord(header, payload []byte) (byte, error) {
	if len(header) != recordHeaderSize {
		return 0, errInvalidRecord
	}

	kind := header[0]
	if kind != recordHardState && kind != recordEntries && kind != recordBatch {
		return 0, errInvalidRecord
	}

	length := binary.BigEndian.Uint32(header[1:5])
	if uint64(length) != uint64(len(payload)) {
		return 0, errInvalidRecord
	}

	checksum := crc32.Checksum(header[:5], castagnoliTable)
	checksum = crc32.Update(checksum, castagnoliTable, payload)
	if checksum != binary.BigEndian.Uint32(header[5:9]) {
		return 0, errInvalidRecord
	}

	return kind, nil
}
