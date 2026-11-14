package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	// recordHeaderSize is 13 bytes: length u32 | type u8 | headerCRC u32 | payloadCRC u32.
	recordHeaderSize = 13
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

func validKind(kind byte) bool {
	return kind == recordHardState || kind == recordEntries || kind == recordBatch
}

// encodeRecord creates a checksummed WAL record.
//
// Layout (big endian):
//   - 4 bytes payload length
//   - 1 byte record type
//   - 4 bytes header CRC32C over the length and type bytes
//   - 4 bytes payload CRC32C
//   - payload
//
// The header has its own checksum so that the length field is verified BEFORE it
// is trusted. With a single checksum over header and payload, a bit flip in the
// length of a record in the middle of the log is indistinguishable from an
// incomplete final write, and recovery would truncate every record after it.
func encodeRecord(kind byte, payload []byte) ([]byte, error) {
	if !validKind(kind) {
		return nil, errInvalidRecord
	}
	if len(payload) == 0 {
		return nil, errInvalidRecord
	}
	if len(payload) > maxRecordSize {
		return nil, errRecordTooBig
	}

	record := make([]byte, recordHeaderSize+len(payload))
	binary.BigEndian.PutUint32(record[0:4], uint32(len(payload)))
	record[4] = kind
	binary.BigEndian.PutUint32(record[5:9], crc32.Checksum(record[:5], castagnoliTable))
	binary.BigEndian.PutUint32(record[9:13], crc32.Checksum(payload, castagnoliTable))
	copy(record[recordHeaderSize:], payload)
	return record, nil
}

// parseHeader verifies the header checksum first and only then trusts the
// length and type. ok is false for any header that cannot be trusted: wrong
// checksum, unknown type, zero length, or a length above the maximum.
func parseHeader(header []byte) (kind byte, length uint32, payloadCRC uint32, ok bool) {
	if len(header) != recordHeaderSize {
		return 0, 0, 0, false
	}
	if crc32.Checksum(header[:5], castagnoliTable) != binary.BigEndian.Uint32(header[5:9]) {
		return 0, 0, 0, false
	}
	length = binary.BigEndian.Uint32(header[0:4])
	kind = header[4]
	if !validKind(kind) || length == 0 || length > maxRecordSize {
		return 0, 0, 0, false
	}
	return kind, length, binary.BigEndian.Uint32(header[9:13]), true
}

// decodeRecord verifies a whole record (header and payload) and returns its type.
func decodeRecord(header, payload []byte) (byte, error) {
	kind, length, payloadCRC, ok := parseHeader(header)
	if !ok {
		return 0, errInvalidRecord
	}
	if uint64(length) != uint64(len(payload)) {
		return 0, errInvalidRecord
	}
	if crc32.Checksum(payload, castagnoliTable) != payloadCRC {
		return 0, errInvalidRecord
	}
	return kind, nil
}
