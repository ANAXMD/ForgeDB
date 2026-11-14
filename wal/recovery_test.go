package wal

// Recovery-rule tests for the WAL (see the recovery table in docs/ or the SPEC):
//
//	torn tail (partial header, valid header with payload past EOF, bad payload
//	checksum on the final record, zero-filled tail)  -> truncate and continue;
//	anything that makes the extent or content of a record in the MIDDLE of the log
//	untrustworthy (bad header checksum, corrupted length, bad payload checksum
//	followed by more bytes)                          -> refuse to start, and leave
//	the file byte-for-byte unchanged so no valid record is ever discarded.
//
// The offsets are measured from the real file size after each synced record, so
// the tests do not depend on the record layout.

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/ANAXMD/ForgeDB/raft"
)

const logRecords = 5

// buildLog writes logRecords synced records (each Save = hard state + two entries)
// and returns the path and the start offset of each record, plus the file size.
func buildLog(t *testing.T) (path string, starts []int64, size int64) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "node.wal")
	w, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < logRecords; i++ {
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		starts = append(starts, fi.Size())
		hs, es := recordContents(i)
		if err := w.Save(&hs, es); err != nil {
			t.Fatal(err)
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, starts, fi.Size()
}

func recordContents(i int) (raft.HardState, []raft.Entry) {
	first := raft.Index(2*i + 1)
	return raft.HardState{Term: 1, VotedFor: 1}, []raft.Entry{
		{Term: 1, Index: first, Data: []byte{byte('a' + i), 'x'}},
		{Term: 1, Index: first + 1, Data: []byte{byte('a' + i), 'y'}},
	}
}

// wantAfter returns the state the log must recover to after its first k records.
func wantAfter(k int) raft.Persisted {
	p := raft.Persisted{Log: []raft.Entry{}}
	for i := 0; i < k; i++ {
		hs, es := recordContents(i)
		p.HardState = hs
		p.Log = append(p.Log, es...)
	}
	return p
}

func samePersisted(a, b raft.Persisted) bool {
	if a.HardState != b.HardState || len(a.Log) != len(b.Log) {
		return false
	}
	for i := range a.Log {
		x, y := a.Log[i], b.Log[i]
		if x.Term != y.Term || x.Index != y.Index || !bytes.Equal(x.Data, y.Data) {
			return false
		}
	}
	return true
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// mustFailStop opens path, requires an error, and requires that the file is
// unchanged afterwards (nothing was truncated away).
func mustFailStop(t *testing.T, path string, what string) {
	t.Helper()
	before := readFile(t, path)
	w, err := OpenPath(path)
	if err == nil {
		got := w.Load()
		w.Close()
		t.Fatalf("%s: Open succeeded (recovered %d entries) but must fail-stop", what, len(got.Log))
	}
	if after := readFile(t, path); !bytes.Equal(before, after) {
		t.Fatalf("%s: Open failed but modified the file (%d -> %d bytes); fail-stop must not touch it", what, len(before), len(after))
	}
}

// mustRecover opens path, requires success with exactly the state after k records
// and a file truncated to wantSize.
func mustRecover(t *testing.T, path string, k int, wantSize int64, what string) {
	t.Helper()
	w, err := OpenPath(path)
	if err != nil {
		t.Fatalf("%s: Open failed: %v (a torn tail must be truncated and recovered)", what, err)
	}
	defer w.Close()
	if got := w.Load(); !samePersisted(got, wantAfter(k)) {
		t.Fatalf("%s: recovered state differs from the state after %d records: got %d entries %+v", what, k, len(got.Log), got.HardState)
	}
	if fi, _ := os.Stat(path); fi.Size() != wantSize {
		t.Fatalf("%s: file size after recovery = %d, want %d", what, fi.Size(), wantSize)
	}
}

// A corrupted length field in a record that is followed by valid records must
// never be mistaken for an incomplete tail: Open must fail and the file must be
// left exactly as it was.
func TestRecoveryCorruptedLengthAmidLaterRecordsFailsStopUnchanged(t *testing.T) {
	for _, length := range []uint32{1 << 20, 3, 0, 0xFFFFFFFF} {
		for k := 0; k < logRecords-1; k++ {
			path, starts, _ := buildLog(t)
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			// Both byte orders are written so the test does not depend on the layout.
			var be, le [4]byte
			binary.BigEndian.PutUint32(be[:], length)
			binary.LittleEndian.PutUint32(le[:], length)
			orig := readFile(t, path)
			for _, b := range [][4]byte{be, le} {
				f.WriteAt(orig[starts[k]:starts[k]+int64(recordHeaderSize)], starts[k]) // restore
				for off := int64(0); off+4 <= int64(recordHeaderSize); off++ {
					f.WriteAt(b[:], starts[k]+off)
					mustFailStop(t, path, "length "+itoa(int(length))+" written at record "+itoa(k)+" header offset "+itoa(int(off)))
					f.WriteAt(orig[starts[k]+off:starts[k]+off+4], starts[k]+off)
				}
			}
			f.Close()
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for n := i; n > 0; n /= 10 {
		s = string(rune('0'+n%10)) + s
	}
	return s
}

// Flipping any single byte of any record that is followed by more records must
// fail-stop, whether the byte is in the header (length, type, checksums) or the
// payload.
func TestRecoveryFlipEveryByteOfMiddleRecordsFailsStopUnchanged(t *testing.T) {
	path, starts, size := buildLog(t)
	orig := readFile(t, path)
	bounds := append(append([]int64(nil), starts...), size)
	for rec := 0; rec < logRecords-1; rec++ {
		for pos := bounds[rec]; pos < bounds[rec+1]; pos++ {
			data := append([]byte(nil), orig...)
			data[pos] ^= 0xFF
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			mustFailStop(t, path, "flip byte "+itoa(int(pos))+" of record "+itoa(rec))
		}
	}
}

// In the final record: damage to the header makes its extent unknown (fail-stop);
// damage to the payload (or payload checksum) is a torn tail and is truncated.
func TestRecoveryFlipEveryByteOfFinalRecord(t *testing.T) {
	path, starts, size := buildLog(t)
	orig := readFile(t, path)
	last := starts[logRecords-1]
	for pos := last; pos < size; pos++ {
		data := append([]byte(nil), orig...)
		data[pos] ^= 0xFF
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		inHeaderCoveredByHeaderCRC := pos-last < int64(recordHeaderSize)-4 // length, type, header checksum
		if inHeaderCoveredByHeaderCRC {
			mustFailStop(t, path, "flip header byte "+itoa(int(pos-last))+" of the final record")
		} else {
			mustRecover(t, path, logRecords-1, last, "flip byte "+itoa(int(pos-last))+" of the final record")
		}
	}
}

// Cutting the file at every possible byte offset (a crash while writing) must
// recover exactly the records that were completely written, and nothing else.
func TestRecoveryTruncateAtEveryOffset(t *testing.T) {
	path, starts, size := buildLog(t)
	orig := readFile(t, path)
	bounds := append(append([]int64(nil), starts...), size)
	for cut := int64(0); cut <= size; cut++ {
		if err := os.WriteFile(path, orig[:cut], 0600); err != nil {
			t.Fatal(err)
		}
		complete := 0
		for complete < logRecords && bounds[complete+1] <= cut {
			complete++
		}
		mustRecover(t, path, complete, bounds[complete], "file cut at offset "+itoa(int(cut)))
	}
}

// A file extended with zero bytes (some filesystems do this after a crash) is a
// torn tail; a tail of non-zero garbage with a bad header is corruption.
func TestRecoveryZeroFilledTailAndGarbageTail(t *testing.T) {
	path, _, size := buildLog(t)
	orig := readFile(t, path)

	if err := os.WriteFile(path, append(append([]byte(nil), orig...), make([]byte, 100)...), 0600); err != nil {
		t.Fatal(err)
	}
	mustRecover(t, path, logRecords, size, "100 zero bytes appended")

	garbage := bytes.Repeat([]byte{0xAB}, 40)
	if err := os.WriteFile(path, append(append([]byte(nil), orig...), garbage...), 0600); err != nil {
		t.Fatal(err)
	}
	mustFailStop(t, path, "40 bytes of non-zero garbage appended")
}
