package wal

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ANAXMD/ForgeDB/raft"
)

func TestHardStateRoundTrip(t *testing.T) {
	want := raft.HardState{Term: 42, VotedFor: 7}

	data, err := encodeHardState(want)
	if err != nil {
		t.Fatal(err)
	}

	got, err := decodeHardState(data)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestEntriesRoundTrip(t *testing.T) {
	want := []raft.Entry{
		{Term: 1, Index: 1, Data: []byte("first")},
		{Term: 3, Index: 2, Data: []byte("second")},
		{Term: 3, Index: 3, Data: nil},
	}

	data, err := encodeEntries(want)
	if err != nil {
		t.Fatal(err)
	}

	got, err := decodeEntries(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Term != want[i].Term ||
			got[i].Index != want[i].Index ||
			!bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("entry %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestRecordChecksumDetectsCorruption(t *testing.T) {
	record, err := encodeRecord(recordEntries, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}

	header := append([]byte(nil), record[:recordHeaderSize]...)
	payload := append([]byte(nil), record[recordHeaderSize:]...)
	payload[0] ^= 0xff

	_, err = decodeRecord(header, payload)
	if !errors.Is(err, errInvalidRecord) {
		t.Fatalf("got error %v, want errInvalidRecord", err)
	}
}

func TestDecodeEntriesRejectsTrailingData(t *testing.T) {
	data, err := encodeEntries([]raft.Entry{
		{Term: 1, Index: 1, Data: []byte("command")},
	})
	if err != nil {
		t.Fatal(err)
	}

	data = append(data, 0)
	if _, err := decodeEntries(data); !errors.Is(err, errInvalidRecord) {
		t.Fatalf("got error %v, want errInvalidRecord", err)
	}
}
