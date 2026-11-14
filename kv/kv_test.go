package kv

import (
	"errors"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	sm := NewStateMachine()

	put := Command{
		ClientID:  1,
		RequestID: 1,
		Op:        OpPut,
		Key:       "name",
		Value:     "ForgeDB",
	}
	got, err := sm.Apply(put)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if got.Value != "ForgeDB" || !got.Found {
		t.Fatalf("Put result = %+v, want ForgeDB and Found=true", got)
	}

	get, err := sm.Apply(Command{
		ClientID:  1,
		RequestID: 2,
		Op:        OpGet,
		Key:       "name",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if get.Value != "ForgeDB" || !get.Found {
		t.Fatalf("Get result = %+v, want ForgeDB and Found=true", get)
	}
}

func TestGetMissingKey(t *testing.T) {
	sm := NewStateMachine()

	got, err := sm.Apply(Command{
		ClientID:  1,
		RequestID: 1,
		Op:        OpGet,
		Key:       "missing",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Found || got.Value != "" {
		t.Fatalf("Get result = %+v, want empty value and Found=false", got)
	}
}

func TestPutOverwritesExistingValue(t *testing.T) {
	sm := NewStateMachine()

	for i, value := range []string{"first", "second"} {
		_, err := sm.Apply(Command{
			ClientID:  1,
			RequestID: uint64(i + 1),
			Op:        OpPut,
			Key:       "key",
			Value:     value,
		})
		if err != nil {
			t.Fatalf("Put %q failed: %v", value, err)
		}
	}

	got, err := sm.Apply(Command{
		ClientID:  1,
		RequestID: 3,
		Op:        OpGet,
		Key:       "key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "second" || !got.Found {
		t.Fatalf("Get result = %+v, want second", got)
	}
}

func TestEmptyValueIsValid(t *testing.T) {
	sm := NewStateMachine()

	_, err := sm.Apply(Command{
		ClientID:  1,
		RequestID: 1,
		Op:        OpPut,
		Key:       "key",
		Value:     "",
	})
	if err != nil {
		t.Fatalf("Put empty value failed: %v", err)
	}

	got, err := sm.Apply(Command{
		ClientID:  1,
		RequestID: 2,
		Op:        OpGet,
		Key:       "key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.Value != "" {
		t.Fatalf("Get result = %+v, want Found=true with empty value", got)
	}
}

func TestMarshalUnmarshalCommand(t *testing.T) {
	want := Command{
		ClientID:  1,
		RequestID: 42,
		Op:        OpPut,
		Key:       "language",
		Value:     "Go",
	}

	data, err := MarshalCommand(want)
	if err != nil {
		t.Fatalf("MarshalCommand failed: %v", err)
	}

	got, err := UnmarshalCommand(data)
	if err != nil {
		t.Fatalf("UnmarshalCommand failed: %v", err)
	}
	if got != want {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestUnmarshalRejectsMalformedCommands(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{"invalid JSON", `{"request_id":`},
		{"unknown field", `{"request_id":1,"op":"get","key":"x","extra":true}`},
		{"trailing JSON", `{"request_id":1,"op":"get","key":"x"} {}`},
		{"zero request ID", `{"request_id":0,"op":"get","key":"x"}`},
		{"empty key", `{"request_id":1,"op":"get","key":""}`},
		{"unknown operation", `{"request_id":1,"op":"delete","key":"x"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalCommand([]byte(tt.data))
			if err == nil {
				t.Fatal("expected command to be rejected")
			}
		})
	}
}

func TestApplyRejectsInvalidCommandWithoutMutation(t *testing.T) {
	sm := NewStateMachine()

	_, err := sm.Apply(Command{
		ClientID:  1,
		RequestID: 0,
		Op:        OpPut,
		Key:       "key",
		Value:     "bad",
	})
	if !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("error = %v, want ErrInvalidCommand", err)
	}

	got, err := sm.Apply(Command{
		ClientID:  1,
		RequestID: 1,
		Op:        OpGet,
		Key:       "key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Found {
		t.Fatalf("invalid Put changed state: %+v", got)
	}
}

func TestStateMachinesAreIndependent(t *testing.T) {
	first := NewStateMachine()
	second := NewStateMachine()

	_, err := first.Apply(Command{
		ClientID:  1,
		RequestID: 1,
		Op:        OpPut,
		Key:       "key",
		Value:     "first",
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := second.Apply(Command{
		ClientID:  1,
		RequestID: 1,
		Op:        OpGet,
		Key:       "key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Found {
		t.Fatalf("second state machine unexpectedly contains first's data: %+v", got)
	}
}

func TestDuplicatePutDoesNotApplyTwice(t *testing.T) {
	sm := NewStateMachine()
	first := Command{ClientID: 1, RequestID: 1, Op: OpPut, Key: "key", Value: "first"}
	second := Command{ClientID: 1, RequestID: 2, Op: OpPut, Key: "key", Value: "second"}

	original, err := sm.Apply(first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sm.Apply(second); err != nil {
		t.Fatal(err)
	}

	retry, err := sm.Apply(first)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if retry != original {
		t.Fatalf("retry result = %+v, want original %+v", retry, original)
	}

	current, err := sm.Apply(Command{ClientID: 1, RequestID: 3, Op: OpGet, Key: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if current.Value != "second" || !current.Found {
		t.Fatalf("retry reapplied old Put: current value = %+v", current)
	}
}

func TestDuplicateGetReturnsOriginalResult(t *testing.T) {
	sm := NewStateMachine()
	get := Command{ClientID: 1, RequestID: 1, Op: OpGet, Key: "key"}

	first, err := sm.Apply(get)
	if err != nil {
		t.Fatal(err)
	}
	if first.Found {
		t.Fatalf("initial Get = %+v, want missing key", first)
	}

	if _, err := sm.Apply(Command{ClientID: 1, RequestID: 2, Op: OpPut, Key: "key", Value: "later"}); err != nil {
		t.Fatal(err)
	}

	retry, err := sm.Apply(get)
	if err != nil {
		t.Fatal(err)
	}
	if retry != first {
		t.Fatalf("retry result = %+v, want original %+v", retry, first)
	}
}

func TestRequestIDsAreScopedToClient(t *testing.T) {
	sm := NewStateMachine()

	for _, cmd := range []Command{
		{ClientID: 1, RequestID: 1, Op: OpPut, Key: "one", Value: "first"},
		{ClientID: 2, RequestID: 1, Op: OpPut, Key: "two", Value: "second"},
	} {
		if _, err := sm.Apply(cmd); err != nil {
			t.Fatalf("Apply(%+v): %v", cmd, err)
		}
	}
}

func TestReusingRequestIDForDifferentCommandIsRejected(t *testing.T) {
	sm := NewStateMachine()
	first := Command{ClientID: 1, RequestID: 1, Op: OpPut, Key: "key", Value: "first"}
	if _, err := sm.Apply(first); err != nil {
		t.Fatal(err)
	}

	conflict := Command{ClientID: 1, RequestID: 1, Op: OpPut, Key: "key", Value: "different"}
	if _, err := sm.Apply(conflict); !errors.Is(err, ErrRequestIDConflict) {
		t.Fatalf("error = %v, want ErrRequestIDConflict", err)
	}
}
