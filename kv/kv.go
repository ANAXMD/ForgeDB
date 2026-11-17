package kv

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type Op string

const (
	OpPut Op = "put"
	OpGet Op = "get"
)

const MaxCommandSize = 1 << 20 // 1 MiB of encoded JSON.

var (
	ErrInvalidCommand    = errors.New("kv: invalid command")
	ErrUnknownOp         = errors.New("kv: unknown operation")
	ErrRequestIDConflict = errors.New("kv: request ID reused with different command")
	ErrCommandTooLarge   = errors.New("kv: command too large")
)

// Command is one operation submitted through Raft.
// Request IDs are unique within a client.
type Command struct {
	ClientID  uint64 `json:"client_id"`
	RequestID uint64 `json:"request_id"`
	Op        Op     `json:"op"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
}

// Result is the deterministic result of applying a command.
type Result struct {
	Value string
	Found bool
}

type completedRequest struct {
	command Command
	result  Result
}

// StateMachine is deterministic when Apply is called serially in committed
// Raft log order.
type StateMachine struct {
	values  map[string]string
	clients map[uint64]map[uint64]completedRequest
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		values:  make(map[string]string),
		clients: make(map[uint64]map[uint64]completedRequest),
	}
}

// MarshalCommand encodes a command for storage in a Raft log entry.
func MarshalCommand(cmd Command) ([]byte, error) {
	if err := validateCommand(cmd); err != nil {
		return nil, err
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxCommandSize {
		return nil, ErrCommandTooLarge
	}
	return data, nil
}

// UnmarshalCommand decodes and validates a Raft log command.
func UnmarshalCommand(data []byte) (Command, error) {
	if len(data) > MaxCommandSize {
		return Command{}, ErrCommandTooLarge
	}

	var cmd Command

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&cmd); err != nil {
		return Command{}, fmt.Errorf("%w: %v", ErrInvalidCommand, err)
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Command{}, ErrInvalidCommand
	}

	if err := validateCommand(cmd); err != nil {
		return Command{}, err
	}
	return cmd, nil
}

// Apply applies one committed command. Replaying an identical client request
// returns its original result without applying the operation again.
func (sm *StateMachine) Apply(cmd Command) (Result, error) {
	if sm == nil {
		return Result{}, ErrInvalidCommand
	}
	if err := validateCommand(cmd); err != nil {
		return Result{}, err
	}

	requests := sm.clients[cmd.ClientID]
	if previous, exists := requests[cmd.RequestID]; exists {
		if previous.command != cmd {
			return Result{}, ErrRequestIDConflict
		}
		return previous.result, nil
	}

	var result Result
	switch cmd.Op {
	case OpPut:
		sm.values[cmd.Key] = cmd.Value
		result = Result{Value: cmd.Value, Found: true}

	case OpGet:
		value, found := sm.values[cmd.Key]
		result = Result{Value: value, Found: found}

	default:
		return Result{}, ErrUnknownOp
	}

	if requests == nil {
		requests = make(map[uint64]completedRequest)
		sm.clients[cmd.ClientID] = requests
	}
	requests[cmd.RequestID] = completedRequest{
		command: cmd,
		result:  result,
	}

	return result, nil
}

func validateCommand(cmd Command) error {
	if cmd.ClientID == 0 || cmd.RequestID == 0 || cmd.Key == "" {
		return ErrInvalidCommand
	}
	if cmd.Op != OpPut && cmd.Op != OpGet {
		return ErrUnknownOp
	}
	return nil
}
