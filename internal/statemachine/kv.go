package statemachine

import (
	"fmt"
	"kvstore/internal/storage"
	"kvstore/pkg/types"
)

type KVStateMachine struct {
	store *storage.MemoryStore
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{
		store: storage.NewMemoryStore(),
	}
}

// Apply executes a committed log entry
func (kv *KVStateMachine) Apply(entry types.LogEntry) (interface{}, error) {
	cmd := entry.Command

	switch cmd.Type {

	case types.CommandPut:
		kv.store.Put(cmd.Key, cmd.Value)
		return nil, nil

	case types.CommandDelete:
		kv.store.Delete(cmd.Key)
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// Get is used for read operations (not through Raft log)
func (kv *KVStateMachine) Get(key string) (string, bool) {
	return kv.store.Get(key)
}