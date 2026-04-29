package storage

import "sync"

type MemoryStore struct {
	mu    sync.RWMutex
	store map[string]string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		store: make(map[string]string),
	}
}

// Put inserts or updates a key
func (m *MemoryStore) Put(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[key] = value
}

// Get retrieves a value
func (m *MemoryStore) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.store[key]
	return val, ok
}

// Delete removes a key
func (m *MemoryStore) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.store, key)
}