package store

import (
	"errors"
	"sync"

	"kvstore/internal/oplog"
)

var ErrNotFound = errors.New("key not found")

type Memory struct {
	mu   sync.RWMutex
	data map[string]string
}

type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewMemory() *Memory {
	return &Memory{
		data: make(map[string]string),
	}
}

func (m *Memory) Put(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
}

func (m *Memory) Get(key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.data[key]
	if !ok {
		return "", ErrNotFound
	}
	return value, nil
}

func (m *Memory) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[key]; !ok {
		return ErrNotFound
	}
	delete(m.data, key)
	return nil
}

func (m *Memory) Snapshot() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]Entry, 0, len(m.data))
	for key, value := range m.data {
		entries = append(entries, Entry{Key: key, Value: value})
	}
	return entries
}

func (m *Memory) Apply(entry oplog.Entry) error {
	switch entry.Operation {
	case oplog.OperationPut:
		m.Put(entry.Key, entry.Value)
	case oplog.OperationDelete:
		return m.Delete(entry.Key)
	default:
		return errors.New("unknown operation")
	}

	return nil
}

func (m *Memory) Rebuild(entries []oplog.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data := make(map[string]string)
	for _, entry := range entries {
		switch entry.Operation {
		case oplog.OperationPut:
			data[entry.Key] = entry.Value
		case oplog.OperationDelete:
			delete(data, entry.Key)
		default:
			return errors.New("unknown operation")
		}
	}

	m.data = data
	return nil
}
