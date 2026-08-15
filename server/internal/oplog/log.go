package oplog

import (
	"sync"
	"time"
)

type Operation string

const (
	OperationPut    Operation = "put"
	OperationDelete Operation = "delete"
)

type Entry struct {
	Index     uint64    `json:"index"`
	Operation Operation `json:"operation"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

type Log struct {
	mu        sync.RWMutex
	entries   []Entry
	nextIndex uint64
}

func New() *Log {
	return &Log{
		entries:   make([]Entry, 0),
		nextIndex: 1,
	}
}

func (l *Log) Append(operation Operation, key, value string) Entry {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := Entry{
		Index:     l.nextIndex,
		Operation: operation,
		Key:       key,
		Value:     value,
		CreatedAt: time.Now().UTC(),
	}

	l.entries = append(l.entries, entry)
	l.nextIndex++

	return entry
}

func (l *Log) Entries() []Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	entries := make([]Entry, len(l.entries))
	copy(entries, l.entries)
	return entries
}

func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}
