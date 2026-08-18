package oplog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	file      *os.File
}

func New() *Log {
	return &Log{
		entries:   make([]Entry, 0),
		nextIndex: 1,
	}
}

func Open(path string) (*Log, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	log := New()
	log.file = file

	if err := log.load(path); err != nil {
		_ = file.Close()
		return nil, err
	}

	return log, nil
}

func (l *Log) Append(operation Operation, key, value string) (Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := Entry{
		Index:     l.nextIndex,
		Operation: operation,
		Key:       key,
		Value:     value,
		CreatedAt: time.Now().UTC(),
	}

	if err := l.write(entry); err != nil {
		return Entry{}, err
	}

	l.entries = append(l.entries, entry)
	l.nextIndex++

	return entry, nil
}

func (l *Log) AppendEntry(entry Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if entry.Index != l.nextIndex {
		return fmt.Errorf("invalid entry index: expected %d, got %d", l.nextIndex, entry.Index)
	}

	if err := l.write(entry); err != nil {
		return err
	}

	l.entries = append(l.entries, entry)
	l.nextIndex++

	return nil
}

func (l *Log) Entries() []Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	entries := make([]Entry, len(l.entries))
	copy(entries, l.entries)
	return entries
}

func (l *Log) EntriesAfter(index uint64) []Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	entries := make([]Entry, 0)
	for _, entry := range l.entries {
		if entry.Index > index {
			entries = append(entries, entry)
		}
	}
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

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil
	}
	return l.file.Close()
}

func (l *Log) load(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("decode log entry: %w", err)
		}
		l.entries = append(l.entries, entry)
		if entry.Index >= l.nextIndex {
			l.nextIndex = entry.Index + 1
		}
	}

	return scanner.Err()
}

func (l *Log) write(entry Entry) error {
	if l.file == nil {
		return nil
	}

	if err := json.NewEncoder(l.file).Encode(entry); err != nil {
		return err
	}
	return l.file.Sync()
}
