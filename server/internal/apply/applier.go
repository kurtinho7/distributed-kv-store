package apply

import (
	"fmt"
	"sync"

	"kvstore/internal/oplog"
	"kvstore/internal/store"
)

type Applier struct {
	mu           sync.Mutex
	log          *oplog.Log
	store        *store.Memory
	commitIndex  uint64
	appliedIndex uint64
}

func NewApplier(log *oplog.Log, store *store.Memory) *Applier {
	return &Applier{
		log:   log,
		store: store,
	}
}

func (a *Applier) CommitIndex() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.commitIndex
}

func (a *Applier) AppliedIndex() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.appliedIndex
}

func (a *Applier) AdvanceCommit(index uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if index <= a.commitIndex {
		return nil
	}

	if index > a.log.LastIndex() {
		return fmt.Errorf("cannot commit index %d beyond last log index %d", index, a.log.LastIndex())
	}

	a.commitIndex = index

	for _, entry := range a.log.Entries() {
		if entry.Index <= a.appliedIndex {
			continue
		}
		if entry.Index > a.commitIndex {
			break
		}

		if err := a.store.Apply(entry); err != nil {
			return fmt.Errorf("apply entry %d: %w", entry.Index, err)
		}

		a.appliedIndex = entry.Index
	}

	return nil
}

func (a *Applier) RebuildCommitted() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	entries := make([]oplog.Entry, 0)
	for _, entry := range a.log.Entries() {
		if entry.Index > a.commitIndex {
			break
		}
		entries = append(entries, entry)
	}

	if err := a.store.Rebuild(entries); err != nil {
		return err
	}

	a.appliedIndex = a.commitIndex
	return nil
}

func (a *Applier) ResetCommit(index uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if index > a.log.LastIndex() {
		return fmt.Errorf("cannot reset commit index %d beyond last log index %d", index, a.log.LastIndex())
	}

	a.commitIndex = index
	entries := make([]oplog.Entry, 0)
	for _, entry := range a.log.Entries() {
		if entry.Index > a.commitIndex {
			break
		}
		entries = append(entries, entry)
	}

	if err := a.store.Rebuild(entries); err != nil {
		return err
	}

	a.appliedIndex = a.commitIndex
	return nil
}
