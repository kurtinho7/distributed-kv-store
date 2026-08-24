package apply

import (
	"testing"

	"kvstore/internal/oplog"
	"kvstore/internal/store"
)

func TestAdvanceCommitAppliesEntriesThroughCommitIndex(t *testing.T) {
	log := oplog.New()
	kv := store.NewMemory()
	applier := NewApplier(log, kv)

	if _, err := log.Append(oplog.OperationPut, "a", "1"); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if _, err := log.Append(oplog.OperationPut, "b", "2"); err != nil {
		t.Fatalf("append second: %v", err)
	}

	if err := applier.AdvanceCommit(1); err != nil {
		t.Fatalf("advance commit: %v", err)
	}

	if _, err := kv.Get("b"); err != store.ErrNotFound {
		t.Fatalf("expected b to remain unapplied, got %v", err)
	}

	value, err := kv.Get("a")
	if err != nil {
		t.Fatalf("expected a to be applied: %v", err)
	}
	if value != "1" {
		t.Fatalf("expected a=1, got %q", value)
	}

	if applier.CommitIndex() != 1 {
		t.Fatalf("expected commit index 1, got %d", applier.CommitIndex())
	}
}

func TestAdvanceCommitRejectsIndexBeyondLog(t *testing.T) {
	log := oplog.New()
	kv := store.NewMemory()
	applier := NewApplier(log, kv)

	if err := applier.AdvanceCommit(1); err == nil {
		t.Fatal("expected commit beyond log to fail")
	}
}

func TestRebuildCommittedRemovesUncommittedEntriesFromStore(t *testing.T) {
	log := oplog.New()
	kv := store.NewMemory()
	applier := NewApplier(log, kv)

	if _, err := log.Append(oplog.OperationPut, "committed", "yes"); err != nil {
		t.Fatalf("append committed: %v", err)
	}
	if _, err := log.Append(oplog.OperationPut, "uncommitted", "no"); err != nil {
		t.Fatalf("append uncommitted: %v", err)
	}

	if err := applier.AdvanceCommit(1); err != nil {
		t.Fatalf("advance commit: %v", err)
	}

	kv.Put("uncommitted", "no")

	if err := applier.RebuildCommitted(); err != nil {
		t.Fatalf("rebuild committed: %v", err)
	}

	if _, err := kv.Get("uncommitted"); err != store.ErrNotFound {
		t.Fatalf("expected uncommitted key to be removed, got %v", err)
	}
}

func TestResetCommitLowersCommitAndRebuildsStore(t *testing.T) {
	log := oplog.New()
	kv := store.NewMemory()
	applier := NewApplier(log, kv)

	if _, err := log.Append(oplog.OperationPut, "first", "yes"); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if _, err := log.Append(oplog.OperationPut, "second", "no"); err != nil {
		t.Fatalf("append second: %v", err)
	}
	if err := applier.AdvanceCommit(2); err != nil {
		t.Fatalf("advance commit: %v", err)
	}

	if err := log.TruncateFrom(2); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := applier.ResetCommit(1); err != nil {
		t.Fatalf("reset commit: %v", err)
	}

	if applier.CommitIndex() != 1 {
		t.Fatalf("expected commit index 1, got %d", applier.CommitIndex())
	}
	if _, err := kv.Get("second"); err != store.ErrNotFound {
		t.Fatalf("expected second key to be removed, got %v", err)
	}
}
