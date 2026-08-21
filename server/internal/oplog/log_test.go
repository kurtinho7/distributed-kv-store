package oplog

import (
	"testing"
	"time"
)

func TestLogAppendAssignsOrderedIndexes(t *testing.T) {
	log := New()

	first, err := log.Append(OperationPut, "language", "go")
	if err != nil {
		t.Fatalf("append first entry: %v", err)
	}
	second, err := log.Append(OperationDelete, "language", "")
	if err != nil {
		t.Fatalf("append second entry: %v", err)
	}

	if first.Index != 1 {
		t.Fatalf("expected first index 1, got %d", first.Index)
	}
	if second.Index != 2 {
		t.Fatalf("expected second index 2, got %d", second.Index)
	}
	if log.LastIndex() != 2 {
		t.Fatalf("expected last index 2, got %d", log.LastIndex())
	}
}

func TestLogEntriesReturnsCopy(t *testing.T) {
	log := New()
	if _, err := log.Append(OperationPut, "language", "go"); err != nil {
		t.Fatalf("append entry: %v", err)
	}

	entries := log.Entries()
	entries[0].Key = "mutated"

	if log.Entries()[0].Key != "language" {
		t.Fatal("expected entries snapshot to be isolated from internal log")
	}
}

func TestOpenReplaysPersistedEntries(t *testing.T) {
	path := t.TempDir() + "/kv.log"

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	if _, err := log.Append(OperationPut, "language", "go"); err != nil {
		t.Fatalf("append put: %v", err)
	}
	if _, err := log.Append(OperationDelete, "language", ""); err != nil {
		t.Fatalf("append delete: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	defer reopened.Close()

	entries := reopened.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Operation != OperationPut || entries[1].Operation != OperationDelete {
		t.Fatalf("unexpected entries: %#v", entries)
	}

	next, err := reopened.Append(OperationPut, "runtime", "go")
	if err != nil {
		t.Fatalf("append after replay: %v", err)
	}
	if next.Index != 3 {
		t.Fatalf("expected next index 3, got %d", next.Index)
	}
}

func TestAppendEntryPreservesReplicatedIndex(t *testing.T) {
	log := New()

	err := log.AppendEntry(Entry{
		Index:     1,
		Operation: OperationPut,
		Key:       "language",
		Value:     "go",
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("append replicated entry: %v", err)
	}

	entries := log.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if entries[0].Index != 1 {
		t.Fatalf("expected replicated index 1, got %d", entries[0].Index)
	}
}

func TestAppendEntryRejectsOutOfOrderIndex(t *testing.T) {
	log := New()

	err := log.AppendEntry(Entry{
		Index:     2,
		Operation: OperationPut,
		Key:       "language",
		Value:     "go",
		CreatedAt: time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("expected out-of-order append to fail")
	}
}

func TestEntriesAfter(t *testing.T) {
	log := New()

	if _, err := log.Append(OperationPut, "a", "1"); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if _, err := log.Append(OperationPut, "b", "2"); err != nil {
		t.Fatalf("append second: %v", err)
	}
	if _, err := log.Append(OperationDelete, "a", ""); err != nil {
		t.Fatalf("append third: %v", err)
	}

	entries := log.EntriesAfter(1)

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	if entries[0].Index != 2 || entries[1].Index != 3 {
		t.Fatalf("unexpected entries after index 1: %#v", entries)
	}
}

func TestTruncateFromRemovesEntriesFromIndex(t *testing.T) {
	log := New()

	if _, err := log.Append(OperationPut, "a", "1"); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if _, err := log.Append(OperationPut, "b", "2"); err != nil {
		t.Fatalf("append second: %v", err)
	}

	if err := log.TruncateFrom(2); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	entries := log.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if entries[0].Key != "a" {
		t.Fatalf("expected key a, got %q", entries[0].Key)
	}

	next, err := log.Append(OperationPut, "c", "3")
	if err != nil {
		t.Fatalf("append after truncate: %v", err)
	}

	if next.Index != 2 {
		t.Fatalf("expected next index 2, got %d", next.Index)
	}
}

func TestTruncateFromRewritesPersistentLog(t *testing.T) {
	path := t.TempDir() + "/kv.log"

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	if _, err := log.Append(OperationPut, "a", "1"); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if _, err := log.Append(OperationPut, "b", "2"); err != nil {
		t.Fatalf("append second: %v", err)
	}
	if err := log.TruncateFrom(2); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	defer reopened.Close()

	entries := reopened.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 persisted entry, got %d", len(entries))
	}

	if entries[0].Key != "a" {
		t.Fatalf("expected persisted key a, got %q", entries[0].Key)
	}
}
