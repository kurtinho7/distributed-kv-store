package oplog

import "testing"

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
