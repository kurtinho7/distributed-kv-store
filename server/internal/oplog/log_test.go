package oplog

import "testing"

func TestLogAppendAssignsOrderedIndexes(t *testing.T) {
	log := New()

	first := log.Append(OperationPut, "language", "go")
	second := log.Append(OperationDelete, "language", "")

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
	log.Append(OperationPut, "language", "go")

	entries := log.Entries()
	entries[0].Key = "mutated"

	if log.Entries()[0].Key != "language" {
		t.Fatal("expected entries snapshot to be isolated from internal log")
	}
}
