package store

import (
	"testing"

	"kvstore/internal/oplog"
)

func TestMemoryStoreLifecycle(t *testing.T) {
	kv := NewMemory()

	kv.Put("language", "go")

	value, err := kv.Get("language")
	if err != nil {
		t.Fatalf("expected key to exist: %v", err)
	}
	if value != "go" {
		t.Fatalf("expected go, got %q", value)
	}

	if err := kv.Delete("language"); err != nil {
		t.Fatalf("expected delete to succeed: %v", err)
	}

	if _, err := kv.Get("language"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryRebuildFromLogEntries(t *testing.T) {
	kv := NewMemory()

	err := kv.Rebuild([]oplog.Entry{
		{Index: 1, Operation: oplog.OperationPut, Key: "a", Value: "1"},
		{Index: 2, Operation: oplog.OperationPut, Key: "b", Value: "2"},
		{Index: 3, Operation: oplog.OperationDelete, Key: "a"},
	})
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	if _, err := kv.Get("a"); err != ErrNotFound {
		t.Fatalf("expected a to be deleted, got %v", err)
	}

	value, err := kv.Get("b")
	if err != nil {
		t.Fatalf("expected b to exist: %v", err)
	}
	if value != "2" {
		t.Fatalf("expected b=2, got %q", value)
	}
}

func TestMemoryRebuildRejectsUnknownOperation(t *testing.T) {
	kv := NewMemory()

	err := kv.Rebuild([]oplog.Entry{
		{Index: 1, Operation: oplog.Operation("unknown"), Key: "a"},
	})
	if err == nil {
		t.Fatal("expected unknown operation error")
	}
}
