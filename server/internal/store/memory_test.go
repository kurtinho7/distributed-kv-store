package store

import "testing"

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
