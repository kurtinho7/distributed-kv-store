package faults

import "testing"

func TestDropAndHealReplicationTo(t *testing.T) {
	state := NewState()

	if state.ShouldDropReplicationTo("node-3") {
		t.Fatal("expected replication to start enabled")
	}

	state.DropReplicationTo("node-3")

	if !state.ShouldDropReplicationTo("node-3") {
		t.Fatal("expected replication to be dropped")
	}

	state.HealReplicationTo("node-3")

	if state.ShouldDropReplicationTo("node-3") {
		t.Fatal("expected replication to be healed")
	}
}

func TestDroppedReplicationTargets(t *testing.T) {
	state := NewState()

	state.DropReplicationTo("node-3")
	state.DropReplicationTo("node-2")

	targets := state.DroppedReplicationTargets()

	if len(targets) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(targets))
	}

	if targets[0] != "node-2" || targets[1] != "node-3" {
		t.Fatalf("unexpected targets: %#v", targets)
	}
}