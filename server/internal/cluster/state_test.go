package cluster

import "testing"

func TestParseMembers(t *testing.T) {
	members, err := ParseMembers("node-1=http://localhost:8080,node-2=http://localhost:8081")
	if err != nil {
		t.Fatalf("parse members: %v", err)
	}

	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}

	if members[0].ID != "node-1" || members[0].Address != "http://localhost:8080" {
		t.Fatalf("unexpected first member: %#v", members[0])
	}

	if members[1].ID != "node-2" || members[1].Address != "http://localhost:8081" {
		t.Fatalf("unexpected second member: %#v", members[1])
	}
}

func TestParseMembersRejectsInvalidMember(t *testing.T) {
	_, err := ParseMembers("node-1")
	if err == nil {
		t.Fatal("expected invalid member error")
	}
}

func TestParseMembersRejectsDuplicateIDs(t *testing.T) {
	_, err := ParseMembers("node-1=http://localhost:8080,node-1=http://localhost:8081")
	if err == nil {
		t.Fatal("expected duplicate member error")
	}
}

func TestSnapshotMarksLeaderAndLocalLogIndex(t *testing.T) {
	state := NewState("node-2", "node-1", []Member{
		{ID: "node-1", Address: "http://localhost:8080"},
		{ID: "node-2", Address: "http://localhost:8081"},
		{ID: "node-3", Address: "http://localhost:8082"},
	})

	members := state.Snapshot(7)

	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}

	if members[0].Role != RoleLeader {
		t.Fatalf("expected node-1 to be leader, got %q", members[0].Role)
	}

	if members[1].Role != RoleFollower {
		t.Fatalf("expected node-2 to be follower, got %q", members[1].Role)
	}

	if members[1].LogIndex != 7 {
		t.Fatalf("expected local node log index 7, got %d", members[1].LogIndex)
	}

	if members[2].LogIndex != 0 {
		t.Fatalf("expected remote node log index 0, got %d", members[2].LogIndex)
	}
}

func TestStateHelpers(t *testing.T) {
	state := NewState("node-2", "node-1", []Member{
		{ID: "node-1", Address: "http://localhost:8080"},
		{ID: "node-2", Address: "http://localhost:8081"},
		{ID: "node-3", Address: "http://localhost:8082"},
	})

	if state.IsLeader() {
		t.Fatal("expected node-2 to be follower")
	}

	leader, ok := state.Leader()
	if !ok {
		t.Fatal("expected leader to exist")
	}
	if leader.ID != "node-1" {
		t.Fatalf("expected node-1 leader, got %q", leader.ID)
	}

	peers := state.Peers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}
	if peers[0].ID == "node-2" || peers[1].ID == "node-2" {
		t.Fatalf("expected peers to exclude local node: %#v", peers)
	}

	if state.Majority() != 2 {
		t.Fatalf("expected majority 2, got %d", state.Majority())
	}
}

func TestMemberByID(t *testing.T) {
	state := NewState("node-2", "node-1", []Member{
		{ID: "node-1", Address: "http://localhost:8080"},
		{ID: "node-2", Address: "http://localhost:8081"},
	})

	member, ok := state.MemberByID("node-1")
	if !ok {
		t.Fatal("expected member to exist")
	}

	if member.Address != "http://localhost:8080" {
		t.Fatalf("expected node-1 address, got %q", member.Address)
	}

	_, ok = state.MemberByID("missing")
	if ok {
		t.Fatal("expected missing member lookup to fail")
	}
}
