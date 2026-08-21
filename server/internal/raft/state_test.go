package raft

import (
	"testing"
	"time"
)

func TestNewStateStartsAsFollower(t *testing.T) {
	state := NewState("node-1")

	if state.Role() != RoleFollower {
		t.Fatalf("expected follower, got %q", state.Role())
	}

	if state.CurrentTerm() != 0 {
		t.Fatalf("expected term 0, got %d", state.CurrentTerm())
	}
}

func TestBecomeCandidateIncrementsTermAndVotesForSelf(t *testing.T) {
	state := NewState("node-1")

	term := state.BecomeCandidate()

	if term != 1 {
		t.Fatalf("expected term 1, got %d", term)
	}

	if state.Role() != RoleCandidate {
		t.Fatalf("expected candidate, got %q", state.Role())
	}
}

func TestBecomeLeaderRecordsSelfAsLeader(t *testing.T) {
	state := NewState("node-1")

	state.BecomeCandidate()
	state.BecomeLeader()

	if state.Role() != RoleLeader {
		t.Fatalf("expected leader, got %q", state.Role())
	}

	if state.LeaderID() != "node-1" {
		t.Fatalf("expected node-1 leader, got %q", state.LeaderID())
	}
}

func TestBecomeFollowerWithHigherTermClearsLeaderRole(t *testing.T) {
	state := NewState("node-1")

	state.BecomeCandidate()
	state.BecomeLeader()
	state.BecomeFollower(2, "node-2")

	if state.Role() != RoleFollower {
		t.Fatalf("expected follower, got %q", state.Role())
	}

	if state.CurrentTerm() != 2 {
		t.Fatalf("expected term 2, got %d", state.CurrentTerm())
	}

	if state.LeaderID() != "node-2" {
		t.Fatalf("expected node-2 leader, got %q", state.LeaderID())
	}
}

func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	state := NewState("node-1")
	state.BecomeFollower(3, "node-2")

	response := state.RequestVote(RequestVoteRequest{
		Term:        2,
		CandidateID: "node-3",
	})

	if response.VoteGranted {
		t.Fatal("expected stale vote request to be rejected")
	}

	if response.Term != 3 {
		t.Fatalf("expected current term 3, got %d", response.Term)
	}
}

func TestRequestVoteGrantsVoteForNewTerm(t *testing.T) {
	state := NewState("node-1")

	response := state.RequestVote(RequestVoteRequest{
		Term:        1,
		CandidateID: "node-2",
	})

	if !response.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if response.Term != 1 {
		t.Fatalf("expected term 1, got %d", response.Term)
	}
}

func TestRequestVoteRejectsSecondCandidateInSameTerm(t *testing.T) {
	state := NewState("node-1")

	first := state.RequestVote(RequestVoteRequest{
		Term:        1,
		CandidateID: "node-2",
	})
	if !first.VoteGranted {
		t.Fatal("expected first vote to be granted")
	}

	second := state.RequestVote(RequestVoteRequest{
		Term:        1,
		CandidateID: "node-3",
	})
	if second.VoteGranted {
		t.Fatal("expected second vote in same term to be rejected")
	}
}

func TestRequestVoteAllowsRepeatVoteForSameCandidate(t *testing.T) {
	state := NewState("node-1")

	first := state.RequestVote(RequestVoteRequest{
		Term:        1,
		CandidateID: "node-2",
	})
	second := state.RequestVote(RequestVoteRequest{
		Term:        1,
		CandidateID: "node-2",
	})

	if !first.VoteGranted || !second.VoteGranted {
		t.Fatal("expected repeated vote for same candidate to be granted")
	}
}

func TestAppendEntriesRecordsLeaderHeartbeat(t *testing.T) {
	state := NewState("node-2")

	response := state.AppendEntries(AppendEntriesRequest{
		Term:     1,
		LeaderID: "node-1",
	})

	if !response.Success {
		t.Fatal("expected heartbeat to succeed")
	}

	if response.Term != 1 {
		t.Fatalf("expected term 1, got %d", response.Term)
	}

	if state.LeaderID() != "node-1" {
		t.Fatalf("expected node-1 leader, got %q", state.LeaderID())
	}

	if state.Role() != RoleFollower {
		t.Fatalf("expected follower role, got %q", state.Role())
	}

	if state.LastHeartbeat().IsZero() {
		t.Fatal("expected last heartbeat to be recorded")
	}
}

func TestAppendEntriesRejectsStaleTerm(t *testing.T) {
	state := NewState("node-2")
	state.BecomeFollower(3, "node-1")

	response := state.AppendEntries(AppendEntriesRequest{
		Term:     2,
		LeaderID: "node-3",
	})

	if response.Success {
		t.Fatal("expected stale heartbeat to be rejected")
	}

	if response.Term != 3 {
		t.Fatalf("expected current term 3, got %d", response.Term)
	}

	if state.LeaderID() != "node-1" {
		t.Fatalf("expected leader to remain node-1, got %q", state.LeaderID())
	}
}

func TestLeaderTimedOutReturnsFalseForLeader(t *testing.T) {
	state := NewState("node-1")
	state.BecomeCandidate()
	state.BecomeLeader()

	if state.LeaderTimedOut(1 * time.Second) {
		t.Fatal("expected leader not to time out itself")
	}
}

func TestLeaderTimedOutReturnsTrueWithoutHeartbeat(t *testing.T) {
	state := NewState("node-2")

	if !state.LeaderTimedOut(1 * time.Second) {
		t.Fatal("expected follower without heartbeat to be timed out")
	}
}

func TestLeaderTimedOutReturnsFalseAfterHeartbeat(t *testing.T) {
	state := NewState("node-2")
	state.AppendEntries(AppendEntriesRequest{
		Term:     1,
		LeaderID: "node-1",
	})

	if state.LeaderTimedOut(1 * time.Second) {
		t.Fatal("expected recent heartbeat not to time out")
	}
}
