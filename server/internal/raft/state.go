package raft

import (
	"sync"
	"time"
)

type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

type State struct {
	mu          sync.RWMutex
	nodeID      string
	currentTerm uint64
	votedFor    string
	role        Role
	leaderID    string
	lastHeartbeat time.Time
}

type RequestVoteRequest struct {
	Term        uint64 `json:"term"`
	CandidateID string `json:"candidateId"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

type AppendEntriesRequest struct {
	Term     uint64 `json:"term"`
	LeaderID string `json:"leaderId"`
}

type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

type Snapshot struct {
	NodeID        string    `json:"nodeId"`
	CurrentTerm   uint64    `json:"currentTerm"`
	VotedFor      string    `json:"votedFor"`
	Role          Role      `json:"role"`
	LeaderID      string    `json:"leaderId"`
	LastHeartbeat time.Time `json:"lastHeartbeat"`
}

func NewState(nodeID string) *State {
	return &State{
		nodeID: nodeID,
		role:   RoleFollower,
	}
}

func (s *State) CurrentTerm() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentTerm
}

func (s *State) Role() Role {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.role
}

func (s *State) LeaderID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leaderID
}

func (s *State) NodeID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nodeID
}

func (s *State) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return Snapshot{
		NodeID:        s.nodeID,
		CurrentTerm:   s.currentTerm,
		VotedFor:      s.votedFor,
		Role:          s.role,
		LeaderID:      s.leaderID,
		LastHeartbeat: s.lastHeartbeat,
	}
}

func (s *State) LastHeartbeat() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastHeartbeat
}

func (s *State) BecomeFollower(term uint64, leaderID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if term > s.currentTerm {
		s.currentTerm = term
		s.votedFor = ""
	}

	s.role = RoleFollower
	s.leaderID = leaderID
}

func (s *State) BecomeCandidate() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentTerm++
	s.role = RoleCandidate
	s.leaderID = ""
	s.votedFor = s.nodeID

	return s.currentTerm
}

func (s *State) BecomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.role = RoleLeader
	s.leaderID = s.nodeID
}

func (s *State) RequestVote(req RequestVoteRequest) RequestVoteResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.currentTerm {
		return RequestVoteResponse{
			Term:        s.currentTerm,
			VoteGranted: false,
		}
	}

	if req.Term > s.currentTerm {
		s.currentTerm = req.Term
		s.votedFor = ""
		s.role = RoleFollower
		s.leaderID = ""
	}

	if s.votedFor == "" || s.votedFor == req.CandidateID {
		s.votedFor = req.CandidateID
		return RequestVoteResponse{
			Term:        s.currentTerm,
			VoteGranted: true,
		}
	}

	return RequestVoteResponse{
		Term:        s.currentTerm,
		VoteGranted: false,
	}
}

func (s *State) AppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.currentTerm {
		return AppendEntriesResponse{
			Term:    s.currentTerm,
			Success: false,
		}
	}

	if req.Term > s.currentTerm {
		s.currentTerm = req.Term
		s.votedFor = ""
	}

	s.role = RoleFollower
	s.leaderID = req.LeaderID
	s.lastHeartbeat = time.Now().UTC()

	return AppendEntriesResponse{
		Term:    s.currentTerm,
		Success: true,
	}
}

func (s *State) LeaderTimedOut(timeout time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.role == RoleLeader {
		return false
	}

	if s.lastHeartbeat.IsZero() {
		return true
	}

	return time.Since(s.lastHeartbeat) > timeout
}