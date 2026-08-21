package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"time"

	"kvstore/internal/cluster"
)

type ElectionRunner struct {
	state    *State
	peers    []cluster.Member
	majority int
	client   *http.Client
}

func NewElectionRunner(state *State, peers []cluster.Member, majority int) *ElectionRunner {
	return &ElectionRunner{
		state:    state,
		peers:    peers,
		majority: majority,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

func (e *ElectionRunner) Start(ctx context.Context) {
	for {
		timeout := time.Duration(750+rand.Intn(750)) * time.Millisecond
		timer := time.NewTimer(timeout)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if e.state.LeaderTimedOut(timeout) {
				e.runElection(ctx)
			}
		}
	}
}

func (e *ElectionRunner) runElection(ctx context.Context) {
	term := e.state.BecomeCandidate()
	votes := 1

	req := RequestVoteRequest{
		Term:        term,
		CandidateID: e.state.NodeID(),
	}

	body, err := json.Marshal(req)
	if err != nil {
		return
	}

	for _, peer := range e.peers {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, peer.Address+"/internal/raft/request-vote", bytes.NewReader(body))
		if err != nil {
			continue
		}
		request.Header.Set("Content-Type", "application/json")

		response, err := e.client.Do(request)
		if err != nil {
			log.Printf("request vote from %s failed: %v", peer.ID, err)
			continue
		}

		var vote RequestVoteResponse
		if err := json.NewDecoder(response.Body).Decode(&vote); err != nil {
			_ = response.Body.Close()
			continue
		}
		_ = response.Body.Close()

		if vote.Term > e.state.CurrentTerm() {
			e.state.BecomeFollower(vote.Term, "")
			return
		}

		if vote.VoteGranted {
			votes++
		}
	}

	if votes >= e.majority {
		e.state.BecomeLeader()
		log.Printf("became leader term=%d votes=%d", term, votes)
	}
}