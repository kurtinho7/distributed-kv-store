package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"kvstore/internal/cluster"
)

type HeartbeatSender struct {
	state  *State
	peers  []cluster.Member
	client *http.Client
}

func NewHeartbeatSender(state *State, peers []cluster.Member) *HeartbeatSender {
	return &HeartbeatSender{
		state: state,
		peers: peers,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

func (h *HeartbeatSender) Start(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	h.send(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.send(ctx)
		}
	}
}

func (h *HeartbeatSender) send(ctx context.Context) {
	if h.state.Role() != RoleLeader {
		return
	}

	req := AppendEntriesRequest{
		Term:     h.state.CurrentTerm(),
		LeaderID: h.state.LeaderID(),
	}

	body, err := json.Marshal(req)
	if err != nil {
		return
	}

	for _, peer := range h.peers {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, peer.Address+"/internal/raft/append-entries", bytes.NewReader(body))
		if err != nil {
			continue
		}
		request.Header.Set("Content-Type", "application/json")

		response, err := h.client.Do(request)
		if err != nil {
			log.Printf("heartbeat to %s failed: %v", peer.ID, err)
			continue
		}
		_ = response.Body.Close()
	}
}