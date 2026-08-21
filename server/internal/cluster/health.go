package cluster

import (
	"context"
	"net/http"
	"time"
)

type HealthChecker struct {
	state  *State
	client *http.Client
}

func NewHealthChecker(state *State) *HealthChecker {
	return &HealthChecker{
		state: state,
		client: &http.Client{
			Timeout: 500 * time.Millisecond,
		},
	}
}

func (h *HealthChecker) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	h.check(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.check(ctx)
		}
	}
}

func (h *HealthChecker) check(ctx context.Context) {
	for _, peer := range h.state.Peers() {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, peer.Address+"/healthz", nil)
		if err != nil {
			h.state.SetHealth(peer.ID, false)
			continue
		}

		response, err := h.client.Do(request)
		if err != nil {
			h.state.SetHealth(peer.ID, false)
			continue
		}

		_ = response.Body.Close()

		if response.StatusCode >= 200 && response.StatusCode < 300 {
			h.state.SetHealth(peer.ID, true)
		} else {
			h.state.SetHealth(peer.ID, false)
		}
	}

	h.state.SetHealth(h.state.NodeID(), true)
}