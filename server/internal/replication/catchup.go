package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"kvstore/internal/oplog"
	"kvstore/internal/store"
)

type catchUpResponse struct {
	Entries []oplog.Entry `json:"entries"`
}

func CatchUp(ctx context.Context, localNodeID string, leaderAddress string, log *oplog.Log, kv *store.Memory) error {
	if leaderAddress == "" {
		return fmt.Errorf("leader address is required")
	}

	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/internal/log?after=%d", leaderAddress, log.LastIndex())
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	request.Header.Set("X-Node-ID", localNodeID)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("leader returned status %d", response.StatusCode)
	}

	var body catchUpResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		return err
	}

	for _, entry := range body.Entries {
		if err := log.AppendEntry(entry); err != nil {
			return fmt.Errorf("append entry %d: %w", entry.Index, err)
		}

		if err := kv.Apply(entry); err != nil {
			return fmt.Errorf("apply entry %d: %w", entry.Index, err)
		}
	}

	return nil
}