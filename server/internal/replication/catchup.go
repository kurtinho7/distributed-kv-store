package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"kvstore/internal/apply"
	"kvstore/internal/oplog"
	"kvstore/internal/store"
)

type catchUpResponse struct {
	Entries     []oplog.Entry `json:"entries"`
	LastIndex   uint64        `json:"lastIndex"`
	CommitIndex uint64        `json:"commitIndex"`
}

func CatchUp(ctx context.Context, localNodeID string, leaderAddress string, log *oplog.Log, kv *store.Memory, applier *apply.Applier) error {
	if leaderAddress == "" {
		return fmt.Errorf("leader address is required")
	}

	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/internal/log?after=0", leaderAddress)
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

	leaderEntries := body.Entries
	localEntries := log.Entries()
	truncateFrom := firstConflictIndex(localEntries, leaderEntries)
	if truncateFrom == 0 && log.LastIndex() > body.LastIndex {
		truncateFrom = body.LastIndex + 1
	}

	if truncateFrom > 0 {
		if err := log.TruncateFrom(truncateFrom); err != nil {
			return fmt.Errorf("truncate from index %d: %w", truncateFrom, err)
		}
		resetIndex := truncateFrom - 1
		if err := applier.ResetCommit(resetIndex); err != nil {
			return fmt.Errorf("reset commit after truncate: %w", err)
		}
	}

	for _, entry := range leaderEntries {
		if entry.Index <= log.LastIndex() {
			continue
		}
		if err := log.AppendEntry(entry); err != nil {
			return fmt.Errorf("append entry %d: %w", entry.Index, err)
		}
	}

	if body.CommitIndex > 0 {
		if err := applier.AdvanceCommit(body.CommitIndex); err != nil {
			return fmt.Errorf("advance commit to %d: %w", body.CommitIndex, err)
		}
	}

	return nil
}

func firstConflictIndex(localEntries, leaderEntries []oplog.Entry) uint64 {
	leaderByIndex := make(map[uint64]oplog.Entry, len(leaderEntries))
	for _, entry := range leaderEntries {
		leaderByIndex[entry.Index] = entry
	}

	for _, local := range localEntries {
		leader, ok := leaderByIndex[local.Index]
		if !ok {
			return local.Index
		}
		if !sameCommand(local, leader) {
			return local.Index
		}
	}

	return 0
}

func sameCommand(a, b oplog.Entry) bool {
	return a.Index == b.Index &&
		a.Operation == b.Operation &&
		a.Key == b.Key &&
		a.Value == b.Value
}
