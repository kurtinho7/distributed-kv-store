package replication

import (
	"context"
	"log"
	"time"

	"kvstore/internal/cluster"
	"kvstore/internal/oplog"
	"kvstore/internal/raft"
	"kvstore/internal/store"
)

type CatchUpRunner struct {
	cluster *cluster.State
	raft    *raft.State
	log     *oplog.Log
	store   *store.Memory
}

func NewCatchUpRunner(cluster *cluster.State, raft *raft.State, log *oplog.Log, store *store.Memory) *CatchUpRunner {
	return &CatchUpRunner{
		cluster: cluster,
		raft:    raft,
		log:     log,
		store:   store,
	}
}

func (r *CatchUpRunner) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	r.catchUp(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.catchUp(ctx)
		}
	}
}

func (r *CatchUpRunner) catchUp(ctx context.Context) {
	if r.raft.Role() == raft.RoleLeader {
		return
	}

	leaderID := r.raft.LeaderID()
	if leaderID == "" {
		return
	}

	leader, ok := r.cluster.MemberByID(leaderID)
	if !ok {
		return
	}

	if err := CatchUp(ctx, r.cluster.NodeID(), leader.Address, r.log, r.store); err != nil {
		log.Printf("periodic catch-up from leader %s failed: %v", leader.ID, err)
	}
}