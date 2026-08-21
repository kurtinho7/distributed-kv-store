package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"kvstore/internal/cluster"
	"kvstore/internal/httpapi"
	"kvstore/internal/oplog"
	"kvstore/internal/replication"
	"kvstore/internal/store"
	raftstate "kvstore/internal/raft"
	"kvstore/internal/faults"
)

func main() {
	addr := env("KV_ADDR", ":8080")
	nodeID := env("KV_NODE_ID", "node-1")
	leaderID := env("KV_LEADER_ID", nodeID)
	logPath := os.Getenv("KV_LOG_PATH")
	clusterConfig := os.Getenv("KV_CLUSTER")

	kv := store.NewMemory()
	operationLog := oplog.New()
	if logPath != "" {
		persistentLog, err := oplog.Open(logPath)
		if err != nil {
			log.Fatalf("open operation log: %v", err)
		}
		defer persistentLog.Close()
		operationLog = persistentLog
		for _, entry := range operationLog.Entries() {
			if err := kv.Apply(entry); err != nil {
				log.Fatalf("replay operation log: %v", err)
			}
		}
		log.Printf("replayed %d operation(s) from %s", len(operationLog.Entries()), logPath)
	}
	members, err := cluster.ParseMembers(clusterConfig)
	if err != nil {
		log.Fatalf("parse cluster config: %v", err)
	}
	state := cluster.NewState(nodeID, leaderID, members)
	healthChecker := cluster.NewHealthChecker(state)
	go healthChecker.Start(context.Background())
	raft := raftstate.NewState(nodeID)

	if nodeID == leaderID {
		raft.BecomeCandidate()
		raft.BecomeLeader()
	}

	if !state.IsLeader() {
		leader, ok := state.Leader()
		if !ok {
			log.Fatalf("leader %q not found in cluster members", leaderID)
		}

		if err := replication.CatchUp(context.Background(), leader.Address, operationLog, kv); err != nil {
			log.Printf("Initial catch-up failed: %v", err)
		} else {
			log.Printf("Successfully caught from leader %s through log index %d", leader.ID, operationLog.LastIndex())
		}
	}
	faultsState := faults.NewState()
	handler := httpapi.NewServer(kv, state, operationLog, raft, faultsState)

	sender := raftstate.NewHeartbeatSender(raft, state.Peers())
	go sender.Start(context.Background())

	election := raftstate.NewElectionRunner(raft, state.Peers(), state.Majority())
	go election.Start(context.Background())

	log.Printf("starting kvstore node=%s addr=%s", nodeID, addr)
	if err := http.ListenAndServe(addr, handler.Routes()); err != nil {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
