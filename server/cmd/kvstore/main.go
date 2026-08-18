package main

import (
	"log"
	"net/http"
	"os"

	"kvstore/internal/cluster"
	"kvstore/internal/httpapi"
	"kvstore/internal/oplog"
	"kvstore/internal/store"
)

func main() {
	addr := env("KV_ADDR", ":8080")
	nodeID := env("KV_NODE_ID", "node-1")
	leaderID := env("KV_LEADER_ID", nodeID)
	logPath := os.Getenv("KV_LOG_PATH")

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
	members := []cluster.Member{
		{ID: nodeID, Address: addr},
	}
	state := cluster.NewState(nodeID, leaderID, members)
	handler := httpapi.NewServer(kv, state, operationLog)

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
