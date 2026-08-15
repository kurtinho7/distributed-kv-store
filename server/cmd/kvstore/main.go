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

	kv := store.NewMemory()
	operationLog := oplog.New()
	state := cluster.NewState(nodeID)
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
