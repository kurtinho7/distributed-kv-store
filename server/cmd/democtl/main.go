package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type jobState string

const (
	jobIdle    jobState = "idle"
	jobRunning jobState = "running"
	jobPassed  jobState = "passed"
	jobFailed  jobState = "failed"
)

type jobStatus struct {
	State     jobState   `json:"state"`
	ExitCode  *int       `json:"exitCode,omitempty"`
	Output    string     `json:"output"`
	StartedAt *time.Time `json:"startedAt,omitempty"`
	EndedAt   *time.Time `json:"endedAt,omitempty"`
}

type controller struct {
	mu     sync.Mutex
	root   string
	status jobStatus
}

func main() {
	addr := env("DEMOCTL_ADDR", ":9090")
	root := env("DEMOCTL_ROOT", findRoot())
	c := &controller{
		root: root,
		status: jobStatus{
			State: jobIdle,
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", c.health)
	mux.HandleFunc("POST /demo/chaos/start", c.startChaos)
	mux.HandleFunc("GET /demo/chaos/status", c.chaosStatus)

	log.Printf("starting demo controller addr=%s root=%s", addr, root)
	if err := http.ListenAndServe(addr, withCORS(mux)); err != nil {
		log.Fatal(err)
	}
}

func (c *controller) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (c *controller) startChaos(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	if c.status.State == jobRunning {
		status := c.status
		c.mu.Unlock()
		writeJSON(w, http.StatusConflict, status)
		return
	}

	now := time.Now().UTC()
	c.status = jobStatus{
		State:     jobRunning,
		StartedAt: &now,
		Output:    "",
	}
	c.mu.Unlock()

	go c.runScript("scripts/chaos-demo.sh", "--duration", "15", "--writers", "8", "--keyspace", "50")

	c.chaosStatus(w, nil)
}

func (c *controller) chaosStatus(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()

	writeJSON(w, http.StatusOK, c.status)
}

func (c *controller) runScript(script string, args ...string) {
	cmdArgs := append([]string{filepath.Join(c.root, script)}, args...)
	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	cmd.Dir = c.root

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	err := cmd.Run()
	endedAt := time.Now().UTC()

	c.mu.Lock()
	defer c.mu.Unlock()

	exitCode := 0
	if err != nil {
		exitCode = 1
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
	}

	c.status.Output = output.String()
	c.status.ExitCode = &exitCode
	c.status.EndedAt = &endedAt
	if err != nil {
		c.status.State = jobFailed
		return
	}
	c.status.State = jobPassed
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func findRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}

	for {
		if _, err := os.Stat(filepath.Join(wd, "docker-compose.yml")); err == nil {
			return wd
		}

		parent := filepath.Dir(wd)
		if parent == wd {
			return "."
		}
		wd = parent
	}
}
