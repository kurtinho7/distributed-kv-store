package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
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
	mu          sync.Mutex
	root        string
	chaosJob    jobStatus
	hammerJob   jobStatus
	verifyJob   jobStatus
	scenarioJob jobStatus
}

type commandResponse struct {
	NodeID string `json:"nodeId,omitempty"`
	Action string `json:"action,omitempty"`
	Output string `json:"output"`
}

type hammerRequest struct {
	DurationSeconds int  `json:"durationSeconds"`
	Writers         int  `json:"writers"`
	Keyspace        int  `json:"keyspace"`
	ReadAfterWrite  bool `json:"readAfterWrite"`
}

func main() {
	addr := env("DEMOCTL_ADDR", ":9090")
	root := env("DEMOCTL_ROOT", findRoot())
	c := &controller{
		root: root,
		chaosJob: jobStatus{
			State: jobIdle,
		},
		hammerJob: jobStatus{
			State: jobIdle,
		},
		verifyJob: jobStatus{
			State: jobIdle,
		},
		scenarioJob: jobStatus{
			State: jobIdle,
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", c.health)
	mux.HandleFunc("POST /demo/chaos/start", c.startChaos)
	mux.HandleFunc("GET /demo/chaos/status", c.chaosStatus)
	mux.HandleFunc("POST /demo/hammer/start", c.startHammer)
	mux.HandleFunc("GET /demo/hammer/status", c.hammerStatus)
	mux.HandleFunc("POST /demo/verify/start", c.startVerify)
	mux.HandleFunc("GET /demo/verify/status", c.verifyStatus)
	mux.HandleFunc("POST /demo/scenarios/leader-failover/start", c.startLeaderFailover)
	mux.HandleFunc("GET /demo/scenarios/leader-failover/status", c.leaderFailoverStatus)
	mux.HandleFunc("POST /demo/scenarios/follower-catchup/start", c.startFollowerCatchUp)
	mux.HandleFunc("GET /demo/scenarios/follower-catchup/status", c.leaderFailoverStatus)
	mux.HandleFunc("POST /demo/nodes/", c.nodeAction)

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
	if c.chaosJob.State == jobRunning {
		status := c.chaosJob
		c.mu.Unlock()
		writeJSON(w, http.StatusConflict, status)
		return
	}

	now := time.Now().UTC()
	c.chaosJob = jobStatus{
		State:     jobRunning,
		StartedAt: &now,
		Output:    "",
	}
	c.mu.Unlock()

	go c.runJob(&c.chaosJob, "scripts/chaos-demo.sh", "--duration", "15", "--writers", "8", "--keyspace", "50")

	c.chaosStatus(w, nil)
}

func (c *controller) chaosStatus(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()

	writeJSON(w, http.StatusOK, c.chaosJob)
}

func (c *controller) startHammer(w http.ResponseWriter, r *http.Request) {
	req := hammerRequest{
		DurationSeconds: 30,
		Writers:         10,
		Keyspace:        100,
	}
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
	}
	if req.DurationSeconds <= 0 {
		req.DurationSeconds = 30
	}
	if req.Writers <= 0 {
		req.Writers = 10
	}
	if req.Keyspace <= 0 {
		req.Keyspace = 100
	}

	c.mu.Lock()
	if c.hammerJob.State == jobRunning {
		status := c.hammerJob
		c.mu.Unlock()
		writeJSON(w, http.StatusConflict, status)
		return
	}

	now := time.Now().UTC()
	c.hammerJob = jobStatus{
		State:     jobRunning,
		StartedAt: &now,
		Output:    "",
	}
	c.mu.Unlock()

	args := []string{
		"--duration", strconv.Itoa(req.DurationSeconds),
		"--writers", strconv.Itoa(req.Writers),
		"--keyspace", strconv.Itoa(req.Keyspace),
	}
	if req.ReadAfterWrite {
		args = append(args, "--read-after-write")
	}
	go c.runJob(&c.hammerJob, "scripts/hammer.sh", args...)

	c.hammerStatus(w, nil)
}

func (c *controller) hammerStatus(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()

	writeJSON(w, http.StatusOK, c.hammerJob)
}

func (c *controller) startVerify(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	if c.verifyJob.State == jobRunning {
		status := c.verifyJob
		c.mu.Unlock()
		writeJSON(w, http.StatusConflict, status)
		return
	}

	now := time.Now().UTC()
	c.verifyJob = jobStatus{
		State:     jobRunning,
		StartedAt: &now,
		Output:    "",
	}
	c.mu.Unlock()

	go c.runJob(&c.verifyJob, "scripts/verify-cluster.sh", "--timeout", "20")

	c.verifyStatus(w, nil)
}

func (c *controller) verifyStatus(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()

	writeJSON(w, http.StatusOK, c.verifyJob)
}

func (c *controller) startLeaderFailover(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	if c.scenarioJob.State == jobRunning {
		status := c.scenarioJob
		c.mu.Unlock()
		writeJSON(w, http.StatusConflict, status)
		return
	}

	now := time.Now().UTC()
	c.scenarioJob = jobStatus{
		State:     jobRunning,
		StartedAt: &now,
		Output:    "",
	}
	c.mu.Unlock()

	go c.runJob(&c.scenarioJob, "scripts/leader-failover-demo.sh", "--duration", "30", "--writers", "10", "--keyspace", "100")

	c.leaderFailoverStatus(w, nil)
}

func (c *controller) startFollowerCatchUp(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	if c.scenarioJob.State == jobRunning {
		status := c.scenarioJob
		c.mu.Unlock()
		writeJSON(w, http.StatusConflict, status)
		return
	}

	now := time.Now().UTC()
	c.scenarioJob = jobStatus{
		State:     jobRunning,
		StartedAt: &now,
		Output:    "",
	}
	c.mu.Unlock()

	go c.runJob(&c.scenarioJob, "scripts/follower-catchup-demo.sh", "--duration", "20", "--writers", "3", "--keyspace", "100")

	c.leaderFailoverStatus(w, nil)
}

func (c *controller) leaderFailoverStatus(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()

	writeJSON(w, http.StatusOK, c.scenarioJob)
}

func (c *controller) nodeAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/demo/nodes/")
	nodeID, action, ok := strings.Cut(path, "/")
	if !ok || nodeID == "" || action == "" {
		writeError(w, http.StatusBadRequest, "expected /demo/nodes/{nodeID}/{action}")
		return
	}

	if !allowedNode(nodeID) {
		writeError(w, http.StatusBadRequest, "unknown node")
		return
	}

	if !allowedAction(action) {
		writeError(w, http.StatusBadRequest, "unknown node action")
		return
	}

	output, err := c.runCompose(action, nodeID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, commandResponse{
			NodeID: nodeID,
			Action: action,
			Output: output,
		})
		return
	}

	writeJSON(w, http.StatusOK, commandResponse{
		NodeID: nodeID,
		Action: action,
		Output: output,
	})
}

func (c *controller) runCompose(action, nodeID string) (string, error) {
	cmd := exec.Command("docker", "compose", action, nodeID)
	cmd.Dir = c.root

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	err := cmd.Run()
	return output.String(), err
}

func (c *controller) runJob(status *jobStatus, script string, args ...string) {
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

	status.Output = output.String()
	status.ExitCode = &exitCode
	status.EndedAt = &endedAt
	if err != nil {
		status.State = jobFailed
		return
	}
	status.State = jobPassed
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
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

func allowedNode(nodeID string) bool {
	switch nodeID {
	case "node-1", "node-2", "node-3", "node-4", "node-5":
		return true
	default:
		return false
	}
}

func allowedAction(action string) bool {
	switch action {
	case "start", "stop", "restart":
		return true
	default:
		return false
	}
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
