package faults

import (
	"sort"
	"sync"
)

type State struct {
	mu                     sync.RWMutex
	droppedReplicationTo   map[string]bool
}

func NewState() *State {
	return &State{
		droppedReplicationTo: make(map[string]bool),
	}
}

func (s *State) DropReplicationTo(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.droppedReplicationTo[nodeID] = true
}

func (s *State) HealReplicationTo(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.droppedReplicationTo, nodeID)
}

func (s *State) ShouldDropReplicationTo(nodeID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.droppedReplicationTo[nodeID]
}

func (s *State) DroppedReplicationTargets() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	targets := make([]string, 0, len(s.droppedReplicationTo))
	for nodeID := range s.droppedReplicationTo {
		targets = append(targets, nodeID)
	}

	sort.Strings(targets)

	return targets
}