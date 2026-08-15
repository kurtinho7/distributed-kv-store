package cluster

import "time"

type Role string

const (
	RoleLeader   Role = "leader"
	RoleFollower Role = "follower"
)

type Member struct {
	ID        string    `json:"id"`
	Address   string    `json:"address"`
	Role      Role      `json:"role"`
	Healthy   bool      `json:"healthy"`
	LogIndex  uint64    `json:"logIndex"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type State struct {
	nodeID string
}

func NewState(nodeID string) *State {
	return &State{nodeID: nodeID}
}

func (s *State) Snapshot(logIndex uint64) []Member {
	return []Member{
		{
			ID:        s.nodeID,
			Address:   "localhost:8080",
			Role:      RoleLeader,
			Healthy:   true,
			LogIndex:  logIndex,
			UpdatedAt: time.Now().UTC(),
		},
	}
}
