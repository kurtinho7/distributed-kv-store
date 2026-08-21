package cluster

import (
	"fmt"
	"strings"
	"time"
)

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
	nodeID   string
	leaderID string
	members  []Member
}

func NewState(nodeID string, leaderID string, members []Member) *State {
	if leaderID == "" {
		leaderID = nodeID
	}

	if len(members) == 0 {
		members = []Member{
			{ID: nodeID, Address: "localhost:8080"},
		}
	}
	return &State{
		nodeID:   nodeID,
		leaderID: leaderID,
		members:  members,
	}
}

func ParseMembers(config string) ([]Member, error) {
	if strings.TrimSpace(config) == "" {
		return nil, nil
	}

	parts := strings.Split(config, ",")
	members := make([]Member, 0, len(parts))
	seen := make(map[string]bool, len(parts))

	for _, part := range parts {
		id, address, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || strings.TrimSpace(id) == "" || strings.TrimSpace(address) == "" {
			return nil, fmt.Errorf("invalid member format: %q, expected id=address", part)
		}

		id = strings.TrimSpace(id)
		if seen[id] {
			return nil, fmt.Errorf("duplicate member ID: %q", id)
		}

		seen[id] = true
		members = append(members, Member{
			ID:      id,
			Address: strings.TrimRight(strings.TrimSpace(address), "/"),
		})
	}
	return members, nil
}

func (s *State) Snapshot(logIndex uint64) []Member {
	now := time.Now().UTC()
	members := make([]Member, 0, len(s.members))

	for _, member := range s.members {
		if member.ID == s.leaderID {
			member.Role = RoleLeader
		} else {
			member.Role = RoleFollower
		}

		member.Healthy = true
		member.UpdatedAt = now

		if member.ID == s.nodeID {
			member.LogIndex = logIndex
		}

		members = append(members, member)

	}

	return members
}

func (s *State) IsLeader() bool {
	return s.nodeID == s.leaderID
}

func (s *State) Leader() (Member, bool){
	for _, member := range s.members {
		if member.ID == s.leaderID {
			return member, true
		}
	}
	return Member{}, false
}

func (s *State) Peers() []Member {
	peers := make([]Member, 0, len(s.members)-1)

	for _, member := range s.members {
		if member.ID == s.nodeID {
			continue
		}

		peers = append(peers, member)
	}
	return peers
}

func (s *State) Majority() int {
	return (len(s.members) / 2) + 1
}

func (s *State) MemberByID(id string) (Member, bool) {
	for _, member := range s.members {
		if member.ID == id {
			return member, true
		}
	}
	return Member{}, false
}
