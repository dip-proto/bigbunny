package routing

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"slices"
)

// Host represents a node in the cluster.
type Host struct {
	ID      string
	Address string // host:port for intra-PoP communication
	Healthy bool
}

// ReplicaSet contains the primary and secondary hosts for a given shard.
type ReplicaSet struct {
	Primary   *Host
	Secondary *Host
	ShardID   string
}

// RendezvousHasher implements rendezvous (highest random weight) hashing
// for deterministic shard-to-host mapping. All nodes compute the same
// placement without coordination.
//
// The routing secret prevents adversaries from pre-computing which shardIDs
// map to which hosts, protecting against targeted host overload attacks.
type RendezvousHasher struct {
	hosts         []*Host
	routingSecret string
}

// NewRendezvousHasher creates a hasher with the given host list and routing secret.
// The routing secret must be shared across all nodes in the cluster.
func NewRendezvousHasher(hosts []*Host, routingSecret string) *RendezvousHasher {
	return &RendezvousHasher{
		hosts:         hosts,
		routingSecret: routingSecret,
	}
}

// UpdateHosts replaces the host list.
func (r *RendezvousHasher) UpdateHosts(hosts []*Host) {
	r.hosts = hosts
}

// GetReplicaSet returns the primary and secondary hosts for a shard.
// Uses rendezvous hashing: each host is scored by hash(secret, shardID, hostID),
// and the two highest-scoring healthy hosts become primary and secondary.
func (r *RendezvousHasher) GetReplicaSet(shardID string) *ReplicaSet {
	if len(r.hosts) == 0 {
		return nil
	}

	type scored struct {
		host  *Host
		score uint64
	}

	scores := make([]scored, 0, len(r.hosts))
	for _, h := range r.hosts {
		scores = append(scores, scored{
			host:  h,
			score: r.hash(shardID, h.ID),
		})
	}

	slices.SortFunc(scores, func(a, b scored) int {
		return cmp.Compare(b.score, a.score) // descending order
	})

	rs := &ReplicaSet{ShardID: shardID}

	// Primary: highest scoring healthy host
	for _, s := range scores {
		if s.host.Healthy {
			rs.Primary = s.host
			break
		}
	}

	// Secondary: next highest scoring healthy host (different from primary)
	for _, s := range scores {
		if s.host.Healthy && (rs.Primary == nil || s.host.ID != rs.Primary.ID) {
			rs.Secondary = s.host
			break
		}
	}

	// Fallback: if no healthy hosts, use unhealthy ones
	if rs.Primary == nil && len(scores) > 0 {
		rs.Primary = scores[0].host
	}
	if rs.Secondary == nil && len(scores) > 1 {
		rs.Secondary = scores[1].host
	}

	return rs
}

func (r *RendezvousHasher) hash(shardID, hostID string) uint64 {
	h := sha256.New()
	// Include routing secret to prevent adversaries from pre-computing
	// which shardIDs map to which hosts
	h.Write([]byte(r.routingSecret))
	h.Write([]byte(shardID))
	h.Write([]byte(hostID))
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

// GetAllHosts returns all hosts known to the hasher, regardless of health status.
func (r *RendezvousHasher) GetAllHosts() []*Host {
	return r.hosts
}

// GetHealthyHosts returns only the hosts currently marked as healthy.
func (r *RendezvousHasher) GetHealthyHosts() []*Host {
	var healthy []*Host
	for _, h := range r.hosts {
		if h.Healthy {
			healthy = append(healthy, h)
		}
	}
	return healthy
}

// SortHostsByID sorts hosts by ID in lexicographic order for deterministic ordering.
func SortHostsByID(hosts []*Host) {
	slices.SortFunc(hosts, func(a, b *Host) int {
		return cmp.Compare(a.ID, b.ID)
	})
}
