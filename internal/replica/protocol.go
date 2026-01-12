package replica

import (
	"time"

	"github.com/dip-proto/bigbunny/internal/store"
)

// MessageType identifies the kind of replication message being sent between nodes in the cluster.
type MessageType int

const (
	MsgCreateStore MessageType = iota
	MsgUpdateStore
	MsgDeleteStore
	MsgLockAcquired
	MsgLockReleased
	MsgHeartbeat
	MsgHeartbeatAck
	MsgRequestSnapshot
	MsgSnapshotData
	MsgRegistryReserve
	MsgRegistryCommit
	MsgRegistryAbort
	MsgRegistryDelete
)

func (m MessageType) String() string {
	switch m {
	case MsgCreateStore:
		return "CREATE_STORE"
	case MsgUpdateStore:
		return "UPDATE_STORE"
	case MsgDeleteStore:
		return "DELETE_STORE"
	case MsgLockAcquired:
		return "LOCK_ACQUIRED"
	case MsgLockReleased:
		return "LOCK_RELEASED"
	case MsgHeartbeat:
		return "HEARTBEAT"
	case MsgHeartbeatAck:
		return "HEARTBEAT_ACK"
	case MsgRequestSnapshot:
		return "REQUEST_SNAPSHOT"
	case MsgSnapshotData:
		return "SNAPSHOT_DATA"
	case MsgRegistryReserve:
		return "REGISTRY_RESERVE"
	case MsgRegistryCommit:
		return "REGISTRY_COMMIT"
	case MsgRegistryAbort:
		return "REGISTRY_ABORT"
	case MsgRegistryDelete:
		return "REGISTRY_DELETE"
	default:
		return "UNKNOWN"
	}
}

// ReplicationMessage carries store data changes from the primary to secondaries, including creates, updates, deletes, and lock state changes.
type ReplicationMessage struct {
	Type        MessageType   `json:"type"`
	StoreID     string        `json:"store_id,omitempty"`
	ShardID     string        `json:"shard_id,omitempty"`
	CustomerID  string        `json:"customer_id,omitempty"`
	DataType    uint8         `json:"data_type,omitempty"`
	Body        []byte        `json:"body,omitempty"`
	ExpiresAt   time.Time     `json:"expires_at,omitempty"`
	Version     uint64        `json:"version,omitempty"`
	LeaderEpoch uint64        `json:"leader_epoch,omitempty"`
	LockID      string        `json:"lock_id,omitempty"`
	LockTimeout time.Duration `json:"lock_timeout,omitempty"`
	PendingName string        `json:"pending_name,omitempty"`
	Tombstone   bool          `json:"tombstone,omitempty"`
	SourceHost  string        `json:"source_host,omitempty"`
	Timestamp   time.Time     `json:"timestamp,omitempty"`
}

// HeartbeatMessage is sent periodically by the primary to all secondaries to maintain its lease and share cluster state.
type HeartbeatMessage struct {
	HostID      string    `json:"host_id"`
	Address     string    `json:"address"` // TCP address of sender (for request forwarding)
	LeaderEpoch uint64    `json:"leader_epoch"`
	StoreCount  int       `json:"store_count"`
	MemoryUsage int64     `json:"memory_usage"`
	Timestamp   time.Time `json:"timestamp"`
}

// HeartbeatAck is the response a secondary sends back after receiving a heartbeat, letting the primary know the secondary is alive.
type HeartbeatAck struct {
	HostID         string    `json:"host_id"`
	LeaderEpoch    uint64    `json:"leader_epoch"`
	LastSeenLeader time.Time `json:"last_seen_leader"`
	Timestamp      time.Time `json:"timestamp"`
}

// SnapshotRequest is sent by a joining node to request the current state from the primary during recovery.
type SnapshotRequest struct {
	HostID       string `json:"host_id"`
	SinceVersion uint64 `json:"since_version"` // 0 = full snapshot
}

// TombstoneEntry records a deleted store and when it was deleted, used to prevent resurrection of recently deleted stores.
type TombstoneEntry struct {
	StoreID   string    `json:"store_id"`
	DeletedAt time.Time `json:"deleted_at"`
}

// SnapshotData contains a complete point-in-time copy of all stores and tombstones that a joining node uses to bootstrap its state.
type SnapshotData struct {
	HostID      string           `json:"host_id"`
	Stores      []*store.Store   `json:"stores"`
	Tombstones  []TombstoneEntry `json:"tombstones"`
	LeaderEpoch uint64           `json:"leader_epoch"`
	Complete    bool             `json:"complete"`
}

// RegistryReplicationMessage carries named store registry changes from the primary to secondaries, including reservations, commits, aborts, and deletes.
type RegistryReplicationMessage struct {
	Type          MessageType `json:"type"`
	CustomerID    string      `json:"customer_id"`
	Name          string      `json:"name"`
	StoreID       string      `json:"store_id,omitempty"`
	State         int         `json:"state"`
	ExpiresAt     time.Time   `json:"expires_at,omitempty"`
	ReservationID string      `json:"reservation_id,omitempty"`
	LeaderEpoch   uint64      `json:"leader_epoch"`
	Version       uint64      `json:"version"`
	SourceHost    string      `json:"source_host"`
	Timestamp     time.Time   `json:"timestamp"`
}
