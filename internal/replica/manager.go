package replica

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dip-proto/bigbunny/internal/registry"
	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
)

// Role represents the current state of a node in the replication cluster.
type Role int

const (
	RoleUnknown Role = iota
	RolePrimary
	RoleSecondary
	RoleJoining
	RoleCatchingUp
)

func (r Role) String() string {
	switch r {
	case RolePrimary:
		return "PRIMARY"
	case RoleSecondary:
		return "SECONDARY"
	case RoleJoining:
		return "JOINING"
	case RoleCatchingUp:
		return "CATCHING_UP"
	default:
		return "UNKNOWN"
	}
}

// Config holds the settings for running a replica manager, including timeouts, addresses, and optional test hooks.
type Config struct {
	HostID             string
	Site               string
	TCPAddress         string        // TCP address of this host (for forwarding)
	LeaseDuration      time.Duration // 3x modify timeout + buffer
	LeaseGrace         time.Duration // additional grace before promotion
	HeartbeatInterval  time.Duration
	ReplicationTimeout time.Duration
	ModifyTimeout      time.Duration // lock timeout for modify operations
	InternalToken      string        // shared secret for internal endpoints

	// Tombstone limits (0 = no limit)
	TombstonePerCustomerLimit int // max tombstones per customer
	TombstoneGlobalLimit      int // max tombstones globally

	// Site verification
	DisableSiteVerification bool // if true, allow replication between nodes with different sites

	// Testing hooks (optional)
	HTTPClient           *http.Client     // custom client for network simulation; nil = default
	Now                  func() time.Time // custom clock for deterministic tests; nil = time.Now
	BroadcastReplication bool             // if true, replicate to all hosts instead of per-shard (test-only)
}

// DefaultConfig returns a Config with sensible defaults for production use.
func DefaultConfig(hostID, site string) *Config {
	return &Config{
		HostID:             hostID,
		Site:               site,
		LeaseDuration:      2 * time.Second, // 3x500ms modify + buffer
		LeaseGrace:         2 * time.Second, // conservative grace
		HeartbeatInterval:  200 * time.Millisecond,
		ReplicationTimeout: 5 * time.Second,
		ModifyTimeout:      500 * time.Millisecond,
	}
}

// Manager coordinates replication between nodes, handles failover, and maintains cluster consistency through heartbeats, lease checks, and tombstone tracking.
type Manager struct {
	config   *Config
	store    *store.Manager
	registry *registry.Manager
	hasher   *routing.RendezvousHasher
	client   *http.Client
	now      func() time.Time // clock function for testability

	mu               sync.RWMutex
	role             Role
	leaderEpoch      uint64
	leaderAddress    string // TCP address of current primary (for request forwarding)
	lastLeaderSeen   time.Time
	peerLastSeen     map[string]time.Time
	replicationQueue chan *ReplicationMessage
	registryQueue    chan *RegistryReplicationMessage

	tombstones             map[string]time.Time           // storeID -> tombstone time
	tombstoneCustomerIndex map[string]map[string]struct{} // customerID -> set of storeIDs
	tombstoneTTL           time.Duration

	lastPromotionAt      time.Time // when this node became primary (for lock unknown window)
	lastReplicationFail  time.Time // when replication last failed
	replicationFailCount int       // consecutive failures
	lastRecoveryAttempt  time.Time // when recovery was last attempted (for retry debounce)
	recoveryAttempts     int       // total recovery attempts (for testing/metrics)

	// joinSyncMu provides a barrier for in-flight mutations during join snapshot.
	// Writers/GC take RLock (concurrent), join snapshot takes Lock (exclusive).
	joinSyncMu sync.RWMutex

	// joinInProgress is set true while a join is active. Used for:
	// 1. Preventing concurrent joins (CAS)
	// 2. Fast-fail path for new writes (503 instead of blocking)
	joinInProgress atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
}

const recoveryRetryInterval = time.Second // minimum time between recovery attempts

// NewManager creates a new replication manager with the given configuration, store manager, and routing hasher.
func NewManager(config *Config, storeMgr *store.Manager, hasher *routing.RendezvousHasher) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	// Use custom client if provided, otherwise create default
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: config.ReplicationTimeout}
	}

	// Use custom clock if provided, otherwise use time.Now
	now := config.Now
	if now == nil {
		now = time.Now
	}

	m := &Manager{
		config:                 config,
		store:                  storeMgr,
		hasher:                 hasher,
		client:                 client,
		now:                    now,
		role:                   RoleUnknown,
		leaderEpoch:            0,
		peerLastSeen:           make(map[string]time.Time),
		replicationQueue:       make(chan *ReplicationMessage, 10000),
		registryQueue:          make(chan *RegistryReplicationMessage, 10000),
		tombstones:             make(map[string]time.Time),
		tombstoneCustomerIndex: make(map[string]map[string]struct{}),
		tombstoneTTL:           24 * time.Hour,
		ctx:                    ctx,
		cancel:                 cancel,
	}

	return m
}

// SetRegistry attaches a registry manager to enable named store replication and cleanup.
func (m *Manager) SetRegistry(reg *registry.Manager) {
	m.registry = reg
}

// GetClient returns the HTTP client used for internal cluster communication.
func (m *Manager) GetClient() *http.Client {
	return m.client
}

func (m *Manager) setInternalAuth(req *http.Request) {
	if m.config.InternalToken != "" {
		req.Header.Set("X-Internal-Token", m.config.InternalToken)
	}
}

// Start launches all background goroutines for replication, heartbeats, lease checking, and garbage collection.
func (m *Manager) Start() {
	go m.replicationLoop()
	go m.registryReplicationLoop()
	go m.heartbeatLoop()
	go m.leaseCheckLoop()
	go m.tombstoneGCLoop()
	go m.orphanGCLoop()
	go m.expiryGCLoop()
}

// Stop signals all background goroutines to shut down.
func (m *Manager) Stop() {
	m.cancel()
}

// Role returns the current role of this node in the cluster.
func (m *Manager) Role() Role {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.role
}

// LeaderEpoch returns the current leader epoch, which is incremented each time a new primary takes over.
func (m *Manager) LeaderEpoch() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.leaderEpoch
}

// GetLeaderAddress returns the TCP address of the current primary for request forwarding.
// If no leader address is known from heartbeats, falls back to deterministic inference
// from the host list (same logic as startup election). This eliminates startup races.
func (m *Manager) GetLeaderAddress() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// If we know the leader from heartbeats, use it
	if m.leaderAddress != "" {
		return m.leaderAddress
	}

	// Fallback: infer from sorted host list (deterministic)
	// This matches the startup election logic (first host = primary)
	hosts := m.hasher.GetHealthyHosts()
	if len(hosts) == 0 {
		return ""
	}

	// Sort by ID (deterministic ordering)
	sortedHosts := slices.Clone(hosts)
	routing.SortHostsByID(sortedHosts)

	return sortedHosts[0].Address
}

// RecoveryAttempts returns the total number of recovery attempts (for testing/metrics).
func (m *Manager) RecoveryAttempts() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.recoveryAttempts
}

// TryAcquireWriteBarrier attempts to acquire the write barrier.
// Returns (release func, true) on success, (nil, false) if join is in progress.
// Uses TryRLock to avoid blocking when a join is waiting for Lock().
func (m *Manager) TryAcquireWriteBarrier() (func(), bool) {
	if m.joinSyncMu.TryRLock() {
		return m.joinSyncMu.RUnlock, true
	}
	return nil, false
}

// StartJoinSync marks join as in-progress, then BLOCKS until all in-flight
// mutations complete. Returns false only if another join is already in progress.
func (m *Manager) StartJoinSync() bool {
	// Prevent concurrent joins (atomic CAS)
	if !m.joinInProgress.CompareAndSwap(false, true) {
		return false // Another join already in progress
	}

	// BLOCK until all in-flight writes complete (waiting for RLocks to release)
	// This is intentionally blocking - joins wait for writes, not the other way around
	m.joinSyncMu.Lock()
	return true
}

// EndJoinSync releases the exclusive lock and clears the in-progress flag.
// Note: Flag must be cleared before unlock to prevent spurious rejection of
// a new join in the window between unlock and flag clear.
func (m *Manager) EndJoinSync() {
	m.joinInProgress.Store(false)
	m.joinSyncMu.Unlock()
}

// IsJoinSyncing returns true if a join is in progress.
// Used for fast-fail 503 path in write handlers.
func (m *Manager) IsJoinSyncing() bool {
	return m.joinInProgress.Load()
}

// SetRole changes the role of this node and updates the epoch when becoming primary.
func (m *Manager) SetRole(role Role) {
	m.mu.Lock()
	defer m.mu.Unlock()
	wasSecondary := m.role == RoleSecondary
	m.role = role
	// Always initialize lastLeaderSeen to prevent immediate promotion
	m.lastLeaderSeen = m.now()
	if role == RolePrimary {
		m.leaderEpoch++
		// Set leader address to self when becoming primary
		m.leaderAddress = m.config.TCPAddress
		// Only set lastPromotionAt on actual failover (secondary → primary)
		// not on initial startup, since there's no prior lock state to be unknown about
		if wasSecondary {
			m.lastPromotionAt = m.now()
		}
	}
}

// ForcePromote forces this node to become primary, incrementing the epoch.
// This is for operational use when manual intervention is needed.
func (m *Manager) ForcePromote() {
	m.mu.Lock()
	defer m.mu.Unlock()
	wasNotPrimary := m.role != RolePrimary
	m.role = RolePrimary
	m.leaderEpoch++
	m.leaderAddress = m.config.TCPAddress
	m.lastLeaderSeen = m.now()
	if wasNotPrimary {
		m.lastPromotionAt = m.now()
		log.Printf("promoted to PRIMARY (epoch=%d)", m.leaderEpoch)
	}
}

// IsLockStateUnknown returns true during the brief window after failover when we cannot be sure whether a lock was held on the old primary.
func (m *Manager) IsLockStateUnknown(storeID string) (unknown bool, retryAfter time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.role != RolePrimary {
		return false, 0
	}

	if m.lastPromotionAt.IsZero() {
		return false, 0
	}

	unknownUntil := m.lastPromotionAt.Add(m.config.ModifyTimeout)
	now := m.now()

	if now.Before(unknownUntil) {
		remaining := max(unknownUntil.Sub(now), time.Second)
		return true, remaining
	}

	return false, 0
}

// QueueReplication adds a message to the async replication queue to be sent to secondaries.
func (m *Manager) QueueReplication(msg *ReplicationMessage) {
	msg.SourceHost = m.config.HostID
	msg.Timestamp = m.now()
	msg.LeaderEpoch = m.LeaderEpoch()

	select {
	case m.replicationQueue <- msg:
	default:
		log.Printf("replication queue full, dropping message: %s for store %s", msg.Type, msg.StoreID)
	}
}

func (m *Manager) replicationLoop() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case msg := <-m.replicationQueue:
			m.replicateToSecondary(msg)
		}
	}
}

func (m *Manager) getReplicationTargets(shardKey string) []*routing.Host {
	var targets []*routing.Host
	if m.config.BroadcastReplication {
		for _, host := range m.hasher.GetAllHosts() {
			if host.ID != m.config.HostID {
				targets = append(targets, host)
			}
		}
	} else {
		replicaSet := m.hasher.GetReplicaSet(shardKey)
		if replicaSet != nil {
			if replicaSet.Primary != nil && replicaSet.Primary.ID != m.config.HostID {
				targets = append(targets, replicaSet.Primary)
			}
			if replicaSet.Secondary != nil && replicaSet.Secondary.ID != m.config.HostID {
				targets = append(targets, replicaSet.Secondary)
			}
		}
	}
	return targets
}

func (m *Manager) replicateToSecondary(msg *ReplicationMessage) {
	if msg.StoreID == "" {
		return
	}

	if msg.ShardID == "" {
		log.Printf("replication message missing ShardID for store %s", msg.StoreID)
		return
	}

	targets := m.getReplicationTargets(msg.ShardID)

	// Track whether any target failed in this round
	anyFailed := false
	for _, host := range targets {
		if err := m.sendReplicationMessage(host.Address, msg); err != nil {
			log.Printf("failed to replicate to secondary %s: %v", host.ID, err)
			anyFailed = true
		}
	}

	// Record per-round success/failure (any failure = degraded)
	if anyFailed {
		m.recordReplicationFailure()
	} else if len(targets) > 0 {
		m.recordReplicationSuccess()
	}
}

func (m *Manager) recordReplicationFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastReplicationFail = m.now()
	m.replicationFailCount++
}

func (m *Manager) recordReplicationSuccess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicationFailCount = 0
}

// IsSecondaryHealthy returns false if replication to the secondary has recently failed, which triggers degraded write warnings.
func (m *Manager) IsSecondaryHealthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.role != RolePrimary {
		return true // not relevant for secondaries
	}

	// Consider unhealthy if 3+ consecutive failures or recent failure within 5s
	if m.replicationFailCount >= 3 {
		return false
	}
	if !m.lastReplicationFail.IsZero() && m.now().Sub(m.lastReplicationFail) < 5*time.Second {
		return false
	}
	return true
}

func (m *Manager) sendReplicationMessage(address string, msg *ReplicationMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	url := fmt.Sprintf("http://%s/internal/replicate", address)
	req, err := http.NewRequestWithContext(m.ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	m.setInternalAuth(req)

	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return nil
}

func (m *Manager) heartbeatLoop() {
	ticker := time.NewTicker(m.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			if m.Role() == RolePrimary {
				m.sendHeartbeats()
			}
		}
	}
}

func (m *Manager) sendHeartbeats() {
	hosts := m.hasher.GetAllHosts()
	hb := &HeartbeatMessage{
		HostID:      m.config.HostID,
		Site:        m.config.Site,        // Include site for cluster membership validation
		Address:     m.config.TCPAddress,  // Include our TCP address for forwarding
		LeaderEpoch: m.LeaderEpoch(),
		StoreCount:  m.store.Count(),
		MemoryUsage: m.store.MemoryUsage(),
		Timestamp:   m.now(),
	}

	for _, host := range hosts {
		if host.ID == m.config.HostID {
			continue
		}
		go m.sendHeartbeat(host.Address, hb)
	}
}

func (m *Manager) sendHeartbeat(address string, hb *HeartbeatMessage) {
	body, err := json.Marshal(hb)
	if err != nil {
		return
	}

	url := fmt.Sprintf("http://%s/internal/heartbeat", address)
	req, err := http.NewRequestWithContext(m.ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	m.setInternalAuth(req)

	resp, err := m.client.Do(req)
	if err != nil {
		return
	}
	defer func() { _ = resp.Body.Close() }()
}

// HandleHeartbeat processes an incoming heartbeat from another node and may trigger role changes or recovery if a higher epoch is seen.
// Returns ErrSiteMismatch if the peer's site doesn't match ours (unless site verification is disabled).
func (m *Manager) HandleHeartbeat(hb *HeartbeatMessage) (*HeartbeatAck, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate site match - reject heartbeats from peers in different sites
	// Skip this check if site verification is disabled
	if !m.config.DisableSiteVerification && hb.Site != "" && hb.Site != m.config.Site {
		log.Printf("rejecting heartbeat from %s: site mismatch (theirs=%q, ours=%q)", hb.HostID, hb.Site, m.config.Site)
		return nil, ErrSiteMismatch
	}

	m.peerLastSeen[hb.HostID] = m.now()

	if hb.LeaderEpoch >= m.leaderEpoch {
		m.lastLeaderSeen = m.now()
		// Update leader address from heartbeat (primary advertises its address)
		if hb.Address != "" {
			m.leaderAddress = hb.Address
		}
		if hb.LeaderEpoch > m.leaderEpoch {
			m.leaderEpoch = hb.LeaderEpoch
			switch m.role {
			case RolePrimary:
				log.Printf("demoting to JOINING: saw higher epoch %d from %s", hb.LeaderEpoch, hb.HostID)
				m.role = RoleJoining
				m.lastPromotionAt = time.Time{}
				m.lastRecoveryAttempt = m.now()
				go m.startRecoveryFromPeer(hb.HostID)
			case RoleJoining:
				// Already JOINING - retry recovery if enough time has passed
				if m.now().Sub(m.lastRecoveryAttempt) >= recoveryRetryInterval {
					log.Printf("retrying recovery from %s (epoch %d)", hb.HostID, hb.LeaderEpoch)
					m.lastRecoveryAttempt = m.now()
					go m.startRecoveryFromPeer(hb.HostID)
				}
			default:
				log.Printf("entering JOINING: saw higher epoch %d from %s", hb.LeaderEpoch, hb.HostID)
				m.role = RoleJoining
				m.lastRecoveryAttempt = m.now()
				go m.startRecoveryFromPeer(hb.HostID)
			}
		} else if m.role == RoleJoining && m.now().Sub(m.lastRecoveryAttempt) >= recoveryRetryInterval {
			// Same epoch but still JOINING - retry recovery from this peer
			log.Printf("retrying recovery from %s (same epoch %d)", hb.HostID, hb.LeaderEpoch)
			m.lastRecoveryAttempt = m.now()
			go m.startRecoveryFromPeer(hb.HostID)
		}
	}

	return &HeartbeatAck{
		HostID:         m.config.HostID,
		Site:           m.config.Site,
		LeaderEpoch:    m.leaderEpoch,
		LastSeenLeader: m.lastLeaderSeen,
		Timestamp:      m.now(),
	}, nil
}

func (m *Manager) leaseCheckLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkLeaseAndPromote()
		}
	}
}

func (m *Manager) checkLeaseAndPromote() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.role != RoleSecondary {
		return
	}

	leaseExpiry := m.config.LeaseDuration + m.config.LeaseGrace
	if m.now().Sub(m.lastLeaderSeen) > leaseExpiry {
		log.Printf("lease expired, promoting to primary (epoch %d -> %d)", m.leaderEpoch, m.leaderEpoch+1)
		m.leaderEpoch++
		m.role = RolePrimary
		m.lastLeaderSeen = m.now()
		m.lastPromotionAt = m.now()
	}
}

func (m *Manager) tombstoneGCLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.cleanupTombstones()
		}
	}
}

func (m *Manager) cleanupTombstones() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	for storeID, ts := range m.tombstones {
		if now.Sub(ts) > m.tombstoneTTL {
			delete(m.tombstones, storeID)
			// Remove from customer index
			for customerID, storeIDs := range m.tombstoneCustomerIndex {
				if _, exists := storeIDs[storeID]; exists {
					delete(storeIDs, storeID)
					if len(storeIDs) == 0 {
						delete(m.tombstoneCustomerIndex, customerID)
					}
					break
				}
			}
		}
	}
}

// AddTombstone adds a tombstone for a deleted store.
// Returns ErrTombstoneLimitExceeded if per-customer or global limits are exceeded.
// This method should be called from primary delete handlers (enforces limits).
func (m *Manager) AddTombstone(storeID, customerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check global limit
	if m.config.TombstoneGlobalLimit > 0 && len(m.tombstones) >= m.config.TombstoneGlobalLimit {
		return ErrTombstoneLimitExceeded
	}

	// Check per-customer limit
	if m.config.TombstonePerCustomerLimit > 0 {
		if len(m.tombstoneCustomerIndex[customerID]) >= m.config.TombstonePerCustomerLimit {
			return ErrTombstoneLimitExceeded
		}
	}

	// Add tombstone
	m.tombstones[storeID] = m.now()
	if m.tombstoneCustomerIndex[customerID] == nil {
		m.tombstoneCustomerIndex[customerID] = make(map[string]struct{})
	}
	m.tombstoneCustomerIndex[customerID][storeID] = struct{}{}
	return nil
}

// AddTombstoneReplicated adds a tombstone without limit checks.
// This method should be called from replication and cleanup paths (secondary must mirror primary).
func (m *Manager) AddTombstoneReplicated(storeID, customerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tombstones[storeID] = m.now()
	if m.tombstoneCustomerIndex[customerID] == nil {
		m.tombstoneCustomerIndex[customerID] = make(map[string]struct{})
	}
	m.tombstoneCustomerIndex[customerID][storeID] = struct{}{}
}

// IsTombstoned returns true if the store was recently deleted and should not be resurrected.
func (m *Manager) IsTombstoned(storeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.tombstones[storeID]
	return exists
}

// TombstonesSnapshot returns a consistent view of all tombstones.
func (m *Manager) TombstonesSnapshot() []TombstoneEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]TombstoneEntry, 0, len(m.tombstones))
	for storeID, deletedAt := range m.tombstones {
		entries = append(entries, TombstoneEntry{
			StoreID:   storeID,
			DeletedAt: deletedAt,
		})
	}
	return entries
}

// ResetTombstones replaces all tombstones with the provided snapshot.
func (m *Manager) ResetTombstones(entries []TombstoneEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetTombstonesLocked(entries)
}

// resetTombstonesLocked replaces tombstones assuming m.mu is already held.
// Note: Customer index is cleared during reset since TombstoneEntry doesn't contain customerID.
// This is acceptable because limit enforcement only applies to primary nodes.
func (m *Manager) resetTombstonesLocked(entries []TombstoneEntry) {
	m.tombstones = make(map[string]time.Time)
	m.tombstoneCustomerIndex = make(map[string]map[string]struct{})
	for _, e := range entries {
		m.tombstones[e.StoreID] = e.DeletedAt
	}
}

// ApplyReplication processes an incoming replication message from the primary and updates local state accordingly.
func (m *Manager) ApplyReplication(msg *ReplicationMessage) error {
	if m.Role() == RoleJoining {
		return ErrJoinInProgress
	}

	if msg.LeaderEpoch < m.LeaderEpoch() {
		return fmt.Errorf("stale epoch: %d < %d", msg.LeaderEpoch, m.LeaderEpoch())
	}

	switch msg.Type {
	case MsgCreateStore:
		s := &store.Store{
			ID:               msg.StoreID,
			ShardID:          msg.ShardID,
			CustomerID:       msg.CustomerID,
			DataType:         store.DataType(msg.DataType),
			Body:             msg.Body,
			ExpiresAt:        msg.ExpiresAt,
			Version:          msg.Version,
			LeaderEpoch:      msg.LeaderEpoch,
			Role:             store.RoleSecondary,
			PendingName:      msg.PendingName,
			LastReplicatedAt: m.now(),
		}
		// Idempotent: creates if not exists, updates if version is newer
		return m.store.CreateOrUpdate(s)

	case MsgUpdateStore:
		// Merge update into existing store to preserve fields not in message
		update := &store.ReplicatedUpdate{
			StoreID:          msg.StoreID,
			CustomerID:       msg.CustomerID,
			DataType:         store.DataType(msg.DataType),
			Body:             msg.Body,
			ExpiresAt:        msg.ExpiresAt,
			Version:          msg.Version,
			LeaderEpoch:      msg.LeaderEpoch,
			LastReplicatedAt: m.now(),
		}
		err := m.store.ApplyReplicatedUpdate(update)
		if err == store.ErrStoreNotFound {
			// Store doesn't exist yet (out-of-order message), create it
			s := &store.Store{
				ID:               msg.StoreID,
				ShardID:          msg.ShardID,
				CustomerID:       msg.CustomerID,
				DataType:         store.DataType(msg.DataType),
				Body:             msg.Body,
				ExpiresAt:        msg.ExpiresAt,
				Version:          msg.Version,
				LeaderEpoch:      msg.LeaderEpoch,
				Role:             store.RoleSecondary,
				LastReplicatedAt: m.now(),
			}
			return m.store.CreateIfNotExists(s)
		}
		// Update means complete-modify succeeded, clear lock unconditionally
		m.store.ClearLockUnconditionally(msg.StoreID)
		return err

	case MsgDeleteStore:
		m.AddTombstoneReplicated(msg.StoreID, msg.CustomerID)
		err := m.store.Delete(msg.StoreID, msg.CustomerID)
		if err == store.ErrStoreNotFound {
			// Already deleted, idempotent
			return nil
		}
		return err

	case MsgLockAcquired:
		// Apply lock state on secondary for failover handling
		if msg.LockID != "" && msg.StoreID != "" {
			_ = m.store.SetLock(msg.StoreID, msg.LockID, msg.Timestamp, msg.LockTimeout)
		}
		return nil

	case MsgLockReleased:
		// Clear lock on secondary
		if msg.LockID != "" && msg.StoreID != "" {
			_ = m.store.ClearLock(msg.StoreID, msg.LockID)
		}
		return nil

	default:
		return fmt.Errorf("unknown message type: %v", msg.Type)
	}
}

// GetStatus returns a map of current cluster state useful for monitoring and debugging.
func (m *Manager) GetStatus() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := m.now()
	peers := make(map[string]string)
	for hostID, lastSeen := range m.peerLastSeen {
		peers[hostID] = now.Sub(lastSeen).String()
	}

	registryCounts := make(map[string]int)
	if m.registry != nil {
		for state, count := range m.registry.CountByState() {
			registryCounts[state.String()] = count
		}
	}

	var lastReplicationFailAgo string
	if !m.lastReplicationFail.IsZero() {
		lastReplicationFailAgo = now.Sub(m.lastReplicationFail).String()
	}

	return map[string]any{
		"host_id":                m.config.HostID,
		"role":                   m.role.String(),
		"leader_epoch":           m.leaderEpoch,
		"last_leader_seen":       now.Sub(m.lastLeaderSeen).String(),
		"store_count":            m.store.Count(),
		"memory_usage":           m.store.MemoryUsage(),
		"peers":                  peers,
		"tombstone_count":        len(m.tombstones),
		"queue_length":           len(m.replicationQueue),
		"registry_queue_length":  len(m.registryQueue),
		"replication_fail_count": m.replicationFailCount,
		"last_replication_fail":  lastReplicationFailAgo,
		"registry_count":         m.registryCount(),
		"registry_states":        registryCounts,
	}
}

func (m *Manager) registryCount() int {
	if m.registry == nil {
		return 0
	}
	return m.registry.Count()
}

func (m *Manager) registryReplicationLoop() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case msg := <-m.registryQueue:
			m.replicateRegistryToSecondary(msg)
		}
	}
}

func (m *Manager) replicateRegistryToSecondary(msg *RegistryReplicationMessage) {
	shardKey := msg.CustomerID + ":" + msg.Name
	targets := m.getReplicationTargets(shardKey)

	for _, host := range targets {
		if err := m.sendRegistryReplicationMessage(host.Address, msg); err != nil {
			log.Printf("failed to replicate registry to secondary %s: %v", host.ID, err)
		}
	}
}

func (m *Manager) sendRegistryReplicationMessage(address string, msg *RegistryReplicationMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	url := fmt.Sprintf("http://%s/internal/replicate-registry", address)
	req, err := http.NewRequestWithContext(m.ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	m.setInternalAuth(req)

	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return nil
}

// QueueRegistryReplication adds a registry message to the async replication queue.
func (m *Manager) QueueRegistryReplication(msg *RegistryReplicationMessage) {
	msg.SourceHost = m.config.HostID
	msg.Timestamp = m.now()
	msg.LeaderEpoch = m.LeaderEpoch()

	select {
	case m.registryQueue <- msg:
	default:
		log.Printf("registry replication queue full, dropping message: %s for %s:%s", msg.Type, msg.CustomerID, msg.Name)
	}
}

// ApplyRegistryReplication processes an incoming registry replication message and updates the local registry.
func (m *Manager) ApplyRegistryReplication(msg *RegistryReplicationMessage) error {
	if m.Role() == RoleJoining {
		return ErrJoinInProgress
	}

	if m.registry == nil {
		return fmt.Errorf("registry not initialized")
	}

	if msg.LeaderEpoch < m.LeaderEpoch() {
		return fmt.Errorf("stale epoch: %d < %d", msg.LeaderEpoch, m.LeaderEpoch())
	}

	switch msg.Type {
	case MsgRegistryReserve, MsgRegistryCommit:
		entry := &registry.Entry{
			CustomerID:    msg.CustomerID,
			Name:          msg.Name,
			StoreID:       msg.StoreID,
			State:         registry.EntryState(msg.State),
			ExpiresAt:     msg.ExpiresAt,
			ReservationID: msg.ReservationID,
			ReservedAt:    msg.Timestamp,
			LeaderEpoch:   msg.LeaderEpoch,
			Version:       msg.Version,
		}
		return m.registry.ApplyReplicatedEntry(entry)

	case MsgRegistryAbort, MsgRegistryDelete:
		return m.registry.ApplyReplicatedDelete(msg.CustomerID, msg.Name)

	default:
		return fmt.Errorf("unknown registry message type: %v", msg.Type)
	}
}

// Registry returns the attached registry manager, or nil if none is set.
func (m *Manager) Registry() *registry.Manager {
	return m.registry
}

func (m *Manager) orphanGCLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			if m.Role() == RolePrimary {
				// Try to acquire barrier - skip if join in progress
				release, ok := m.TryAcquireWriteBarrier()
				if !ok {
					// Join in progress, skip this tick
					continue
				}
				m.cleanupOrphans()
				m.cleanupExpiredReservations()
				m.cleanupStuckDeletingEntries()
				release()
			}
		}
	}
}

func (m *Manager) cleanupOrphans() {
	orphans := m.store.GetOrphanedStores(registry.ReservationTTL * 2)

	for _, orphan := range orphans {
		if m.registry != nil {
			entry, err := m.registry.Lookup(orphan.CustomerID, orphan.PendingName)
			if err == nil && entry.State == registry.StateActive && entry.StoreID == orphan.ID {
				continue
			}
		}

		log.Printf("cleaning up orphaned store %s (pending_name=%s)", orphan.ID, orphan.PendingName)

		if err := m.store.ForceDelete(orphan.ID); err != nil {
			log.Printf("failed to delete orphan store %s: %v", orphan.ID, err)
			continue
		}

		m.AddTombstoneReplicated(orphan.ID, orphan.CustomerID)
		m.QueueReplication(&ReplicationMessage{
			Type:       MsgDeleteStore,
			StoreID:    orphan.ID,
			ShardID:    orphan.ShardID,
			CustomerID: orphan.CustomerID,
			Tombstone:  true,
		})
	}
}

func (m *Manager) cleanupExpiredReservations() {
	if m.registry == nil {
		return
	}

	expired := m.registry.GetExpiredReservations()

	for _, entry := range expired {
		log.Printf("cleaning up expired reservation for %s:%s", entry.CustomerID, entry.Name)

		if err := m.registry.Abort(entry.CustomerID, entry.Name, entry.ReservationID); err != nil {
			continue
		}

		m.QueueRegistryReplication(&RegistryReplicationMessage{
			Type:          MsgRegistryAbort,
			CustomerID:    entry.CustomerID,
			Name:          entry.Name,
			ReservationID: entry.ReservationID,
		})
	}
}

func (m *Manager) cleanupStuckDeletingEntries() {
	if m.registry == nil {
		return
	}

	deleting := m.registry.GetDeletingEntries()

	for _, entry := range deleting {
		_, err := m.store.Get(entry.StoreID, entry.CustomerID)
		if err == nil {
			continue
		}

		log.Printf("cleaning up stuck deleting entry %s:%s (store %s already deleted)", entry.CustomerID, entry.Name, entry.StoreID)

		if err := m.registry.Delete(entry.CustomerID, entry.Name); err != nil {
			continue
		}

		m.QueueRegistryReplication(&RegistryReplicationMessage{
			Type:       MsgRegistryDelete,
			CustomerID: entry.CustomerID,
			Name:       entry.Name,
		})
	}
}

func (m *Manager) expiryGCLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			if m.Role() == RolePrimary {
				// Try to acquire barrier - skip if join in progress
				release, ok := m.TryAcquireWriteBarrier()
				if !ok {
					// Join in progress, skip this tick
					continue
				}
				m.cleanupExpiredStores()
				release()
			}
		}
	}
}

func (m *Manager) cleanupExpiredStores() {
	// Get snapshot of potentially expired stores
	expired := m.store.GetExpiredStores()

	for _, s := range expired {
		// Atomically delete only if still expired and not locked.
		// This prevents race with mid-modify operations that may extend TTL.
		if !m.store.DeleteIfExpiredAndUnlocked(s.ID) {
			// Store was either already deleted, TTL extended, or is locked - skip
			continue
		}

		log.Printf("cleaned up expired store %s (customer=%s)", s.ID, s.CustomerID)

		// Clean up any registry entry pointing to this store
		if m.registry != nil {
			if entry, err := m.registry.LookupByStoreID(s.ID); err == nil {
				log.Printf("removing registry entry %s:%s for expired store", entry.CustomerID, entry.Name)
				if err := m.registry.Delete(entry.CustomerID, entry.Name); err == nil {
					m.QueueRegistryReplication(&RegistryReplicationMessage{
						Type:       MsgRegistryDelete,
						CustomerID: entry.CustomerID,
						Name:       entry.Name,
					})
				}
			}
		}

		m.AddTombstoneReplicated(s.ID, s.CustomerID)
		m.QueueReplication(&ReplicationMessage{
			Type:       MsgDeleteStore,
			StoreID:    s.ID,
			ShardID:    s.ShardID,
			CustomerID: s.CustomerID,
			Tombstone:  true,
		})
	}
}

// StartRecovery initiates recovery by discovering the primary and fetching snapshots.
// This is used on startup when this node is not the primary.
func (m *Manager) StartRecovery() {
	m.mu.Lock()
	m.role = RoleJoining
	// Don't set lastRecoveryAttempt here - only set it when actually attempting recovery.
	// This allows the first heartbeat to trigger recovery immediately.
	m.mu.Unlock()

	// Find primary by probing /status on all peers
	primaryAddr := m.discoverPrimary()
	if primaryAddr == "" {
		log.Printf("recovery: no primary found, will wait for heartbeat")
		return
	}

	m.mu.Lock()
	m.lastRecoveryAttempt = m.now()
	m.mu.Unlock()

	m.doRecovery(primaryAddr)
}

// startRecoveryFromPeer initiates recovery from a known peer (the heartbeat sender).
func (m *Manager) startRecoveryFromPeer(peerHostID string) {
	// Find peer address
	hosts := m.hasher.GetAllHosts()
	var peerAddr string
	for _, h := range hosts {
		if h.ID == peerHostID {
			peerAddr = h.Address
			break
		}
	}

	if peerAddr == "" {
		log.Printf("recovery: peer %s not found in cluster", peerHostID)
		return
	}

	m.doRecovery(peerAddr)
}

// discoverPrimary probes /status on all peers to find the current primary.
func (m *Manager) discoverPrimary() string {
	hosts := m.hasher.GetAllHosts()

	for _, host := range hosts {
		if host.ID == m.config.HostID {
			continue
		}

		url := fmt.Sprintf("http://%s/status", host.Address)
		req, err := http.NewRequestWithContext(m.ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}

		resp, err := m.client.Do(req)
		if err != nil {
			continue
		}

		var status struct {
			Role string `json:"role"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			_ = resp.Body.Close()
			continue
		}
		_ = resp.Body.Close()

		if status.Role == "PRIMARY" {
			return host.Address
		}
	}

	return ""
}

// doRecovery fetches snapshots from the primary and resets local state.
func (m *Manager) doRecovery(primaryAddr string) {
	// Increment recovery attempt counter (for testing/metrics)
	m.mu.Lock()
	m.recoveryAttempts++
	m.mu.Unlock()

	log.Printf("recovery: starting recovery from %s", primaryAddr)

	// Fetch combined snapshot
	joinSnapshot, err := m.fetchJoinSnapshot(primaryAddr)
	if err != nil {
		log.Printf("recovery: failed to fetch join snapshot: %v", err)
		m.recoveryFailed()
		return
	}

	// Validate site (matches existing pattern: !DisableSiteVerification)
	if !m.config.DisableSiteVerification && joinSnapshot.Site != "" && joinSnapshot.Site != m.config.Site {
		log.Printf("recovery: rejecting snapshot: site mismatch (theirs=%q, ours=%q)",
			joinSnapshot.Site, m.config.Site)
		m.recoveryFailed()
		return
	}

	// Early rejection of obviously stale snapshots (unlocked check)
	currentEpoch := m.LeaderEpoch()
	if joinSnapshot.LeaderEpoch < currentEpoch {
		log.Printf("recovery: rejecting stale snapshot (epoch %d < %d)",
			joinSnapshot.LeaderEpoch, currentEpoch)
		m.recoveryFailed()
		return
	}

	// Apply reset under lock to prevent race with epoch updates from heartbeats
	m.mu.Lock()
	defer m.mu.Unlock()

	// Re-check epoch under lock - abort if epoch advanced beyond snapshot during fetch
	// CRITICAL: prevents applying stale snapshot if heartbeat updated epoch mid-fetch
	if joinSnapshot.LeaderEpoch < m.leaderEpoch {
		log.Printf("recovery: aborting - epoch advanced during fetch (snapshot %d < current %d)",
			joinSnapshot.LeaderEpoch, m.leaderEpoch)
		return
	}

	// Validate registry configuration match (both directions)
	if m.registry != nil && !joinSnapshot.HasRegistry {
		log.Printf("recovery: aborting - secondary has registry but primary does not")
		return
	}
	if m.registry == nil && joinSnapshot.HasRegistry {
		log.Printf("recovery: aborting - primary has registry but secondary does not")
		return
	}

	// Validate registry snapshot before applying any changes
	if m.registry != nil {
		if err := registry.ValidateSnapshot(joinSnapshot.Registry); err != nil {
			log.Printf("recovery: invalid registry snapshot: %v", err)
			return
		}
	}

	// Reset local state (under lock to ensure atomicity with epoch check)
	m.store.Reset(joinSnapshot.Stores)
	log.Printf("recovery: reset store with %d stores", len(joinSnapshot.Stores))

	if m.registry != nil {
		if err := m.registry.Reset(joinSnapshot.Registry); err != nil {
			log.Printf("recovery: failed to reset registry: %v", err)
			return
		}
		log.Printf("recovery: reset registry with %d entries", len(joinSnapshot.Registry))
	}

	// Apply tombstones: delete stores that were deleted on primary
	m.resetTombstonesLocked(joinSnapshot.Tombstones)
	for _, ts := range joinSnapshot.Tombstones {
		_ = m.store.ForceDelete(ts.StoreID)
	}
	log.Printf("recovery: applied %d tombstones", len(joinSnapshot.Tombstones))

	// Update epoch and transition to secondary
	// CRITICAL: Update lastLeaderSeen to prevent immediate promotion
	if joinSnapshot.LeaderEpoch >= m.leaderEpoch {
		m.leaderEpoch = joinSnapshot.LeaderEpoch
	}
	m.lastLeaderSeen = m.now() // Prevents immediate promotion!
	m.role = RoleSecondary

	log.Printf("recovery: complete, now SECONDARY at epoch %d", m.leaderEpoch)
}

// recoveryFailed is called when recovery fails, keeping us in JOINING state.
func (m *Manager) recoveryFailed() {
	// Stay in JOINING state; next heartbeat will retry recovery
	log.Printf("recovery: failed, staying in JOINING state")
}

// fetchJoinSnapshot fetches the combined join snapshot from the primary.
func (m *Manager) fetchJoinSnapshot(primaryAddr string) (*JoinSnapshotData, error) {
	url := fmt.Sprintf("http://%s/internal/join-snapshot", primaryAddr)
	req, err := http.NewRequestWithContext(m.ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-Internal-Token", m.config.InternalToken)

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusServiceUnavailable {
		// Another join in progress, will retry on next heartbeat
		return nil, fmt.Errorf("join in progress on primary")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var snapshot JoinSnapshotData
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &snapshot, nil
}
