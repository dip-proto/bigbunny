package replica

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/dip-proto/bigbunny/internal/registry"
	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
)

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

	// Testing hooks (optional)
	HTTPClient           *http.Client     // custom client for network simulation; nil = default
	Now                  func() time.Time // custom clock for deterministic tests; nil = time.Now
	BroadcastReplication bool             // if true, replicate to all hosts instead of per-shard (test-only)
}

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

	tombstones   map[string]time.Time // storeID -> tombstone time
	tombstoneTTL time.Duration

	lastPromotionAt      time.Time // when this node became primary (for lock unknown window)
	lastReplicationFail  time.Time // when replication last failed
	replicationFailCount int       // consecutive failures
	lastRecoveryAttempt  time.Time // when recovery was last attempted (for retry debounce)
	recoveryAttempts     int       // total recovery attempts (for testing/metrics)

	ctx    context.Context
	cancel context.CancelFunc
}

const recoveryRetryInterval = time.Second // minimum time between recovery attempts

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
		config:           config,
		store:            storeMgr,
		hasher:           hasher,
		client:           client,
		now:              now,
		role:             RoleUnknown,
		leaderEpoch:      0,
		peerLastSeen:     make(map[string]time.Time),
		replicationQueue: make(chan *ReplicationMessage, 10000),
		registryQueue:    make(chan *RegistryReplicationMessage, 10000),
		tombstones:       make(map[string]time.Time),
		tombstoneTTL:     24 * time.Hour,
		ctx:              ctx,
		cancel:           cancel,
	}

	return m
}

func (m *Manager) SetRegistry(reg *registry.Manager) {
	m.registry = reg
}

func (m *Manager) GetClient() *http.Client {
	return m.client
}

func (m *Manager) setInternalAuth(req *http.Request) {
	if m.config.InternalToken != "" {
		req.Header.Set("X-Internal-Token", m.config.InternalToken)
	}
}

func (m *Manager) Start() {
	go m.replicationLoop()
	go m.registryReplicationLoop()
	go m.heartbeatLoop()
	go m.leaseCheckLoop()
	go m.tombstoneGCLoop()
	go m.orphanGCLoop()
	go m.expiryGCLoop()
}

func (m *Manager) Stop() {
	m.cancel()
}

func (m *Manager) Role() Role {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.role
}

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
	sortedHosts := make([]*routing.Host, len(hosts))
	copy(sortedHosts, hosts)
	sortHostsByID(sortedHosts)

	return sortedHosts[0].Address
}

// sortHostsByID sorts hosts by ID in lexicographic order (deterministic)
func sortHostsByID(hosts []*routing.Host) {
	for i := 0; i < len(hosts)-1; i++ {
		for j := i + 1; j < len(hosts); j++ {
			if hosts[j].ID < hosts[i].ID {
				hosts[i], hosts[j] = hosts[j], hosts[i]
			}
		}
	}
}

// RecoveryAttempts returns the total number of recovery attempts (for testing/metrics).
func (m *Manager) RecoveryAttempts() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.recoveryAttempts
}

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
	}
}

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

func (m *Manager) replicateToSecondary(msg *ReplicationMessage) {
	if msg.StoreID == "" {
		return
	}

	// Determine which hosts to replicate to
	var targets []*routing.Host
	if m.config.BroadcastReplication {
		// Test mode: replicate to all other hosts
		for _, host := range m.hasher.GetAllHosts() {
			if host.ID != m.config.HostID {
				targets = append(targets, host)
			}
		}
	} else {
		// Production mode: use per-shard routing
		// ShardID is carried in the message (required for encrypted store IDs)
		if msg.ShardID == "" {
			log.Printf("replication message missing ShardID for store %s", msg.StoreID)
			return
		}
		replicaSet := m.hasher.GetReplicaSet(msg.ShardID)
		if replicaSet != nil {
			if replicaSet.Primary != nil && replicaSet.Primary.ID != m.config.HostID {
				targets = append(targets, replicaSet.Primary)
			}
			if replicaSet.Secondary != nil && replicaSet.Secondary.ID != m.config.HostID {
				targets = append(targets, replicaSet.Secondary)
			}
		}
	}

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
		Address:     m.config.TCPAddress, // Include our TCP address for forwarding
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

func (m *Manager) HandleHeartbeat(hb *HeartbeatMessage) *HeartbeatAck {
	m.mu.Lock()
	defer m.mu.Unlock()

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
		LeaderEpoch:    m.leaderEpoch,
		LastSeenLeader: m.lastLeaderSeen,
		Timestamp:      m.now(),
	}
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
		}
	}
}

func (m *Manager) AddTombstone(storeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tombstones[storeID] = m.now()
}

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
func (m *Manager) resetTombstonesLocked(entries []TombstoneEntry) {
	m.tombstones = make(map[string]time.Time)
	for _, e := range entries {
		m.tombstones[e.StoreID] = e.DeletedAt
	}
}

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
		m.AddTombstone(msg.StoreID)
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
	// Determine which hosts to replicate to
	var targets []*routing.Host
	if m.config.BroadcastReplication {
		// Test mode: replicate to all other hosts
		for _, host := range m.hasher.GetAllHosts() {
			if host.ID != m.config.HostID {
				targets = append(targets, host)
			}
		}
	} else {
		// Production mode: use per-shard routing with customer_id:name as shard key
		shardKey := msg.CustomerID + ":" + msg.Name
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
				m.cleanupOrphans()
				m.cleanupExpiredReservations()
				m.cleanupStuckDeletingEntries()
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

		m.AddTombstone(orphan.ID)
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
				m.cleanupExpiredStores()
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

		m.AddTombstone(s.ID)
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

	// Fetch store snapshot
	storeSnapshot, err := m.fetchStoreSnapshot(primaryAddr)
	if err != nil {
		log.Printf("recovery: failed to fetch store snapshot: %v", err)
		m.recoveryFailed()
		return
	}

	// Early rejection of obviously stale snapshots
	currentEpoch := m.LeaderEpoch()
	if storeSnapshot.LeaderEpoch < currentEpoch {
		log.Printf("recovery: rejecting stale snapshot (epoch %d < %d)", storeSnapshot.LeaderEpoch, currentEpoch)
		m.recoveryFailed()
		return
	}

	// Fetch registry snapshot
	registrySnapshot, err := m.fetchRegistrySnapshot(primaryAddr)
	if err != nil {
		log.Printf("recovery: failed to fetch registry snapshot: %v", err)
		m.recoveryFailed()
		return
	}

	// Apply reset under lock to prevent race with epoch updates from heartbeats
	m.mu.Lock()
	defer m.mu.Unlock()

	// Re-check epoch under lock - abort if epoch advanced beyond snapshot during fetch
	if storeSnapshot.LeaderEpoch < m.leaderEpoch {
		log.Printf("recovery: aborting - epoch advanced during fetch (snapshot %d < current %d)",
			storeSnapshot.LeaderEpoch, m.leaderEpoch)
		return
	}

	// Reset local state (under lock to ensure atomicity with epoch check)
	m.store.Reset(storeSnapshot.Stores)
	log.Printf("recovery: reset store with %d stores", len(storeSnapshot.Stores))

	if m.registry != nil {
		m.registry.Reset(registrySnapshot.Entries)
		log.Printf("recovery: reset registry with %d entries", len(registrySnapshot.Entries))
	}

	// Apply tombstones: delete stores that were deleted on primary
	m.resetTombstonesLocked(storeSnapshot.Tombstones)
	for _, ts := range storeSnapshot.Tombstones {
		_ = m.store.ForceDelete(ts.StoreID)
	}
	log.Printf("recovery: applied %d tombstones", len(storeSnapshot.Tombstones))

	// Update epoch (only if >= current) and transition to secondary
	if storeSnapshot.LeaderEpoch >= m.leaderEpoch {
		m.leaderEpoch = storeSnapshot.LeaderEpoch
	}
	m.lastLeaderSeen = m.now()
	m.role = RoleSecondary

	log.Printf("recovery: complete, now SECONDARY at epoch %d", m.leaderEpoch)
}

// recoveryFailed is called when recovery fails, keeping us in JOINING state.
func (m *Manager) recoveryFailed() {
	// Stay in JOINING state; next heartbeat will retry recovery
	log.Printf("recovery: failed, staying in JOINING state")
}

// fetchStoreSnapshot fetches the store snapshot from the primary.
func (m *Manager) fetchStoreSnapshot(primaryAddr string) (*SnapshotData, error) {
	url := fmt.Sprintf("http://%s/internal/snapshot", primaryAddr)
	req, err := http.NewRequestWithContext(m.ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	m.setInternalAuth(req)

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var snapshot SnapshotData
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &snapshot, nil
}

// RegistrySnapshotResponse matches the API response structure.
type RegistrySnapshotResponse struct {
	Entries     []*registry.Entry `json:"entries"`
	LeaderEpoch uint64            `json:"leader_epoch"`
}

// fetchRegistrySnapshot fetches the registry snapshot from the primary.
func (m *Manager) fetchRegistrySnapshot(primaryAddr string) (*RegistrySnapshotResponse, error) {
	url := fmt.Sprintf("http://%s/internal/registry/snapshot", primaryAddr)
	req, err := http.NewRequestWithContext(m.ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	m.setInternalAuth(req)

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var snapshot RegistrySnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &snapshot, nil
}
