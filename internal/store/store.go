package store

import (
	"encoding/json"
	"math"
	"sync"
	"time"
)

type ReplicaRole int

const (
	RolePrimary ReplicaRole = iota
	RoleSecondary
)

type DataType uint8

const (
	DataTypeBlob DataType = iota
	DataTypeCounter
)

func (d DataType) String() string {
	switch d {
	case DataTypeBlob:
		return "blob"
	case DataTypeCounter:
		return "counter"
	default:
		return "unknown"
	}
}

func (r ReplicaRole) String() string {
	switch r {
	case RolePrimary:
		return "primary"
	case RoleSecondary:
		return "secondary"
	default:
		return "unknown"
	}
}

// LockState tracks an active modify lock on a store.
type LockState struct {
	LockID    string
	HeldSince time.Time
	Timeout   time.Duration
}

func (l *LockState) IsExpired() bool {
	if l == nil || l.LockID == "" {
		return true
	}
	return time.Since(l.HeldSince) > l.Timeout
}

// Store represents an in-memory session store with TTL and locking support.
type Store struct {
	ID               string
	ShardID          string // for replication routing (extracted from encrypted store ID)
	CustomerID       string
	DataType         DataType
	Body             []byte
	ExpiresAt        time.Time
	Version          uint64
	LeaderEpoch      uint64
	Lock             *LockState
	Role             ReplicaRole
	LastReplicatedAt time.Time
	PendingName      string // for orphan detection during named store creation
	CreatedAt        time.Time
}

func (s *Store) IsExpired() bool {
	return !s.ExpiresAt.IsZero() && time.Now().After(s.ExpiresAt)
}

// CounterData represents the data stored in a counter-type store.
// Min and Max define optional bounds. When nil, the counter is unbounded in that direction.
// Bounds are immutable after creation.
type CounterData struct {
	Value int64  `json:"value"`
	Min   *int64 `json:"min,omitempty"`
	Max   *int64 `json:"max,omitempty"`
}

// CounterResult is returned from increment operations.
type CounterResult struct {
	Value   int64  // New counter value after operation
	Version uint64 // New version number
	Bounded bool   // True if the operation was clamped by min/max bounds
	Min     *int64 // Minimum bound (nil if unbounded)
	Max     *int64 // Maximum bound (nil if unbounded)
	// Fields needed for replication
	ShardID     string
	ExpiresAt   time.Time
	Body        []byte // Serialized counter data
	LeaderEpoch uint64
}

const storeOverhead = 256 // estimated per-store metadata overhead in bytes

// Manager provides thread-safe in-memory storage with customer isolation,
// locking for atomic modifications, and memory tracking.
type Manager struct {
	mu     sync.RWMutex
	stores map[string]*Store // keyed by store ID

	customerIndex map[string]map[string]struct{} // customerID -> set of store IDs

	usedBytes   int64 // current memory usage
	memoryLimit int64 // max memory allowed (0 = no limit)
}

// NewManager creates a store manager with no memory limit.
func NewManager() *Manager {
	return &Manager{
		stores:        make(map[string]*Store),
		customerIndex: make(map[string]map[string]struct{}),
	}
}

// NewManagerWithLimit creates a store manager that rejects creates when
// memory usage would exceed the limit.
func NewManagerWithLimit(memoryLimit int64) *Manager {
	return &Manager{
		stores:        make(map[string]*Store),
		customerIndex: make(map[string]map[string]struct{}),
		memoryLimit:   memoryLimit,
	}
}

func (m *Manager) SetMemoryLimit(limit int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.memoryLimit = limit
}

func storeSize(s *Store) int64 {
	return int64(len(s.Body)) + storeOverhead
}

func (m *Manager) Create(s *Store) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.stores[s.ID]; exists {
		return ErrStoreExists
	}

	size := storeSize(s)
	if m.memoryLimit > 0 && m.usedBytes+size > m.memoryLimit {
		return ErrCapacityExceeded
	}

	s.CreatedAt = time.Now()
	s.Version = 1
	m.stores[s.ID] = s
	m.usedBytes += size

	if m.customerIndex[s.CustomerID] == nil {
		m.customerIndex[s.CustomerID] = make(map[string]struct{})
	}
	m.customerIndex[s.CustomerID][s.ID] = struct{}{}

	return nil
}

func (m *Manager) Get(storeID, customerID string) (*Store, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, exists := m.stores[storeID]
	if !exists {
		return nil, ErrStoreNotFound
	}
	if s.CustomerID != customerID {
		return nil, ErrUnauthorized
	}
	if s.IsExpired() {
		return nil, ErrStoreExpired
	}
	// Return a copy to avoid races with concurrent modifications
	return s.Copy(), nil
}

func (m *Manager) Update(s *Store) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.stores[s.ID]
	if !exists {
		return ErrStoreNotFound
	}
	if existing.CustomerID != s.CustomerID {
		return ErrUnauthorized
	}

	// Track memory change
	oldSize := storeSize(existing)
	newSize := storeSize(s)
	m.usedBytes += newSize - oldSize

	s.Version = existing.Version + 1
	m.stores[s.ID] = s
	return nil
}

// ReplicatedUpdate contains fields that can be updated via replication.
// Fields not included here are preserved from the existing store.
type ReplicatedUpdate struct {
	StoreID          string
	CustomerID       string
	DataType         DataType
	Body             []byte
	ExpiresAt        time.Time
	Version          uint64
	LeaderEpoch      uint64
	LastReplicatedAt time.Time
}

// ApplyReplicatedUpdate merges an update from replication into the existing store.
// It's idempotent: if the incoming version <= current version, it's a no-op.
// Preserves fields not in the update (CreatedAt, PendingName, Lock, Role).
// Note: This is used for replication, so capacity checks are skipped (secondary follows primary).
func (m *Manager) ApplyReplicatedUpdate(update *ReplicatedUpdate) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.stores[update.StoreID]
	if !exists {
		return ErrStoreNotFound
	}
	if existing.CustomerID != update.CustomerID {
		return ErrUnauthorized
	}

	// Idempotent: skip if we already have this or newer version
	if update.Version <= existing.Version {
		return nil
	}

	// Track size change
	oldBodyLen := len(existing.Body)
	newBodyLen := len(update.Body)
	m.usedBytes += int64(newBodyLen - oldBodyLen)

	// Merge: update only the replicated fields, preserve the rest
	existing.DataType = update.DataType
	existing.Body = update.Body
	existing.ExpiresAt = update.ExpiresAt
	existing.Version = update.Version
	existing.LeaderEpoch = update.LeaderEpoch
	existing.LastReplicatedAt = update.LastReplicatedAt
	// Preserve: ShardID, CreatedAt, PendingName, Lock, Role, CustomerID

	return nil
}

// CreateOrUpdate is idempotent: creates if not exists, updates if version is newer.
// Rejects updates with mismatched CustomerID.
// Note: This is used for replication, so capacity checks are skipped (secondary follows primary).
func (m *Manager) CreateOrUpdate(s *Store) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.stores[s.ID]
	if !exists {
		// Create
		size := storeSize(s)
		s.CreatedAt = time.Now()
		m.stores[s.ID] = s
		m.usedBytes += size
		if m.customerIndex[s.CustomerID] == nil {
			m.customerIndex[s.CustomerID] = make(map[string]struct{})
		}
		m.customerIndex[s.CustomerID][s.ID] = struct{}{}
		return nil
	}

	// Reject customer mismatch - cannot reassign ownership
	if existing.CustomerID != s.CustomerID {
		return ErrUnauthorized
	}

	// Update only if newer, preserve CreatedAt
	if s.Version > existing.Version {
		oldSize := storeSize(existing)
		newSize := storeSize(s)
		s.CreatedAt = existing.CreatedAt // preserve original creation time
		m.stores[s.ID] = s
		m.usedBytes += newSize - oldSize
	}
	return nil
}

// CreateIfNotExists creates a store only if it doesn't exist.
// Returns nil if store already exists (idempotent).
// Note: This is used for replication, so capacity checks are skipped (secondary follows primary).
func (m *Manager) CreateIfNotExists(s *Store) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.stores[s.ID]; exists {
		return nil // already exists, no-op
	}

	size := storeSize(s)
	s.CreatedAt = time.Now()
	m.stores[s.ID] = s
	m.usedBytes += size
	if m.customerIndex[s.CustomerID] == nil {
		m.customerIndex[s.CustomerID] = make(map[string]struct{})
	}
	m.customerIndex[s.CustomerID][s.ID] = struct{}{}
	return nil
}

func (m *Manager) Delete(storeID, customerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return ErrStoreNotFound
	}
	if s.CustomerID != customerID {
		return ErrUnauthorized
	}

	m.usedBytes -= storeSize(s)
	delete(m.stores, storeID)
	if idx := m.customerIndex[customerID]; idx != nil {
		delete(idx, storeID)
	}
	return nil
}

// AcquireLock attempts to lock a store for modification. Returns the current
// store contents on success. Fails if already locked by another holder.
func (m *Manager) AcquireLock(storeID, customerID, lockID string, timeout time.Duration) (*Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return nil, ErrStoreNotFound
	}
	if s.CustomerID != customerID {
		return nil, ErrUnauthorized
	}
	if s.IsExpired() {
		return nil, ErrStoreExpired
	}

	if s.Lock != nil && !s.Lock.IsExpired() {
		return nil, ErrStoreLocked
	}

	s.Lock = &LockState{
		LockID:    lockID,
		HeldSince: time.Now(),
		Timeout:   timeout,
	}
	// Return a copy to avoid races
	return s.Copy(), nil
}

func (m *Manager) ReleaseLock(storeID, customerID, lockID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return ErrStoreNotFound
	}
	if s.CustomerID != customerID {
		return ErrUnauthorized
	}
	if s.Lock == nil || s.Lock.LockID != lockID {
		return ErrLockMismatch
	}

	s.Lock = nil
	return nil
}

func (m *Manager) SetLock(storeID, lockID string, heldSince time.Time, timeout time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return ErrStoreNotFound
	}

	if s.Lock != nil && !s.Lock.IsExpired() && s.Lock.LockID != lockID {
		return nil
	}

	s.Lock = &LockState{
		LockID:    lockID,
		HeldSince: heldSince,
		Timeout:   timeout,
	}
	return nil
}

func (m *Manager) ClearLock(storeID, lockID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return ErrStoreNotFound
	}

	if s.Lock == nil || s.Lock.IsExpired() || s.Lock.LockID == lockID {
		s.Lock = nil
	}
	return nil
}

func (m *Manager) ClearLockUnconditionally(storeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, exists := m.stores[storeID]; exists {
		s.Lock = nil
	}
}

// ForceReleaseLock forcibly clears any lock on a store, returning an error if the store doesn't exist.
// This is for operational use when manual intervention is needed.
func (m *Manager) ForceReleaseLock(storeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return ErrStoreNotFound
	}
	s.Lock = nil
	return nil
}

// CompleteLock atomically updates a store and releases its lock.
// The lockID must match the current holder. Optionally updates TTL.
func (m *Manager) CompleteLock(storeID, customerID, lockID string, newBody []byte, newExpiresAt time.Time) (*Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return nil, ErrStoreNotFound
	}
	if s.CustomerID != customerID {
		return nil, ErrUnauthorized
	}
	if s.Lock == nil || s.Lock.LockID != lockID {
		return nil, ErrLockMismatch
	}

	oldSize := storeSize(s)
	newSize := int64(len(newBody)) + storeOverhead
	sizeDelta := newSize - oldSize

	if m.memoryLimit > 0 && sizeDelta > 0 && m.usedBytes+sizeDelta > m.memoryLimit {
		return nil, ErrCapacityExceeded
	}

	s.Body = newBody
	if !newExpiresAt.IsZero() {
		s.ExpiresAt = newExpiresAt
	}
	s.Version++
	s.Lock = nil
	m.usedBytes += sizeDelta

	// Return a copy to avoid races
	return s.Copy(), nil
}

func (s *Store) Copy() *Store {
	bodyCopy := make([]byte, len(s.Body))
	copy(bodyCopy, s.Body)

	cpy := &Store{
		ID:               s.ID,
		ShardID:          s.ShardID,
		CustomerID:       s.CustomerID,
		DataType:         s.DataType,
		Body:             bodyCopy,
		ExpiresAt:        s.ExpiresAt,
		Version:          s.Version,
		LeaderEpoch:      s.LeaderEpoch,
		Role:             s.Role,
		LastReplicatedAt: s.LastReplicatedAt,
		PendingName:      s.PendingName,
		CreatedAt:        s.CreatedAt,
	}
	if s.Lock != nil {
		cpy.Lock = &LockState{
			LockID:    s.Lock.LockID,
			HeldSince: s.Lock.HeldSince,
			Timeout:   s.Lock.Timeout,
		}
	}
	return cpy
}

func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.stores)
}

func (m *Manager) MemoryUsage() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.usedBytes
}

func (m *Manager) MemoryLimit() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.memoryLimit
}

func (m *Manager) GetOrphanedStores(maxAge time.Duration) []*Store {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var orphans []*Store
	now := time.Now()
	for _, s := range m.stores {
		if s.PendingName != "" && now.Sub(s.CreatedAt) > maxAge {
			orphans = append(orphans, s.Copy())
		}
	}
	return orphans
}

func (m *Manager) ClearPendingName(storeID, customerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return ErrStoreNotFound
	}
	if s.CustomerID != customerID {
		return ErrUnauthorized
	}

	s.PendingName = ""
	return nil
}

func (m *Manager) GetExpiredStores() []*Store {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var expired []*Store
	for _, s := range m.stores {
		if s.IsExpired() {
			expired = append(expired, s.Copy())
		}
	}
	return expired
}

func (m *Manager) ForceDelete(storeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return nil // idempotent
	}

	m.usedBytes -= storeSize(s)
	delete(m.stores, storeID)
	if idx := m.customerIndex[s.CustomerID]; idx != nil {
		delete(idx, storeID)
	}
	return nil
}

// DeleteIfExpiredAndUnlocked atomically deletes a store only if it's still expired
// and not currently locked. Returns true if deleted, false if skipped.
// Used by GC to avoid race with mid-modify operations.
func (m *Manager) DeleteIfExpiredAndUnlocked(storeID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return false // already gone
	}

	// Skip if not expired (TTL may have been extended)
	if !s.IsExpired() {
		return false
	}

	// Skip if locked (modify in progress)
	if s.Lock != nil && !s.Lock.IsExpired() {
		return false
	}

	m.usedBytes -= storeSize(s)
	delete(m.stores, storeID)
	if idx := m.customerIndex[s.CustomerID]; idx != nil {
		delete(idx, storeID)
	}
	return true
}

// Snapshot returns a consistent point-in-time view of all stores.
// Returns deep copies to avoid races.
func (m *Manager) Snapshot() []*Store {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stores := make([]*Store, 0, len(m.stores))
	for _, s := range m.stores {
		stores = append(stores, s.Copy())
	}
	return stores
}

// Reset replaces all stores with the provided snapshot.
// Rebuilds indexes and usedBytes; preserves memoryLimit.
// Does NOT enforce capacity (secondary must mirror primary).
// Sets Role=RoleSecondary on all stores.
func (m *Manager) Reset(stores []*Store) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clear existing state
	m.stores = make(map[string]*Store)
	m.customerIndex = make(map[string]map[string]struct{})
	m.usedBytes = 0

	// Rebuild from snapshot
	for _, s := range stores {
		s.Role = RoleSecondary
		m.stores[s.ID] = s
		m.usedBytes += storeSize(s)

		if m.customerIndex[s.CustomerID] == nil {
			m.customerIndex[s.CustomerID] = make(map[string]struct{})
		}
		m.customerIndex[s.CustomerID][s.ID] = struct{}{}
	}
}

// CreateCounter creates a new counter store with the given initial value and optional bounds.
// Validates that min <= max if both bounds are provided.
// Returns the created store on success.
func (m *Manager) CreateCounter(id, shardID, customerID string, initialValue int64, min, max *int64, expiresAt time.Time, leaderEpoch uint64) (*Store, error) {
	// Validate bounds
	if min != nil && max != nil && *min > *max {
		return nil, ErrInvalidBounds
	}
	if min != nil && initialValue < *min {
		return nil, ErrValueOutOfBounds
	}
	if max != nil && initialValue > *max {
		return nil, ErrValueOutOfBounds
	}

	// Serialize counter data
	counterData := CounterData{
		Value: initialValue,
		Min:   min,
		Max:   max,
	}
	body, err := json.Marshal(counterData)
	if err != nil {
		return nil, err
	}

	store := &Store{
		ID:          id,
		ShardID:     shardID,
		CustomerID:  customerID,
		DataType:    DataTypeCounter,
		Body:        body,
		ExpiresAt:   expiresAt,
		LeaderEpoch: leaderEpoch,
		Role:        RolePrimary,
	}

	if err := m.Create(store); err != nil {
		return nil, err
	}

	return store.Copy(), nil
}

// Increment atomically increments a counter by delta and returns the new value.
// This operation:
// - Validates the store is a counter type
// - Checks the store is not locked (fails with ErrStoreLocked)
// - Detects integer overflow/underflow (fails with ErrOverflow)
// - Clamps to bounds if present (sets Bounded=true in result)
// - Increments the version and queues replication
//
// The operation is atomic and thread-safe without exposing locks to the client.
func (m *Manager) Increment(storeID, customerID string, delta int64, leaderEpoch uint64) (*CounterResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return nil, ErrStoreNotFound
	}
	if s.CustomerID != customerID {
		return nil, ErrUnauthorized
	}
	if s.IsExpired() {
		return nil, ErrStoreExpired
	}
	if s.DataType != DataTypeCounter {
		return nil, ErrTypeMismatch
	}
	if s.Lock != nil && !s.Lock.IsExpired() {
		return nil, ErrStoreLocked
	}

	// Deserialize current counter data
	var counterData CounterData
	if err := json.Unmarshal(s.Body, &counterData); err != nil {
		return nil, err
	}

	// Check for overflow/underflow before applying delta
	if delta > 0 {
		if counterData.Value > math.MaxInt64-delta {
			return nil, ErrOverflow
		}
	} else if delta < 0 {
		if counterData.Value < math.MinInt64-delta {
			return nil, ErrOverflow
		}
	}

	// Apply delta
	newValue := counterData.Value + delta
	bounded := false

	// Apply bounds
	if counterData.Min != nil && newValue < *counterData.Min {
		newValue = *counterData.Min
		bounded = true
	}
	if counterData.Max != nil && newValue > *counterData.Max {
		newValue = *counterData.Max
		bounded = true
	}

	// Update counter data
	counterData.Value = newValue
	newBody, err := json.Marshal(counterData)
	if err != nil {
		return nil, err
	}

	// Update store
	oldSize := storeSize(s)
	s.Body = newBody
	s.Version++
	s.LeaderEpoch = leaderEpoch
	newSize := storeSize(s)
	m.usedBytes += newSize - oldSize

	return &CounterResult{
		Value:       newValue,
		Version:     s.Version,
		Bounded:     bounded,
		Min:         counterData.Min,
		Max:         counterData.Max,
		ShardID:     s.ShardID,
		ExpiresAt:   s.ExpiresAt,
		Body:        newBody,
		LeaderEpoch: leaderEpoch,
	}, nil
}

// GetCounter retrieves counter data from a store.
// Returns ErrTypeMismatch if the store is not a counter.
func (m *Manager) GetCounter(storeID, customerID string) (*CounterData, uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, exists := m.stores[storeID]
	if !exists {
		return nil, 0, ErrStoreNotFound
	}
	if s.CustomerID != customerID {
		return nil, 0, ErrUnauthorized
	}
	if s.IsExpired() {
		return nil, 0, ErrStoreExpired
	}
	if s.DataType != DataTypeCounter {
		return nil, 0, ErrTypeMismatch
	}

	var counterData CounterData
	if err := json.Unmarshal(s.Body, &counterData); err != nil {
		return nil, 0, err
	}

	return &counterData, s.Version, nil
}

// SetCounter sets a counter to a specific value.
// Validates the value is within bounds if present.
// Returns ErrTypeMismatch if the store is not a counter.
func (m *Manager) SetCounter(storeID, customerID string, value int64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.stores[storeID]
	if !exists {
		return 0, ErrStoreNotFound
	}
	if s.CustomerID != customerID {
		return 0, ErrUnauthorized
	}
	if s.IsExpired() {
		return 0, ErrStoreExpired
	}
	if s.DataType != DataTypeCounter {
		return 0, ErrTypeMismatch
	}

	// Deserialize current counter data to get bounds
	var counterData CounterData
	if err := json.Unmarshal(s.Body, &counterData); err != nil {
		return 0, err
	}

	// Validate value is within bounds
	if counterData.Min != nil && value < *counterData.Min {
		return 0, ErrValueOutOfBounds
	}
	if counterData.Max != nil && value > *counterData.Max {
		return 0, ErrValueOutOfBounds
	}

	// Update counter value
	counterData.Value = value
	newBody, err := json.Marshal(counterData)
	if err != nil {
		return 0, err
	}

	// Update store
	oldSize := storeSize(s)
	s.Body = newBody
	s.Version++
	newSize := storeSize(s)
	m.usedBytes += newSize - oldSize

	return s.Version, nil
}
