package registry

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// EntryState represents the lifecycle state of a registry entry as it moves through
// reservation, activation, and deletion phases.
type EntryState int

const (
	// StateCreating indicates a reservation is held but the store hasn't been created yet.
	StateCreating EntryState = iota
	// StateActive indicates the store has been created and is linked to this name.
	StateActive
	// StateDeleting indicates the entry is being deleted but the operation isn't complete.
	StateDeleting
)

// String returns a human-readable representation of the entry state.
func (s EntryState) String() string {
	switch s {
	case StateCreating:
		return "creating"
	case StateActive:
		return "active"
	case StateDeleting:
		return "deleting"
	default:
		return "unknown"
	}
}

// Entry represents a named store registration in the registry. It maps a customer-scoped
// name to a store ID, tracking the entry through its lifecycle from reservation to deletion.
type Entry struct {
	CustomerID    string
	Name          string
	StoreID       string // set when state=Active
	State         EntryState
	ExpiresAt     time.Time // store expiry (copied from store)
	ReservationID string    // unique ID for this reservation attempt
	ReservedAt    time.Time // when reservation was created
	LeaderEpoch   uint64
	Version       uint64
}

// Key returns the composite key used to index this entry in the registry.
func (e *Entry) Key() string {
	return MakeKey(e.CustomerID, e.Name)
}

// MakeKey builds the composite key for a customer and name pair.
func MakeKey(customerID, name string) string {
	return customerID + ":" + name
}

// Copy returns a deep copy of the entry to avoid pointer aliasing issues.
func (e *Entry) Copy() *Entry {
	return &Entry{
		CustomerID:    e.CustomerID,
		Name:          e.Name,
		StoreID:       e.StoreID,
		State:         e.State,
		ExpiresAt:     e.ExpiresAt,
		ReservationID: e.ReservationID,
		ReservedAt:    e.ReservedAt,
		LeaderEpoch:   e.LeaderEpoch,
		Version:       e.Version,
	}
}

// ReservationTTL is how long a name reservation is held before it expires and can be reclaimed.
const ReservationTTL = 5 * time.Second

// IsReservationExpired returns true if this entry is a reservation that has timed out.
func (e *Entry) IsReservationExpired() bool {
	if e.State != StateCreating {
		return false
	}
	return time.Since(e.ReservedAt) > ReservationTTL
}

// Manager handles named store registrations with a two-phase reservation protocol.
// It ensures name uniqueness within each customer's namespace and coordinates with
// replication to maintain consistency across nodes.
type Manager struct {
	mu      sync.RWMutex
	entries map[string]*Entry // key = customerID:name
}

// NewManager creates an empty registry manager ready to accept reservations.
func NewManager() *Manager {
	return &Manager{
		entries: make(map[string]*Entry),
	}
}

// Reserve attempts to claim a name for a customer. This is the first phase of the
// two-phase creation protocol. The reservation is held for ReservationTTL, giving the
// caller time to create the actual store before committing.
func (m *Manager) Reserve(customerID, name string, leaderEpoch uint64) (*Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := MakeKey(customerID, name)

	if existing, exists := m.entries[key]; exists {
		if existing.State == StateActive {
			return nil, ErrNameExists
		}
		if existing.State == StateCreating && !existing.IsReservationExpired() {
			return nil, ErrNameReserved
		}
		if existing.State == StateDeleting {
			return nil, ErrNameDeleting
		}
	}

	reservationID, err := generateReservationID()
	if err != nil {
		return nil, err
	}

	entry := &Entry{
		CustomerID:    customerID,
		Name:          name,
		State:         StateCreating,
		ReservationID: reservationID,
		ReservedAt:    time.Now(),
		LeaderEpoch:   leaderEpoch,
		Version:       1,
	}
	m.entries[key] = entry

	return entry.Copy(), nil
}

// Commit finalizes a reservation by linking it to the newly created store. This is the
// second phase of the two-phase protocol. The reservationID must match to prevent races.
func (m *Manager) Commit(customerID, name, reservationID, storeID string, expiresAt time.Time, leaderEpoch uint64) (*Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := MakeKey(customerID, name)
	entry, exists := m.entries[key]
	if !exists {
		return nil, ErrEntryNotFound
	}

	if entry.CustomerID != customerID {
		return nil, ErrUnauthorized
	}

	if entry.State != StateCreating {
		return nil, ErrInvalidState
	}

	if entry.ReservationID != reservationID {
		return nil, ErrReservationMismatch
	}

	entry.State = StateActive
	entry.StoreID = storeID
	entry.ExpiresAt = expiresAt
	entry.LeaderEpoch = leaderEpoch
	entry.Version++

	return entry.Copy(), nil
}

// Abort cancels a pending reservation, freeing the name for others. This is idempotent
// and returns nil if the entry doesn't exist.
func (m *Manager) Abort(customerID, name, reservationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := MakeKey(customerID, name)
	entry, exists := m.entries[key]
	if !exists {
		return nil // idempotent
	}

	if entry.CustomerID != customerID {
		return ErrUnauthorized
	}

	if entry.State != StateCreating {
		return ErrInvalidState
	}

	if entry.ReservationID != reservationID {
		return ErrReservationMismatch
	}

	delete(m.entries, key)
	return nil
}

// Lookup retrieves a registry entry by customer and name. Returns a copy to avoid races.
func (m *Manager) Lookup(customerID, name string) (*Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := MakeKey(customerID, name)
	entry, exists := m.entries[key]
	if !exists {
		return nil, ErrEntryNotFound
	}

	if entry.CustomerID != customerID {
		return nil, ErrUnauthorized
	}

	return entry.Copy(), nil
}

// LookupByStoreID finds an active entry by its store ID. This is useful for reverse lookups
// when you have a store ID but need to find its registered name.
func (m *Manager) LookupByStoreID(storeID string) (*Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, entry := range m.entries {
		if entry.StoreID == storeID && entry.State == StateActive {
			return entry.Copy(), nil
		}
	}
	return nil, ErrEntryNotFound
}

// MarkDeleting transitions an active entry to the deleting state. This prevents new
// reservations for the name while the underlying store is being deleted.
func (m *Manager) MarkDeleting(customerID, name string, leaderEpoch uint64) (*Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := MakeKey(customerID, name)
	entry, exists := m.entries[key]
	if !exists {
		return nil, ErrEntryNotFound
	}

	if entry.CustomerID != customerID {
		return nil, ErrUnauthorized
	}

	if entry.State == StateCreating {
		return nil, ErrInvalidState
	}

	entry.State = StateDeleting
	entry.LeaderEpoch = leaderEpoch
	entry.Version++

	return entry.Copy(), nil
}

// Delete removes an entry from the registry entirely. This is idempotent and returns
// nil if the entry doesn't exist.
func (m *Manager) Delete(customerID, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := MakeKey(customerID, name)
	entry, exists := m.entries[key]
	if !exists {
		return nil // idempotent
	}

	if entry.CustomerID != customerID {
		return ErrUnauthorized
	}

	delete(m.entries, key)
	return nil
}

// RevertToActive moves a deleting entry back to active state. This is used when the
// underlying store deletion fails and the entry needs to be recovered.
func (m *Manager) RevertToActive(customerID, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := MakeKey(customerID, name)
	entry, exists := m.entries[key]
	if !exists {
		return ErrEntryNotFound
	}

	if entry.CustomerID != customerID {
		return ErrUnauthorized
	}

	if entry.State != StateDeleting {
		return ErrInvalidState
	}

	entry.State = StateActive
	entry.Version++
	return nil
}

// ApplyReplicatedEntry applies an entry received from replication. It uses version-based
// conflict resolution, only applying updates with a higher version number.
func (m *Manager) ApplyReplicatedEntry(e *Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := e.Key()
	existing, exists := m.entries[key]

	if !exists {
		m.entries[key] = e.Copy()
		return nil
	}

	if e.Version > existing.Version {
		m.entries[key] = e.Copy()
	}
	return nil
}

// ApplyReplicatedDelete removes an entry as instructed by replication. Unlike Delete,
// this doesn't check ownership since it's already been validated by the primary.
func (m *Manager) ApplyReplicatedDelete(customerID, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := MakeKey(customerID, name)
	delete(m.entries, key)
	return nil
}

// GetExpiredReservations returns all reservations that have timed out and can be cleaned up.
func (m *Manager) GetExpiredReservations() []*Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var expired []*Entry
	for _, entry := range m.entries {
		if entry.IsReservationExpired() {
			expired = append(expired, entry.Copy())
		}
	}
	return expired
}

// GetDeletingEntries returns all entries currently in the deleting state.
func (m *Manager) GetDeletingEntries() []*Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var deleting []*Entry
	for _, entry := range m.entries {
		if entry.State == StateDeleting {
			deleting = append(deleting, entry.Copy())
		}
	}
	return deleting
}

// Count returns the total number of entries in the registry across all states.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

// CountByState returns entry counts grouped by their lifecycle state.
func (m *Manager) CountByState() map[EntryState]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	counts := make(map[EntryState]int)
	for _, entry := range m.entries {
		counts[entry.State]++
	}
	return counts
}

func generateReservationID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// Snapshot returns a consistent point-in-time view of all registry entries.
// Returns deep copies to avoid races.
func (m *Manager) Snapshot() []*Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]*Entry, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e.Copy())
	}
	return entries
}

// Reset replaces all registry entries with the provided snapshot.
func (m *Manager) Reset(entries []*Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = make(map[string]*Entry)
	for _, e := range entries {
		m.entries[e.Key()] = e
	}
}
