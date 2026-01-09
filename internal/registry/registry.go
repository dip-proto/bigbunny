package registry

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

type EntryState int

const (
	StateCreating EntryState = iota // Reservation held, store not yet created
	StateActive                     // Store created and linked
	StateDeleting                   // Delete in progress
)

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

func (e *Entry) Key() string {
	return MakeKey(e.CustomerID, e.Name)
}

func MakeKey(customerID, name string) string {
	return customerID + ":" + name
}

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

const ReservationTTL = 5 * time.Second

func (e *Entry) IsReservationExpired() bool {
	if e.State != StateCreating {
		return false
	}
	return time.Since(e.ReservedAt) > ReservationTTL
}

type Manager struct {
	mu      sync.RWMutex
	entries map[string]*Entry // key = customerID:name
}

func NewManager() *Manager {
	return &Manager{
		entries: make(map[string]*Entry),
	}
}

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

func (m *Manager) ApplyReplicatedDelete(customerID, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := MakeKey(customerID, name)
	delete(m.entries, key)
	return nil
}

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

func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

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
