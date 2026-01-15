package registry

import (
	"testing"
	"time"
)

func TestLookupByStoreID_BasicIndexing(t *testing.T) {
	m := NewManager()

	// Reserve and commit an entry
	entry, err := m.Reserve("cust1", "mystore", 1)
	if err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	_, err = m.Commit("cust1", "mystore", entry.ReservationID, "store-123", time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// LookupByStoreID should find it
	found, err := m.LookupByStoreID("store-123")
	if err != nil {
		t.Fatalf("LookupByStoreID failed: %v", err)
	}
	if found.CustomerID != "cust1" || found.Name != "mystore" {
		t.Errorf("got entry %s:%s, want cust1:mystore", found.CustomerID, found.Name)
	}

	// Non-existent storeID should return error
	_, err = m.LookupByStoreID("nonexistent")
	if err != ErrEntryNotFound {
		t.Errorf("expected ErrEntryNotFound, got %v", err)
	}
}

func TestLookupByStoreID_MarkDeletingRemovesFromIndex(t *testing.T) {
	m := NewManager()

	entry, _ := m.Reserve("cust1", "mystore", 1)
	m.Commit("cust1", "mystore", entry.ReservationID, "store-123", time.Now().Add(time.Hour), 1)

	// Verify it's in the index
	_, err := m.LookupByStoreID("store-123")
	if err != nil {
		t.Fatalf("entry should be in index: %v", err)
	}

	// Mark as deleting
	_, err = m.MarkDeleting("cust1", "mystore", 2)
	if err != nil {
		t.Fatalf("MarkDeleting failed: %v", err)
	}

	// Should no longer be in index
	_, err = m.LookupByStoreID("store-123")
	if err != ErrEntryNotFound {
		t.Errorf("expected ErrEntryNotFound after MarkDeleting, got %v", err)
	}
}

func TestLookupByStoreID_RevertToActiveRestoresIndex(t *testing.T) {
	m := NewManager()

	entry, _ := m.Reserve("cust1", "mystore", 1)
	m.Commit("cust1", "mystore", entry.ReservationID, "store-123", time.Now().Add(time.Hour), 1)
	m.MarkDeleting("cust1", "mystore", 2)

	// Verify removed from index
	_, err := m.LookupByStoreID("store-123")
	if err != ErrEntryNotFound {
		t.Fatalf("should not be in index after MarkDeleting")
	}

	// Revert to active
	err = m.RevertToActive("cust1", "mystore")
	if err != nil {
		t.Fatalf("RevertToActive failed: %v", err)
	}

	// Should be back in index
	found, err := m.LookupByStoreID("store-123")
	if err != nil {
		t.Fatalf("should be in index after RevertToActive: %v", err)
	}
	if found.StoreID != "store-123" {
		t.Errorf("got storeID %s, want store-123", found.StoreID)
	}
}

func TestLookupByStoreID_DeleteRemovesFromIndex(t *testing.T) {
	m := NewManager()

	entry, _ := m.Reserve("cust1", "mystore", 1)
	m.Commit("cust1", "mystore", entry.ReservationID, "store-123", time.Now().Add(time.Hour), 1)

	// Delete the entry
	err := m.Delete("cust1", "mystore")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Should no longer be in index
	_, err = m.LookupByStoreID("store-123")
	if err != ErrEntryNotFound {
		t.Errorf("expected ErrEntryNotFound after Delete, got %v", err)
	}
}

func TestLookupByStoreID_ApplyReplicatedEntry(t *testing.T) {
	m := NewManager()

	// Apply a new active entry via replication
	entry := &Entry{
		CustomerID:  "cust1",
		Name:        "replicated",
		StoreID:     "store-456",
		State:       StateActive,
		ExpiresAt:   time.Now().Add(time.Hour),
		LeaderEpoch: 1,
		Version:     1,
	}
	err := m.ApplyReplicatedEntry(entry)
	if err != nil {
		t.Fatalf("ApplyReplicatedEntry failed: %v", err)
	}

	// Should be in index
	found, err := m.LookupByStoreID("store-456")
	if err != nil {
		t.Fatalf("should be in index: %v", err)
	}
	if found.Name != "replicated" {
		t.Errorf("got name %s, want replicated", found.Name)
	}

	// Apply update that changes storeID
	entry2 := &Entry{
		CustomerID:  "cust1",
		Name:        "replicated",
		StoreID:     "store-789",
		State:       StateActive,
		ExpiresAt:   time.Now().Add(time.Hour),
		LeaderEpoch: 2,
		Version:     2,
	}
	err = m.ApplyReplicatedEntry(entry2)
	if err != nil {
		t.Fatalf("ApplyReplicatedEntry update failed: %v", err)
	}

	// Old storeID should be gone
	_, err = m.LookupByStoreID("store-456")
	if err != ErrEntryNotFound {
		t.Errorf("old storeID should be removed from index")
	}

	// New storeID should be present
	found, err = m.LookupByStoreID("store-789")
	if err != nil {
		t.Fatalf("new storeID should be in index: %v", err)
	}
	if found.Version != 2 {
		t.Errorf("got version %d, want 2", found.Version)
	}
}

func TestLookupByStoreID_ApplyReplicatedEntryNonActive(t *testing.T) {
	m := NewManager()

	// Apply a deleting entry - should not be indexed
	entry := &Entry{
		CustomerID:  "cust1",
		Name:        "deleting",
		StoreID:     "store-del",
		State:       StateDeleting,
		ExpiresAt:   time.Now().Add(time.Hour),
		LeaderEpoch: 1,
		Version:     1,
	}
	m.ApplyReplicatedEntry(entry)

	_, err := m.LookupByStoreID("store-del")
	if err != ErrEntryNotFound {
		t.Errorf("deleting entries should not be indexed")
	}
}

func TestLookupByStoreID_ApplyReplicatedDelete(t *testing.T) {
	m := NewManager()

	entry, _ := m.Reserve("cust1", "mystore", 1)
	m.Commit("cust1", "mystore", entry.ReservationID, "store-123", time.Now().Add(time.Hour), 1)

	// Apply replicated delete
	err := m.ApplyReplicatedDelete("cust1", "mystore")
	if err != nil {
		t.Fatalf("ApplyReplicatedDelete failed: %v", err)
	}

	// Should no longer be in index
	_, err = m.LookupByStoreID("store-123")
	if err != ErrEntryNotFound {
		t.Errorf("expected ErrEntryNotFound after ApplyReplicatedDelete, got %v", err)
	}
}

func TestLookupByStoreID_Reset(t *testing.T) {
	m := NewManager()

	// Add some entries
	entry1, _ := m.Reserve("cust1", "store1", 1)
	m.Commit("cust1", "store1", entry1.ReservationID, "id-1", time.Now().Add(time.Hour), 1)

	entry2, _ := m.Reserve("cust1", "store2", 1)
	m.Commit("cust1", "store2", entry2.ReservationID, "id-2", time.Now().Add(time.Hour), 1)

	// Reset with a new set of entries
	newEntries := []*Entry{
		{CustomerID: "cust2", Name: "new1", StoreID: "new-id-1", State: StateActive, Version: 1},
		{CustomerID: "cust2", Name: "new2", StoreID: "new-id-2", State: StateActive, Version: 1},
		{CustomerID: "cust2", Name: "creating", StoreID: "", State: StateCreating, Version: 1},
	}
	if err := m.Reset(newEntries); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Old entries should be gone
	_, err := m.LookupByStoreID("id-1")
	if err != ErrEntryNotFound {
		t.Errorf("old entries should be cleared from index")
	}

	// New entries should be present
	found, err := m.LookupByStoreID("new-id-1")
	if err != nil {
		t.Fatalf("new entry should be in index: %v", err)
	}
	if found.CustomerID != "cust2" {
		t.Errorf("got customer %s, want cust2", found.CustomerID)
	}

	found, err = m.LookupByStoreID("new-id-2")
	if err != nil {
		t.Fatalf("new entry should be in index: %v", err)
	}
	if found.Name != "new2" {
		t.Errorf("got name %s, want new2", found.Name)
	}
}

func TestLookupByStoreID_IndexConsistency(t *testing.T) {
	m := NewManager()

	// Create multiple entries
	for i := 0; i < 10; i++ {
		entry, _ := m.Reserve("cust1", string(rune('a'+i)), 1)
		m.Commit("cust1", string(rune('a'+i)), entry.ReservationID, "store-"+string(rune('a'+i)), time.Now().Add(time.Hour), 1)
	}

	// Verify index has exactly 10 entries
	m.mu.RLock()
	indexSize := len(m.byStoreID)
	m.mu.RUnlock()

	if indexSize != 10 {
		t.Errorf("index size = %d, want 10", indexSize)
	}

	// Delete half of them
	for i := 0; i < 5; i++ {
		m.Delete("cust1", string(rune('a'+i)))
	}

	// Verify index has exactly 5 entries
	m.mu.RLock()
	indexSize = len(m.byStoreID)
	m.mu.RUnlock()

	if indexSize != 5 {
		t.Errorf("index size after deletes = %d, want 5", indexSize)
	}

	// Verify the right entries remain
	for i := 5; i < 10; i++ {
		storeID := "store-" + string(rune('a'+i))
		_, err := m.LookupByStoreID(storeID)
		if err != nil {
			t.Errorf("entry %s should still be in index", storeID)
		}
	}
}

func TestStoreIDCollision_Commit(t *testing.T) {
	m := NewManager()

	// Create first entry with a storeID
	entry1, _ := m.Reserve("cust1", "store1", 1)
	_, err := m.Commit("cust1", "store1", entry1.ReservationID, "shared-id", time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}

	// Try to create second entry with same storeID - should fail
	entry2, _ := m.Reserve("cust1", "store2", 1)
	_, err = m.Commit("cust1", "store2", entry2.ReservationID, "shared-id", time.Now().Add(time.Hour), 1)
	if err != ErrStoreIDCollision {
		t.Errorf("expected ErrStoreIDCollision, got %v", err)
	}

	// Verify first entry is still intact
	found, err := m.LookupByStoreID("shared-id")
	if err != nil {
		t.Fatalf("first entry should still be in index: %v", err)
	}
	if found.Name != "store1" {
		t.Errorf("got name %s, want store1", found.Name)
	}

	// Verify second entry stayed in Creating state
	entry2Check, _ := m.Lookup("cust1", "store2")
	if entry2Check.State != StateCreating {
		t.Errorf("second entry should still be Creating, got %s", entry2Check.State)
	}
}

func TestStoreIDCollision_RevertToActive(t *testing.T) {
	m := NewManager()

	// Create two entries
	entry1, _ := m.Reserve("cust1", "store1", 1)
	m.Commit("cust1", "store1", entry1.ReservationID, "id-1", time.Now().Add(time.Hour), 1)

	entry2, _ := m.Reserve("cust1", "store2", 1)
	m.Commit("cust1", "store2", entry2.ReservationID, "id-2", time.Now().Add(time.Hour), 1)

	// Mark both as deleting
	m.MarkDeleting("cust1", "store1", 2)
	m.MarkDeleting("cust1", "store2", 2)

	// Manually corrupt: change store2's storeID to match store1's
	m.mu.Lock()
	m.entries[MakeKey("cust1", "store2")].StoreID = "id-1"
	m.mu.Unlock()

	// Revert store1 - should succeed
	err := m.RevertToActive("cust1", "store1")
	if err != nil {
		t.Fatalf("RevertToActive for store1 failed: %v", err)
	}

	// Revert store2 - should fail due to collision
	err = m.RevertToActive("cust1", "store2")
	if err != ErrStoreIDCollision {
		t.Errorf("expected ErrStoreIDCollision, got %v", err)
	}

	// Verify store2 stayed in Deleting state
	entry2Check, _ := m.Lookup("cust1", "store2")
	if entry2Check.State != StateDeleting {
		t.Errorf("store2 should still be Deleting, got %s", entry2Check.State)
	}
}

func TestStoreIDCollision_ApplyReplicatedEntry(t *testing.T) {
	m := NewManager()

	// Create first entry
	entry1, _ := m.Reserve("cust1", "store1", 1)
	m.Commit("cust1", "store1", entry1.ReservationID, "shared-id", time.Now().Add(time.Hour), 1)

	// Try to apply replicated entry with same storeID but different key
	replicated := &Entry{
		CustomerID:  "cust2",
		Name:        "store2",
		StoreID:     "shared-id",
		State:       StateActive,
		ExpiresAt:   time.Now().Add(time.Hour),
		LeaderEpoch: 1,
		Version:     1,
	}
	err := m.ApplyReplicatedEntry(replicated)
	if err != ErrStoreIDCollision {
		t.Errorf("expected ErrStoreIDCollision, got %v", err)
	}

	// Verify replicated entry was not added
	_, err = m.Lookup("cust2", "store2")
	if err != ErrEntryNotFound {
		t.Errorf("conflicting entry should not have been added")
	}
}

func TestStoreIDCollision_Reset(t *testing.T) {
	m := NewManager()

	// Try to reset with duplicate storeIDs
	entries := []*Entry{
		{CustomerID: "cust1", Name: "store1", StoreID: "shared-id", State: StateActive, Version: 1},
		{CustomerID: "cust2", Name: "store2", StoreID: "shared-id", State: StateActive, Version: 1},
	}
	err := m.Reset(entries)
	if err != ErrStoreIDCollision {
		t.Errorf("expected ErrStoreIDCollision, got %v", err)
	}

	// Verify reset didn't happen (manager should be empty)
	if m.Count() != 0 {
		t.Errorf("manager should be empty after failed reset, got %d entries", m.Count())
	}
}

func TestApplyReplicatedEntry_StateTransitionRemovesFromIndex(t *testing.T) {
	m := NewManager()

	// Apply an active entry
	active := &Entry{
		CustomerID:  "cust1",
		Name:        "mystore",
		StoreID:     "store-123",
		State:       StateActive,
		ExpiresAt:   time.Now().Add(time.Hour),
		LeaderEpoch: 1,
		Version:     1,
	}
	if err := m.ApplyReplicatedEntry(active); err != nil {
		t.Fatalf("ApplyReplicatedEntry failed: %v", err)
	}

	// Verify it's in the index
	_, err := m.LookupByStoreID("store-123")
	if err != nil {
		t.Fatalf("entry should be in index: %v", err)
	}

	// Apply replicated update transitioning to StateDeleting (same storeID)
	deleting := &Entry{
		CustomerID:  "cust1",
		Name:        "mystore",
		StoreID:     "store-123",
		State:       StateDeleting,
		ExpiresAt:   time.Now().Add(time.Hour),
		LeaderEpoch: 2,
		Version:     2,
	}
	if err := m.ApplyReplicatedEntry(deleting); err != nil {
		t.Fatalf("ApplyReplicatedEntry for deleting failed: %v", err)
	}

	// Verify it's removed from index (deleting entries shouldn't be indexed)
	_, err = m.LookupByStoreID("store-123")
	if err != ErrEntryNotFound {
		t.Errorf("expected ErrEntryNotFound after state transition to Deleting, got %v", err)
	}

	// Verify the entry itself still exists with correct state
	entry, err := m.Lookup("cust1", "mystore")
	if err != nil {
		t.Fatalf("entry should still exist: %v", err)
	}
	if entry.State != StateDeleting {
		t.Errorf("entry state should be Deleting, got %s", entry.State)
	}
}
