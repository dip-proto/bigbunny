package test

import (
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/registry"
)

func TestRegistryReserveAndCommit(t *testing.T) {
	mgr := registry.NewManager()

	entry, err := mgr.Reserve("cust1", "my-store", 1)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	if entry.State != registry.StateCreating {
		t.Errorf("state mismatch: got %v, want StateCreating", entry.State)
	}
	if entry.ReservationID == "" {
		t.Error("reservation ID should not be empty")
	}

	committed, err := mgr.Commit("cust1", "my-store", entry.ReservationID, "store-id-123", time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	if committed.State != registry.StateActive {
		t.Errorf("state mismatch: got %v, want StateActive", committed.State)
	}
	if committed.StoreID != "store-id-123" {
		t.Errorf("store ID mismatch: got %s, want store-id-123", committed.StoreID)
	}

	lookup, err := mgr.Lookup("cust1", "my-store")
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if lookup.StoreID != "store-id-123" {
		t.Errorf("store ID mismatch: got %s, want store-id-123", lookup.StoreID)
	}
}

func TestRegistryReserveDuplicate(t *testing.T) {
	mgr := registry.NewManager()

	_, err := mgr.Reserve("cust1", "my-store", 1)
	if err != nil {
		t.Fatalf("first reserve failed: %v", err)
	}

	_, err = mgr.Reserve("cust1", "my-store", 1)
	if err != registry.ErrNameReserved {
		t.Errorf("expected ErrNameReserved, got %v", err)
	}
}

func TestRegistryReserveExisting(t *testing.T) {
	mgr := registry.NewManager()

	entry, _ := mgr.Reserve("cust1", "my-store", 1)
	mgr.Commit("cust1", "my-store", entry.ReservationID, "store-id-123", time.Now().Add(time.Hour), 1)

	_, err := mgr.Reserve("cust1", "my-store", 1)
	if err != registry.ErrNameExists {
		t.Errorf("expected ErrNameExists, got %v", err)
	}
}

func TestRegistryAbort(t *testing.T) {
	mgr := registry.NewManager()

	entry, _ := mgr.Reserve("cust1", "my-store", 1)

	err := mgr.Abort("cust1", "my-store", entry.ReservationID)
	if err != nil {
		t.Fatalf("abort failed: %v", err)
	}

	_, err = mgr.Lookup("cust1", "my-store")
	if err != registry.ErrEntryNotFound {
		t.Errorf("expected ErrEntryNotFound after abort, got %v", err)
	}

	_, err = mgr.Reserve("cust1", "my-store", 1)
	if err != nil {
		t.Errorf("reserve after abort should succeed: %v", err)
	}
}

func TestRegistryAbortWrongReservation(t *testing.T) {
	mgr := registry.NewManager()

	mgr.Reserve("cust1", "my-store", 1)

	err := mgr.Abort("cust1", "my-store", "wrong-id")
	if err != registry.ErrReservationMismatch {
		t.Errorf("expected ErrReservationMismatch, got %v", err)
	}
}

func TestRegistryCommitWrongReservation(t *testing.T) {
	mgr := registry.NewManager()

	mgr.Reserve("cust1", "my-store", 1)

	_, err := mgr.Commit("cust1", "my-store", "wrong-id", "store-id", time.Now().Add(time.Hour), 1)
	if err != registry.ErrReservationMismatch {
		t.Errorf("expected ErrReservationMismatch, got %v", err)
	}
}

func TestRegistryDelete(t *testing.T) {
	mgr := registry.NewManager()

	entry, _ := mgr.Reserve("cust1", "my-store", 1)
	mgr.Commit("cust1", "my-store", entry.ReservationID, "store-id-123", time.Now().Add(time.Hour), 1)

	err := mgr.Delete("cust1", "my-store")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, err = mgr.Lookup("cust1", "my-store")
	if err != registry.ErrEntryNotFound {
		t.Errorf("expected ErrEntryNotFound after delete, got %v", err)
	}
}

func TestRegistryMarkDeleting(t *testing.T) {
	mgr := registry.NewManager()

	entry, _ := mgr.Reserve("cust1", "my-store", 1)
	mgr.Commit("cust1", "my-store", entry.ReservationID, "store-id-123", time.Now().Add(time.Hour), 1)

	deleting, err := mgr.MarkDeleting("cust1", "my-store", 2)
	if err != nil {
		t.Fatalf("mark deleting failed: %v", err)
	}

	if deleting.State != registry.StateDeleting {
		t.Errorf("state mismatch: got %v, want StateDeleting", deleting.State)
	}

	_, err = mgr.Reserve("cust1", "my-store", 1)
	if err != registry.ErrNameDeleting {
		t.Errorf("expected ErrNameDeleting, got %v", err)
	}
}

func TestRegistryRevertToActive(t *testing.T) {
	mgr := registry.NewManager()

	entry, _ := mgr.Reserve("cust1", "my-store", 1)
	mgr.Commit("cust1", "my-store", entry.ReservationID, "store-id-123", time.Now().Add(time.Hour), 1)
	mgr.MarkDeleting("cust1", "my-store", 2)

	err := mgr.RevertToActive("cust1", "my-store")
	if err != nil {
		t.Fatalf("revert to active failed: %v", err)
	}

	lookup, _ := mgr.Lookup("cust1", "my-store")
	if lookup.State != registry.StateActive {
		t.Errorf("state mismatch: got %v, want StateActive", lookup.State)
	}
}

func TestRegistryCustomerIsolation(t *testing.T) {
	mgr := registry.NewManager()

	entry, _ := mgr.Reserve("cust1", "my-store", 1)
	mgr.Commit("cust1", "my-store", entry.ReservationID, "store-id-123", time.Now().Add(time.Hour), 1)

	_, err := mgr.Lookup("cust2", "my-store")
	if err != registry.ErrEntryNotFound {
		t.Errorf("expected ErrEntryNotFound for different customer, got %v", err)
	}

	entry2, err := mgr.Reserve("cust2", "my-store", 1)
	if err != nil {
		t.Fatalf("different customer should be able to reserve same name: %v", err)
	}
	if entry2.CustomerID != "cust2" {
		t.Errorf("customer ID mismatch: got %s, want cust2", entry2.CustomerID)
	}
}

func TestRegistryLookupByStoreID(t *testing.T) {
	mgr := registry.NewManager()

	entry, _ := mgr.Reserve("cust1", "my-store", 1)
	mgr.Commit("cust1", "my-store", entry.ReservationID, "store-id-123", time.Now().Add(time.Hour), 1)

	found, err := mgr.LookupByStoreID("store-id-123")
	if err != nil {
		t.Fatalf("lookup by store ID failed: %v", err)
	}
	if found.Name != "my-store" {
		t.Errorf("name mismatch: got %s, want my-store", found.Name)
	}

	_, err = mgr.LookupByStoreID("non-existent")
	if err != registry.ErrEntryNotFound {
		t.Errorf("expected ErrEntryNotFound, got %v", err)
	}
}

func TestRegistryApplyReplicatedEntry(t *testing.T) {
	mgr := registry.NewManager()

	entry := &registry.Entry{
		CustomerID:    "cust1",
		Name:          "replicated-store",
		StoreID:       "store-id-456",
		State:         registry.StateActive,
		ExpiresAt:     time.Now().Add(time.Hour),
		ReservationID: "res-123",
		LeaderEpoch:   1,
		Version:       1,
	}

	err := mgr.ApplyReplicatedEntry(entry)
	if err != nil {
		t.Fatalf("apply replicated entry failed: %v", err)
	}

	lookup, err := mgr.Lookup("cust1", "replicated-store")
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if lookup.StoreID != "store-id-456" {
		t.Errorf("store ID mismatch: got %s, want store-id-456", lookup.StoreID)
	}
}

func TestRegistryApplyReplicatedEntryIdempotent(t *testing.T) {
	mgr := registry.NewManager()

	entry := &registry.Entry{
		CustomerID:  "cust1",
		Name:        "replicated-store",
		StoreID:     "store-id-v1",
		State:       registry.StateActive,
		LeaderEpoch: 1,
		Version:     1,
	}
	mgr.ApplyReplicatedEntry(entry)

	entryOld := &registry.Entry{
		CustomerID:  "cust1",
		Name:        "replicated-store",
		StoreID:     "store-id-old",
		State:       registry.StateActive,
		LeaderEpoch: 1,
		Version:     0,
	}
	mgr.ApplyReplicatedEntry(entryOld)

	lookup, _ := mgr.Lookup("cust1", "replicated-store")
	if lookup.StoreID != "store-id-v1" {
		t.Errorf("old version should not overwrite: got %s, want store-id-v1", lookup.StoreID)
	}

	entryNew := &registry.Entry{
		CustomerID:  "cust1",
		Name:        "replicated-store",
		StoreID:     "store-id-v2",
		State:       registry.StateActive,
		LeaderEpoch: 2,
		Version:     2,
	}
	mgr.ApplyReplicatedEntry(entryNew)

	lookup, _ = mgr.Lookup("cust1", "replicated-store")
	if lookup.StoreID != "store-id-v2" {
		t.Errorf("new version should overwrite: got %s, want store-id-v2", lookup.StoreID)
	}
}

func TestRegistryApplyReplicatedDelete(t *testing.T) {
	mgr := registry.NewManager()

	entry := &registry.Entry{
		CustomerID: "cust1",
		Name:       "to-delete",
		StoreID:    "store-id",
		State:      registry.StateActive,
		Version:    1,
	}
	mgr.ApplyReplicatedEntry(entry)

	err := mgr.ApplyReplicatedDelete("cust1", "to-delete")
	if err != nil {
		t.Fatalf("apply replicated delete failed: %v", err)
	}

	_, err = mgr.Lookup("cust1", "to-delete")
	if err != registry.ErrEntryNotFound {
		t.Errorf("expected ErrEntryNotFound after replicated delete, got %v", err)
	}
}

func TestRegistryCount(t *testing.T) {
	mgr := registry.NewManager()

	if mgr.Count() != 0 {
		t.Errorf("expected count 0, got %d", mgr.Count())
	}

	entry1, _ := mgr.Reserve("cust1", "store1", 1)
	if mgr.Count() != 1 {
		t.Errorf("expected count 1, got %d", mgr.Count())
	}

	entry2, _ := mgr.Reserve("cust1", "store2", 1)
	if mgr.Count() != 2 {
		t.Errorf("expected count 2, got %d", mgr.Count())
	}

	mgr.Commit("cust1", "store1", entry1.ReservationID, "id1", time.Now().Add(time.Hour), 1)
	mgr.Commit("cust1", "store2", entry2.ReservationID, "id2", time.Now().Add(time.Hour), 1)

	counts := mgr.CountByState()
	if counts[registry.StateActive] != 2 {
		t.Errorf("expected 2 active, got %d", counts[registry.StateActive])
	}
}

func TestMakeKey(t *testing.T) {
	key := registry.MakeKey("customer-123", "my-store")
	expected := "customer-123:my-store"
	if key != expected {
		t.Errorf("key mismatch: got %s, want %s", key, expected)
	}
}

// Phase C.5: Snapshot and Reset tests

func TestRegistrySnapshot(t *testing.T) {
	mgr := registry.NewManager()

	// Create some entries
	entry1, _ := mgr.Reserve("cust1", "store1", 1)
	mgr.Commit("cust1", "store1", entry1.ReservationID, "id1", time.Now().Add(time.Hour), 1)

	entry2, _ := mgr.Reserve("cust1", "store2", 1)
	mgr.Commit("cust1", "store2", entry2.ReservationID, "id2", time.Now().Add(time.Hour), 1)

	entry3, _ := mgr.Reserve("cust2", "store1", 1)
	mgr.Commit("cust2", "store1", entry3.ReservationID, "id3", time.Now().Add(time.Hour), 1)

	// Take snapshot
	snapshot := mgr.Snapshot()

	if len(snapshot) != 3 {
		t.Errorf("expected 3 entries in snapshot, got %d", len(snapshot))
	}

	// Verify all entries are in snapshot
	foundEntries := make(map[string]bool)
	for _, e := range snapshot {
		key := e.Key()
		foundEntries[key] = true
	}

	expectedKeys := []string{"cust1:store1", "cust1:store2", "cust2:store1"}
	for _, key := range expectedKeys {
		if !foundEntries[key] {
			t.Errorf("entry %s not found in snapshot", key)
		}
	}
}

func TestRegistryReset(t *testing.T) {
	mgr := registry.NewManager()

	// Create initial entries
	entry1, _ := mgr.Reserve("cust1", "old-store1", 1)
	mgr.Commit("cust1", "old-store1", entry1.ReservationID, "old-id1", time.Now().Add(time.Hour), 1)

	entry2, _ := mgr.Reserve("cust1", "old-store2", 1)
	mgr.Commit("cust1", "old-store2", entry2.ReservationID, "old-id2", time.Now().Add(time.Hour), 1)

	// Reset with new entries
	newEntries := []*registry.Entry{
		{CustomerID: "cust2", Name: "new-store1", StoreID: "new-id1", State: registry.StateActive, Version: 1},
		{CustomerID: "cust2", Name: "new-store2", StoreID: "new-id2", State: registry.StateActive, Version: 1},
		{CustomerID: "cust3", Name: "new-store1", StoreID: "new-id3", State: registry.StateActive, Version: 1},
	}
	mgr.Reset(newEntries)

	// Old entries should be gone
	_, err := mgr.Lookup("cust1", "old-store1")
	if err != registry.ErrEntryNotFound {
		t.Errorf("expected old entry to be gone, got %v", err)
	}

	// New entries should exist
	for _, e := range newEntries {
		got, err := mgr.Lookup(e.CustomerID, e.Name)
		if err != nil {
			t.Errorf("expected new entry %s:%s to exist: %v", e.CustomerID, e.Name, err)
			continue
		}
		if got.StoreID != e.StoreID {
			t.Errorf("entry %s:%s store ID mismatch: got %s, want %s", e.CustomerID, e.Name, got.StoreID, e.StoreID)
		}
	}

	// Count should be updated
	if mgr.Count() != 3 {
		t.Errorf("expected count 3, got %d", mgr.Count())
	}
}

func TestRegistryResetEmptiesAll(t *testing.T) {
	mgr := registry.NewManager()

	// Create entries
	entry1, _ := mgr.Reserve("cust1", "store1", 1)
	mgr.Commit("cust1", "store1", entry1.ReservationID, "id1", time.Now().Add(time.Hour), 1)

	if mgr.Count() != 1 {
		t.Fatalf("expected 1 entry, got %d", mgr.Count())
	}

	// Reset with empty list
	mgr.Reset([]*registry.Entry{})

	if mgr.Count() != 0 {
		t.Errorf("expected 0 entries after reset with empty list, got %d", mgr.Count())
	}
}
