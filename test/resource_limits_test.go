package test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/api"
	"github.com/dip-proto/bigbunny/internal/auth"
	"github.com/dip-proto/bigbunny/internal/ratelimit"
	"github.com/dip-proto/bigbunny/internal/registry"
	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
)

const storeOverhead = 256 // matches internal/store/store.go

func TestCustomerMemoryQuota_CreateEnforced(t *testing.T) {
	mgr := store.NewManager()
	quota := int64(1000) // 1KB quota
	mgr.SetCustomerMemoryQuota(quota)

	// Create a small store - should succeed
	s1 := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 100), // 100 bytes + overhead
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s1); err != nil {
		t.Fatalf("first create should succeed: %v", err)
	}

	// Create another store that would exceed quota - should fail
	s2 := &store.Store{
		ID:         "store2",
		CustomerID: "cust1",
		Body:       make([]byte, 800), // 800 bytes + overhead would exceed quota
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s2); err != store.ErrCustomerQuotaExceeded {
		t.Errorf("expected ErrCustomerQuotaExceeded, got %v", err)
	}

	// Different customer should still be able to create
	s3 := &store.Store{
		ID:         "store3",
		CustomerID: "cust2",
		Body:       make([]byte, 500),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s3); err != nil {
		t.Errorf("different customer should be able to create: %v", err)
	}
}

func TestCustomerMemoryQuota_DeleteRestoresQuota(t *testing.T) {
	mgr := store.NewManager()
	quota := int64(600) // Small quota
	mgr.SetCustomerMemoryQuota(quota)

	// Create a store that uses most of the quota
	s := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 300),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Second store would exceed quota
	s2 := &store.Store{
		ID:         "store2",
		CustomerID: "cust1",
		Body:       make([]byte, 300),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s2); err != store.ErrCustomerQuotaExceeded {
		t.Errorf("expected ErrCustomerQuotaExceeded, got %v", err)
	}

	// Delete the first store
	if err := mgr.Delete(s.ID, s.CustomerID); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Now second store should succeed
	if err := mgr.Create(s2); err != nil {
		t.Errorf("create after delete should succeed: %v", err)
	}
}

func TestCustomerMemoryQuota_ForceDeleteRestoresQuota(t *testing.T) {
	mgr := store.NewManager()
	quota := int64(600)
	mgr.SetCustomerMemoryQuota(quota)

	s := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 300),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	// ForceDelete should also restore quota
	mgr.ForceDelete(s.ID)

	s2 := &store.Store{
		ID:         "store2",
		CustomerID: "cust1",
		Body:       make([]byte, 300),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s2); err != nil {
		t.Errorf("create after force delete should succeed: %v", err)
	}
}

func TestCustomerMemoryQuota_UpdateTracksMemory(t *testing.T) {
	mgr := store.NewManager()
	quota := int64(1000)
	mgr.SetCustomerMemoryQuota(quota)

	// Create initial store
	s := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 100),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	initialMem := mgr.MemoryUsage()

	// Update with larger body
	updated := &store.Store{
		ID:         s.ID,
		CustomerID: s.CustomerID,
		Body:       make([]byte, 300),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Update(updated); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	newMem := mgr.MemoryUsage()
	expectedDelta := int64(200) // 300 - 100
	if newMem-initialMem != expectedDelta {
		t.Errorf("memory delta wrong: got %d, expected %d", newMem-initialMem, expectedDelta)
	}
}

func TestCustomerMemoryQuota_CompleteLockTracksMemory(t *testing.T) {
	mgr := store.NewManager()
	quota := int64(1000)
	mgr.SetCustomerMemoryQuota(quota)

	s := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 100),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	// Acquire lock
	_, err := mgr.AcquireLock(s.ID, s.CustomerID, "lock1", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire lock failed: %v", err)
	}

	initialMem := mgr.MemoryUsage()

	// Complete with larger body
	newBody := make([]byte, 400)
	_, err = mgr.CompleteLock(s.ID, s.CustomerID, "lock1", newBody, time.Time{})
	if err != nil {
		t.Fatalf("complete lock failed: %v", err)
	}

	newMem := mgr.MemoryUsage()
	expectedDelta := int64(300) // 400 - 100
	if newMem-initialMem != expectedDelta {
		t.Errorf("memory delta wrong: got %d, expected %d", newMem-initialMem, expectedDelta)
	}
}

func TestCustomerMemoryQuota_CounterIncrementTracksMemory(t *testing.T) {
	mgr := store.NewManager()
	quota := int64(10000)
	mgr.SetCustomerMemoryQuota(quota)

	// Create counter
	_, err := mgr.CreateCounter("counter1", "shard1", "cust1", 0, nil, nil, time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("create counter failed: %v", err)
	}

	initialMem := mgr.MemoryUsage()

	// Increment counter - JSON body may change slightly in size
	_, err = mgr.Increment("counter1", "cust1", 999999999, 1, time.Time{})
	if err != nil {
		t.Fatalf("increment failed: %v", err)
	}

	// Memory should be tracked (may have small delta due to JSON encoding)
	newMem := mgr.MemoryUsage()
	// The JSON encoding goes from {"value":0} to {"value":999999999}
	// The memory should be updated accordingly
	if newMem < initialMem {
		t.Errorf("memory should not decrease after increment: before=%d, after=%d", initialMem, newMem)
	}
}

func TestCustomerMemoryQuota_ReplicationBypassesQuota(t *testing.T) {
	mgr := store.NewManager()
	quota := int64(500) // Very small quota
	mgr.SetCustomerMemoryQuota(quota)

	// Fill up quota
	s1 := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 200),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s1)

	// CreateOrUpdate (replication) should bypass quota
	s2 := &store.Store{
		ID:         "store2",
		CustomerID: "cust1",
		Body:       make([]byte, 500), // Would exceed quota
		ExpiresAt:  time.Now().Add(time.Hour),
		Version:    1,
	}
	if err := mgr.CreateOrUpdate(s2); err != nil {
		t.Errorf("CreateOrUpdate should bypass quota: %v", err)
	}

	// CreateIfNotExists (replication) should also bypass quota
	s3 := &store.Store{
		ID:         "store3",
		CustomerID: "cust1",
		Body:       make([]byte, 500),
		ExpiresAt:  time.Now().Add(time.Hour),
		Version:    1,
	}
	if err := mgr.CreateIfNotExists(s3); err != nil {
		t.Errorf("CreateIfNotExists should bypass quota: %v", err)
	}

	// But memory should still be tracked
	if mgr.MemoryUsage() == 0 {
		t.Error("memory should be tracked even for replicated stores")
	}
}

func TestCustomerMemoryQuota_ApplyReplicatedUpdateTracksMemory(t *testing.T) {
	mgr := store.NewManager()

	// Create initial store
	s := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 100),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	initialMem := mgr.MemoryUsage()

	// Apply replicated update with larger body
	update := &store.ReplicatedUpdate{
		StoreID:          s.ID,
		CustomerID:       s.CustomerID,
		Body:             make([]byte, 500),
		ExpiresAt:        time.Now().Add(time.Hour),
		Version:          2,
		LeaderEpoch:      1,
		LastReplicatedAt: time.Now(),
	}
	if err := mgr.ApplyReplicatedUpdate(update); err != nil {
		t.Fatalf("apply replicated update failed: %v", err)
	}

	newMem := mgr.MemoryUsage()
	expectedDelta := int64(400) // 500 - 100
	if newMem-initialMem != expectedDelta {
		t.Errorf("memory delta wrong: got %d, expected %d", newMem-initialMem, expectedDelta)
	}
}

func TestCustomerMemoryQuota_MultipleCustomersIsolated(t *testing.T) {
	mgr := store.NewManager()
	quota := int64(500)
	mgr.SetCustomerMemoryQuota(quota)

	// Customer 1 fills up their quota
	s1 := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 200),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s1)

	// Customer 1 can't create more
	s2 := &store.Store{
		ID:         "store2",
		CustomerID: "cust1",
		Body:       make([]byte, 200),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s2); err != store.ErrCustomerQuotaExceeded {
		t.Errorf("cust1 should be quota limited: %v", err)
	}

	// Customer 2 should be unaffected
	s3 := &store.Store{
		ID:         "store3",
		CustomerID: "cust2",
		Body:       make([]byte, 200),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s3); err != nil {
		t.Errorf("cust2 should be able to create: %v", err)
	}
}

func TestCustomerMemoryQuota_ResetRebuildsTracking(t *testing.T) {
	mgr := store.NewManager()
	quota := int64(10000)
	mgr.SetCustomerMemoryQuota(quota)

	// Create stores
	s1 := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 1000),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s1)

	// Reset with different stores
	newStores := []*store.Store{
		{ID: "new1", CustomerID: "cust2", Body: make([]byte, 500), ExpiresAt: time.Now().Add(time.Hour)},
		{ID: "new2", CustomerID: "cust2", Body: make([]byte, 500), ExpiresAt: time.Now().Add(time.Hour)},
	}
	mgr.Reset(newStores)

	// Old customer memory should be cleared, new customer memory should be tracked
	// Verify by checking total memory
	expectedMem := int64(2 * (500 + storeOverhead))
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("memory after reset: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}
}

func TestCustomerMemoryQuota_DeleteIfExpiredAndUnlocked(t *testing.T) {
	mgr := store.NewManager()
	quota := int64(1000)
	mgr.SetCustomerMemoryQuota(quota)

	// Create expired store
	s := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 500),
		ExpiresAt:  time.Now().Add(-time.Hour), // Already expired
	}
	mgr.Create(s)

	initialMem := mgr.MemoryUsage()

	// Delete if expired
	deleted := mgr.DeleteIfExpiredAndUnlocked(s.ID)
	if !deleted {
		t.Error("expected store to be deleted")
	}

	// Memory should be freed
	if mgr.MemoryUsage() != 0 {
		t.Errorf("memory should be 0 after delete, got %d", mgr.MemoryUsage())
	}

	// Quota should be available again
	s2 := &store.Store{
		ID:         "store2",
		CustomerID: "cust1",
		Body:       make([]byte, 500),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s2); err != nil {
		t.Errorf("create after delete should succeed: %v", err)
	}

	if mgr.MemoryUsage() != initialMem {
		t.Errorf("memory should be restored: got %d, expected %d", mgr.MemoryUsage(), initialMem)
	}
}

func TestGlobalMemoryLimit_CreateEnforced(t *testing.T) {
	limit := int64(1000)
	mgr := store.NewManagerWithLimit(limit)

	// Create a store that fits
	s1 := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 500),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s1); err != nil {
		t.Fatalf("first create should succeed: %v", err)
	}

	// Second store would exceed global limit
	s2 := &store.Store{
		ID:         "store2",
		CustomerID: "cust2", // Different customer
		Body:       make([]byte, 500),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s2); err != store.ErrCapacityExceeded {
		t.Errorf("expected ErrCapacityExceeded, got %v", err)
	}
}

func TestGlobalMemoryLimit_CompleteLockEnforced(t *testing.T) {
	limit := int64(500)
	mgr := store.NewManagerWithLimit(limit)

	// Create a small store
	s := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 50),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	// Acquire lock
	_, err := mgr.AcquireLock(s.ID, s.CustomerID, "lock1", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire lock failed: %v", err)
	}

	// Try to complete with body that exceeds limit
	newBody := make([]byte, 500) // Would exceed limit
	_, err = mgr.CompleteLock(s.ID, s.CustomerID, "lock1", newBody, time.Time{})
	if err != store.ErrCapacityExceeded {
		t.Errorf("expected ErrCapacityExceeded, got %v", err)
	}
}

func TestTombstoneLimit_PerCustomer(t *testing.T) {
	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	storeMgr := store.NewManager()
	cfg := replica.DefaultConfig("test-host", "test-site")
	cfg.TombstonePerCustomerLimit = 3
	cfg.TombstoneGlobalLimit = 0 // No global limit

	replicaMgr := replica.NewManager(cfg, storeMgr, hasher)
	replicaMgr.SetRole(replica.RolePrimary)

	// Add tombstones up to limit
	for i := 0; i < 3; i++ {
		err := replicaMgr.AddTombstone(string(rune('a'+i)), "cust1")
		if err != nil {
			t.Errorf("tombstone %d should succeed: %v", i, err)
		}
	}

	// Next tombstone should fail
	err := replicaMgr.AddTombstone("d", "cust1")
	if err != replica.ErrTombstoneLimitExceeded {
		t.Errorf("expected ErrTombstoneLimitExceeded, got %v", err)
	}

	// Different customer should be able to add tombstones
	err = replicaMgr.AddTombstone("x", "cust2")
	if err != nil {
		t.Errorf("different customer should be able to add tombstone: %v", err)
	}
}

func TestTombstoneLimit_Global(t *testing.T) {
	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	storeMgr := store.NewManager()
	cfg := replica.DefaultConfig("test-host", "test-site")
	cfg.TombstonePerCustomerLimit = 0 // No per-customer limit
	cfg.TombstoneGlobalLimit = 5

	replicaMgr := replica.NewManager(cfg, storeMgr, hasher)
	replicaMgr.SetRole(replica.RolePrimary)

	// Add tombstones up to global limit (across different customers)
	for i := 0; i < 5; i++ {
		cust := "cust" + string(rune('0'+i))
		err := replicaMgr.AddTombstone(string(rune('a'+i)), cust)
		if err != nil {
			t.Errorf("tombstone %d should succeed: %v", i, err)
		}
	}

	// Next tombstone should fail regardless of customer
	err := replicaMgr.AddTombstone("z", "cust9")
	if err != replica.ErrTombstoneLimitExceeded {
		t.Errorf("expected ErrTombstoneLimitExceeded, got %v", err)
	}
}

func TestTombstoneLimit_ReplicatedBypassesLimit(t *testing.T) {
	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	storeMgr := store.NewManager()
	cfg := replica.DefaultConfig("test-host", "test-site")
	cfg.TombstonePerCustomerLimit = 2
	cfg.TombstoneGlobalLimit = 3

	replicaMgr := replica.NewManager(cfg, storeMgr, hasher)
	replicaMgr.SetRole(replica.RoleSecondary)

	// Fill up both limits using AddTombstoneReplicated (bypasses limits)
	for i := 0; i < 10; i++ {
		replicaMgr.AddTombstoneReplicated(string(rune('a'+i)), "cust1")
	}

	// Verify tombstones were added
	snapshot := replicaMgr.TombstonesSnapshot()
	if len(snapshot) != 10 {
		t.Errorf("expected 10 tombstones, got %d", len(snapshot))
	}
}

func TestTombstoneLimit_BothLimitsEnforced(t *testing.T) {
	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	storeMgr := store.NewManager()
	cfg := replica.DefaultConfig("test-host", "test-site")
	cfg.TombstonePerCustomerLimit = 2
	cfg.TombstoneGlobalLimit = 4 // Changed to 4 to make global limit trigger

	replicaMgr := replica.NewManager(cfg, storeMgr, hasher)
	replicaMgr.SetRole(replica.RolePrimary)

	// Customer 1: add 2 tombstones (hits per-customer limit)
	replicaMgr.AddTombstone("a", "cust1")
	replicaMgr.AddTombstone("b", "cust1")
	err := replicaMgr.AddTombstone("c", "cust1")
	if err != replica.ErrTombstoneLimitExceeded {
		t.Errorf("expected per-customer limit to be hit: %v", err)
	}

	// Customer 2: can still add up to per-customer limit
	replicaMgr.AddTombstone("d", "cust2")
	replicaMgr.AddTombstone("e", "cust2")
	err = replicaMgr.AddTombstone("f", "cust2")
	if err != replica.ErrTombstoneLimitExceeded {
		t.Errorf("expected per-customer limit for cust2 to be hit: %v", err)
	}

	// At this point we have 4 tombstones (a, b, d, e) which is the global limit
	// Customer 3: should hit global limit even though per-customer limit not reached
	err = replicaMgr.AddTombstone("g", "cust3")
	if err != replica.ErrTombstoneLimitExceeded {
		t.Errorf("expected global limit to be hit: %v", err)
	}
}

func TestMemoryTracking_ConsistentAcrossOperations(t *testing.T) {
	mgr := store.NewManager()

	// Track expected memory as we perform operations
	var expectedMem int64

	// Create
	s1 := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 100),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s1)
	expectedMem += int64(100 + storeOverhead)
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("after create: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}

	// Update (increase size)
	s1Updated := &store.Store{
		ID:         s1.ID,
		CustomerID: s1.CustomerID,
		Body:       make([]byte, 200),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Update(s1Updated)
	expectedMem += 100 // size delta
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("after update: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}

	// Create second store
	s2 := &store.Store{
		ID:         "store2",
		CustomerID: "cust1",
		Body:       make([]byte, 50),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s2)
	expectedMem += int64(50 + storeOverhead)
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("after second create: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}

	// Delete first store
	mgr.Delete(s1.ID, s1.CustomerID)
	expectedMem -= int64(200 + storeOverhead)
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("after delete: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}

	// ForceDelete second store
	mgr.ForceDelete(s2.ID)
	expectedMem -= int64(50 + storeOverhead)
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("after force delete: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}

	// Should be zero
	if mgr.MemoryUsage() != 0 {
		t.Errorf("final memory should be 0, got %d", mgr.MemoryUsage())
	}
}

func TestMemoryTracking_CreateOrUpdate(t *testing.T) {
	mgr := store.NewManager()

	// CreateOrUpdate new store
	s1 := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 100),
		ExpiresAt:  time.Now().Add(time.Hour),
		Version:    1,
	}
	mgr.CreateOrUpdate(s1)
	expectedMem := int64(100 + storeOverhead)
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("after create: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}

	// CreateOrUpdate existing store (update path)
	s1Updated := &store.Store{
		ID:         s1.ID,
		CustomerID: s1.CustomerID,
		Body:       make([]byte, 300),
		ExpiresAt:  time.Now().Add(time.Hour),
		Version:    2, // Higher version
	}
	mgr.CreateOrUpdate(s1Updated)
	expectedMem += 200 // size delta
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("after update via CreateOrUpdate: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}
}

func TestMemoryTracking_CreateIfNotExists(t *testing.T) {
	mgr := store.NewManager()

	s1 := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 100),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.CreateIfNotExists(s1)
	expectedMem := int64(100 + storeOverhead)
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("after create: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}

	// CreateIfNotExists for existing store - should be no-op
	s1Again := &store.Store{
		ID:         s1.ID,
		CustomerID: s1.CustomerID,
		Body:       make([]byte, 500), // Different size
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.CreateIfNotExists(s1Again)
	// Memory should not change
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("after no-op CreateIfNotExists: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}
}

func TestMemoryTracking_CounterOperations(t *testing.T) {
	mgr := store.NewManager()

	// Create counter
	_, err := mgr.CreateCounter("counter1", "shard1", "cust1", 0, nil, nil, time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("create counter failed: %v", err)
	}

	initialMem := mgr.MemoryUsage()
	if initialMem == 0 {
		t.Error("counter should have memory usage")
	}

	// Increment - value changes from {"value":0} to {"value":1}
	_, err = mgr.Increment("counter1", "cust1", 1, 1, time.Time{})
	if err != nil {
		t.Fatalf("increment failed: %v", err)
	}

	// Memory may change slightly due to JSON encoding
	// Just verify it's tracked (non-negative)
	if mgr.MemoryUsage() < 0 {
		t.Error("memory usage should be non-negative")
	}

	// Set counter
	_, err = mgr.SetCounter("counter1", "cust1", 999999999, time.Time{})
	if err != nil {
		t.Fatalf("set counter failed: %v", err)
	}

	// Memory should still be tracked
	if mgr.MemoryUsage() == 0 {
		t.Error("memory usage should be non-zero after counter operations")
	}
}

func TestMemoryTracking_CompleteLock(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 100),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)
	initialMem := mgr.MemoryUsage()

	// Acquire lock
	_, err := mgr.AcquireLock(s.ID, s.CustomerID, "lock1", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire lock failed: %v", err)
	}

	// Complete with different size body
	newBody := make([]byte, 250)
	_, err = mgr.CompleteLock(s.ID, s.CustomerID, "lock1", newBody, time.Time{})
	if err != nil {
		t.Fatalf("complete lock failed: %v", err)
	}

	expectedMem := initialMem + 150 // 250 - 100
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("after complete lock: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}
}

func TestMemoryTracking_ApplyReplicatedUpdate(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "store1",
		CustomerID: "cust1",
		Body:       make([]byte, 100),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)
	initialMem := mgr.MemoryUsage()

	// Apply replicated update
	update := &store.ReplicatedUpdate{
		StoreID:          s.ID,
		CustomerID:       s.CustomerID,
		Body:             make([]byte, 400),
		ExpiresAt:        time.Now().Add(time.Hour),
		Version:          2,
		LeaderEpoch:      1,
		LastReplicatedAt: time.Now(),
	}
	mgr.ApplyReplicatedUpdate(update)

	expectedMem := initialMem + 300 // 400 - 100
	if mgr.MemoryUsage() != expectedMem {
		t.Errorf("after apply replicated update: got %d, expected %d", mgr.MemoryUsage(), expectedMem)
	}
}

func TestTombstoneIsTombstoned(t *testing.T) {
	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	storeMgr := store.NewManager()
	cfg := replica.DefaultConfig("test-host", "test-site")
	replicaMgr := replica.NewManager(cfg, storeMgr, hasher)

	// Store should not be tombstoned initially
	if replicaMgr.IsTombstoned("store1") {
		t.Error("store should not be tombstoned initially")
	}

	// Add tombstone
	replicaMgr.AddTombstoneReplicated("store1", "cust1")

	// Store should be tombstoned
	if !replicaMgr.IsTombstoned("store1") {
		t.Error("store should be tombstoned after AddTombstoneReplicated")
	}

	// Different store should not be tombstoned
	if replicaMgr.IsTombstoned("store2") {
		t.Error("different store should not be tombstoned")
	}
}

func TestTombstoneSnapshot(t *testing.T) {
	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	storeMgr := store.NewManager()
	cfg := replica.DefaultConfig("test-host", "test-site")
	replicaMgr := replica.NewManager(cfg, storeMgr, hasher)

	// Add some tombstones
	replicaMgr.AddTombstoneReplicated("store1", "cust1")
	replicaMgr.AddTombstoneReplicated("store2", "cust1")
	replicaMgr.AddTombstoneReplicated("store3", "cust2")

	// Get snapshot
	snapshot := replicaMgr.TombstonesSnapshot()
	if len(snapshot) != 3 {
		t.Errorf("expected 3 tombstones, got %d", len(snapshot))
	}

	// Verify all tombstones are in snapshot
	found := make(map[string]bool)
	for _, entry := range snapshot {
		found[entry.StoreID] = true
		if entry.DeletedAt.IsZero() {
			t.Errorf("tombstone %s has zero deleted time", entry.StoreID)
		}
	}
	for _, id := range []string{"store1", "store2", "store3"} {
		if !found[id] {
			t.Errorf("tombstone %s not found in snapshot", id)
		}
	}
}

func TestTombstoneResetTombstones(t *testing.T) {
	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	storeMgr := store.NewManager()
	cfg := replica.DefaultConfig("test-host", "test-site")
	replicaMgr := replica.NewManager(cfg, storeMgr, hasher)

	// Add initial tombstones
	replicaMgr.AddTombstoneReplicated("old1", "cust1")
	replicaMgr.AddTombstoneReplicated("old2", "cust1")

	// Reset with new tombstones
	newTombstones := []replica.TombstoneEntry{
		{StoreID: "new1", DeletedAt: time.Now()},
		{StoreID: "new2", DeletedAt: time.Now()},
		{StoreID: "new3", DeletedAt: time.Now()},
	}
	replicaMgr.ResetTombstones(newTombstones)

	// Old tombstones should be gone
	if replicaMgr.IsTombstoned("old1") || replicaMgr.IsTombstoned("old2") {
		t.Error("old tombstones should be cleared after reset")
	}

	// New tombstones should exist
	for _, entry := range newTombstones {
		if !replicaMgr.IsTombstoned(entry.StoreID) {
			t.Errorf("new tombstone %s should exist after reset", entry.StoreID)
		}
	}

	// Snapshot should match new tombstones
	snapshot := replicaMgr.TombstonesSnapshot()
	if len(snapshot) != 3 {
		t.Errorf("expected 3 tombstones after reset, got %d", len(snapshot))
	}
}

type testRateLimitClock struct {
	mu   sync.Mutex
	time time.Time
}

func (c *testRateLimitClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.time
}

func (c *testRateLimitClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.time = c.time.Add(d)
}

func setupAPIServerWithRateLimiter(t *testing.T, limiter *ratelimit.Limiter) (string, *http.Client, func()) {
	t.Helper()

	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	replicaCfg := replica.DefaultConfig("test-host", "test-site")
	replicaMgr := replica.NewManager(replicaCfg, storeMgr, hasher)
	registryMgr := registry.NewManager()
	replicaMgr.SetRegistry(registryMgr)
	replicaMgr.SetRole(replica.RolePrimary)
	replicaMgr.Start()

	cipher := auth.NewCipher(auth.DevKeySet())
	apiCfg := &api.Config{
		Site:          "test-site",
		HostID:        "test-host",
		DefaultTTL:    time.Hour,
		MaxBodySize:   2 * 1024,
		ModifyTimeout: 500 * time.Millisecond,
		Cipher:        cipher,
	}
	apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher, limiter)

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	// Start TCP server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		server.Shutdown(ctx)
		replicaMgr.Stop()
	}

	return "http://" + listener.Addr().String(), client, cleanup
}

func TestRateLimit_APIIntegration(t *testing.T) {
	clock := &testRateLimitClock{time: time.Now()}
	limiter := ratelimit.NewLimiterWithClock(5, 5, clock) // 5 req/s, 5 burst
	defer limiter.Stop()

	baseURL, client, cleanup := setupAPIServerWithRateLimiter(t, limiter)
	defer cleanup()

	// First 5 requests should succeed (burst)
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("test"))
		req.Header.Set("X-Customer-ID", "customer1")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("request %d: expected 200, got %d", i, resp.StatusCode)
		}
	}

	// 6th request should be rate limited
	req, _ := http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("test"))
	req.Header.Set("X-Customer-ID", "customer1")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("rate limited request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", resp.StatusCode)
	}
	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter != "1" {
		t.Errorf("expected Retry-After: 1, got %s", retryAfter)
	}

	// Advance time to refill tokens
	clock.Advance(1 * time.Second)

	// Should be able to make requests again
	req, _ = http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("test"))
	req.Header.Set("X-Customer-ID", "customer1")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("request after refill failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 after refill, got %d", resp.StatusCode)
	}
}

func TestRateLimit_PerCustomerIsolation(t *testing.T) {
	clock := &testRateLimitClock{time: time.Now()}
	limiter := ratelimit.NewLimiterWithClock(2, 2, clock) // 2 req/s, 2 burst
	defer limiter.Stop()

	baseURL, client, cleanup := setupAPIServerWithRateLimiter(t, limiter)
	defer cleanup()

	// Customer 1 uses all their tokens
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("test"))
		req.Header.Set("X-Customer-ID", "customer1")
		resp, _ := client.Do(req)
		resp.Body.Close()
	}

	// Customer 1 should be rate limited
	req, _ := http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("test"))
	req.Header.Set("X-Customer-ID", "customer1")
	resp, _ := client.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("customer1 should be rate limited, got %d", resp.StatusCode)
	}

	// Customer 2 should still have tokens
	req, _ = http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("test"))
	req.Header.Set("X-Customer-ID", "customer2")
	resp, _ = client.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("customer2 should not be rate limited, got %d", resp.StatusCode)
	}
}

func TestRateLimit_DisabledWhenNil(t *testing.T) {
	// No rate limiter (nil)
	baseURL, client, cleanup := setupAPIServerWithRateLimiter(t, nil)
	defer cleanup()

	// Should be able to make many requests without rate limiting
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("test"))
		req.Header.Set("X-Customer-ID", "customer1")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			t.Errorf("request %d: should not be rate limited when limiter is nil", i)
		}
	}
}

func TestRateLimit_AllEndpointsProtected(t *testing.T) {
	clock := &testRateLimitClock{time: time.Now()}
	limiter := ratelimit.NewLimiterWithClock(1, 1, clock) // 1 req/s, 1 burst
	defer limiter.Stop()

	baseURL, client, cleanup := setupAPIServerWithRateLimiter(t, limiter)
	defer cleanup()

	// Use up the token with a create
	req, _ := http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("test"))
	req.Header.Set("X-Customer-ID", "customer1")
	resp, _ := client.Do(req)
	storeID, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// All these endpoints should be rate limited
	endpoints := []struct {
		method string
		path   string
	}{
		{"POST", "/api/v1/create"},
		{"POST", "/api/v1/snapshot/" + string(storeID)},
		{"POST", "/api/v1/begin-modify/" + string(storeID)},
		{"POST", "/api/v1/delete/" + string(storeID)},
	}

	for _, ep := range endpoints {
		req, _ := http.NewRequest(ep.method, baseURL+ep.path, strings.NewReader("test"))
		req.Header.Set("X-Customer-ID", "customer1")
		resp, _ := client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Errorf("%s %s: expected 429, got %d", ep.method, ep.path, resp.StatusCode)
		}
	}
}

func TestRateLimit_Stats(t *testing.T) {
	clock := &testRateLimitClock{time: time.Now()}
	limiter := ratelimit.NewLimiterWithClock(10, 10, clock)
	defer limiter.Stop()

	// Initially no buckets
	if limiter.Stats() != 0 {
		t.Errorf("expected 0 buckets initially, got %d", limiter.Stats())
	}

	// Make requests from different customers
	limiter.Allow("customer1")
	limiter.Allow("customer2")
	limiter.Allow("customer3")

	if limiter.Stats() != 3 {
		t.Errorf("expected 3 buckets, got %d", limiter.Stats())
	}

	// Same customer doesn't add new bucket
	limiter.Allow("customer1")
	if limiter.Stats() != 3 {
		t.Errorf("expected 3 buckets (no change), got %d", limiter.Stats())
	}
}

func TestCustomerMemoryQuota_APIIntegration(t *testing.T) {
	storeMgr := store.NewManager()
	// storeOverhead is 256 bytes per store
	// So a 500-byte quota allows one small store (~300 bytes with overhead)
	quota := int64(500)
	storeMgr.SetCustomerMemoryQuota(quota)

	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	replicaCfg := replica.DefaultConfig("test-host", "test-site")
	replicaMgr := replica.NewManager(replicaCfg, storeMgr, hasher)
	registryMgr := registry.NewManager()
	replicaMgr.SetRegistry(registryMgr)
	replicaMgr.SetRole(replica.RolePrimary)
	replicaMgr.Start()
	defer replicaMgr.Stop()

	cipher := auth.NewCipher(auth.DevKeySet())
	apiCfg := &api.Config{
		Site:          "test-site",
		HostID:        "test-host",
		DefaultTTL:    time.Hour,
		MaxBodySize:   2 * 1024,
		ModifyTimeout: 500 * time.Millisecond,
		Cipher:        cipher,
	}
	apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher, nil)

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()

	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := "http://" + listener.Addr().String()

	// Create a small store - should succeed (10 bytes + 256 overhead = 266 bytes < 500)
	req, _ := http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("small data"))
	req.Header.Set("X-Customer-ID", "customer1")
	resp, _ := client.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("first create should succeed, got %d", resp.StatusCode)
	}

	// Create another store that would exceed quota for customer1
	// Customer1 has ~266 bytes used; another ~266 bytes would be ~532 > 500
	req, _ = http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("more data!"))
	req.Header.Set("X-Customer-ID", "customer1")
	resp, _ = client.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusInsufficientStorage {
		t.Errorf("second create should fail with 507, got %d: %s", resp.StatusCode, body)
	}

	// Different customer should be able to create (has their own quota)
	req, _ = http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("small data"))
	req.Header.Set("X-Customer-ID", "customer2")
	resp, _ = client.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("different customer should be able to create, got %d", resp.StatusCode)
	}
}
