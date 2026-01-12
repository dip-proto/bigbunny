package test

import (
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
)

func TestIsLockStateUnknown_InitialPrimary(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)
	mgr.SetRole(replica.RolePrimary) // initial startup as primary

	// Should NOT be unknown on initial startup (no prior lock state to be unknown about)
	unknown, _ := mgr.IsLockStateUnknown("any-store")
	if unknown {
		t.Error("expected lock state to be known on initial startup as primary")
	}
}

func TestIsLockStateUnknown_AfterPromotion(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")
	config.ModifyTimeout = 100 * time.Millisecond // short for testing

	mgr := replica.NewManager(config, storeMgr, hasher)

	// Start as secondary
	mgr.SetRole(replica.RoleSecondary)

	// Promote to primary (simulates failover)
	mgr.SetRole(replica.RolePrimary)

	// Should be unknown immediately after promotion
	unknown, retryAfter := mgr.IsLockStateUnknown("any-store")
	if !unknown {
		t.Error("expected lock state to be unknown immediately after promotion")
	}
	if retryAfter < 1*time.Second {
		t.Errorf("expected retryAfter >= 1s, got %v", retryAfter)
	}

	// Wait for ModifyTimeout to pass
	time.Sleep(config.ModifyTimeout + 10*time.Millisecond)

	// Should now be known
	unknown, _ = mgr.IsLockStateUnknown("any-store")
	if unknown {
		t.Error("expected lock state to be known after ModifyTimeout passed")
	}
}

func TestIsLockStateUnknown_Secondary(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)
	mgr.SetRole(replica.RoleSecondary)

	// Secondary should never report lock state as unknown (not relevant for secondaries)
	unknown, _ := mgr.IsLockStateUnknown("any-store")
	if unknown {
		t.Error("secondary should not report lock state unknown")
	}
}

func TestApplyReplication_LockAcquired(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)
	mgr.SetRole(replica.RoleSecondary)

	// Create a store first
	s := &store.Store{
		ID:         "v1:local:shard1:test123",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	storeMgr.Create(s)

	// Apply lock acquired replication message
	msg := &replica.ReplicationMessage{
		Type:        replica.MsgLockAcquired,
		StoreID:     s.ID,
		CustomerID:  s.CustomerID,
		LockID:      "replicated-lock",
		Timestamp:   time.Now(),
		LockTimeout: 500 * time.Millisecond,
		LeaderEpoch: 1,
	}

	err := mgr.ApplyReplication(msg)
	if err != nil {
		t.Fatalf("ApplyReplication failed: %v", err)
	}

	// Verify lock is set - try to acquire should fail
	_, err = storeMgr.AcquireLock(s.ID, s.CustomerID, "another-lock", 500*time.Millisecond)
	if err != store.ErrStoreLocked {
		t.Errorf("expected ErrStoreLocked after replicating lock, got %v", err)
	}
}

func TestApplyReplication_LockReleased(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)
	mgr.SetRole(replica.RoleSecondary)

	// Create a store with a lock
	s := &store.Store{
		ID:         "v1:local:shard1:release-test",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	storeMgr.Create(s)
	storeMgr.SetLock(s.ID, "lock-to-release", time.Now(), 500*time.Millisecond)

	// Apply lock released replication message
	msg := &replica.ReplicationMessage{
		Type:        replica.MsgLockReleased,
		StoreID:     s.ID,
		CustomerID:  s.CustomerID,
		LockID:      "lock-to-release",
		LeaderEpoch: 1,
	}

	err := mgr.ApplyReplication(msg)
	if err != nil {
		t.Fatalf("ApplyReplication failed: %v", err)
	}

	// Verify lock is cleared - acquire should succeed
	_, err = storeMgr.AcquireLock(s.ID, s.CustomerID, "new-lock", 500*time.Millisecond)
	if err != nil {
		t.Errorf("expected acquire to succeed after replicated release, got %v", err)
	}
}

func TestApplyReplication_UpdateClearsLock(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)
	mgr.SetRole(replica.RoleSecondary)

	// Create a store with a lock (simulating a modify in progress)
	s := &store.Store{
		ID:         "v1:local:shard1:update-clear-test",
		CustomerID: "cust1",
		Body:       []byte("old-data"),
		ExpiresAt:  time.Now().Add(time.Hour),
		Version:    1,
	}
	storeMgr.Create(s)
	storeMgr.SetLock(s.ID, "modify-lock", time.Now(), 500*time.Millisecond)

	// Apply update (complete-modify) replication - should clear the lock
	msg := &replica.ReplicationMessage{
		Type:        replica.MsgUpdateStore,
		StoreID:     s.ID,
		CustomerID:  s.CustomerID,
		Body:        []byte("new-data"),
		ExpiresAt:   s.ExpiresAt,
		Version:     2,
		LeaderEpoch: 1,
	}

	err := mgr.ApplyReplication(msg)
	if err != nil {
		t.Fatalf("ApplyReplication failed: %v", err)
	}

	// Verify lock is cleared unconditionally
	_, err = storeMgr.AcquireLock(s.ID, s.CustomerID, "new-lock", 500*time.Millisecond)
	if err != nil {
		t.Errorf("expected acquire to succeed after replicated update, got %v", err)
	}

	// Verify body was updated
	got, _ := storeMgr.Get(s.ID, s.CustomerID)
	if string(got.Body) != "new-data" {
		t.Errorf("body not updated: got %q", got.Body)
	}
}

func TestApplyReplication_StaleEpochRejected(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)
	// Set epoch to 5
	mgr.SetRole(replica.RoleSecondary)
	for range 4 {
		mgr.SetRole(replica.RolePrimary)
		mgr.SetRole(replica.RoleSecondary)
	}
	mgr.SetRole(replica.RolePrimary)

	// Create a store
	s := &store.Store{
		ID:         "v1:local:shard1:stale-epoch-test",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	storeMgr.Create(s)

	// Try to apply message with stale epoch
	msg := &replica.ReplicationMessage{
		Type:        replica.MsgLockAcquired,
		StoreID:     s.ID,
		CustomerID:  s.CustomerID,
		LockID:      "stale-lock",
		LeaderEpoch: 1, // stale
	}

	err := mgr.ApplyReplication(msg)
	if err == nil {
		t.Error("expected error for stale epoch")
	}
}

// Phase C.5: Recovery tests

func TestApplyReplication_RejectsWhenJoining(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)
	// Start in JOINING state via StartRecovery (without peers, it stays in JOINING)
	mgr.StartRecovery()

	if mgr.Role() != replica.RoleJoining {
		t.Fatalf("expected JOINING role, got %v", mgr.Role())
	}

	// Try to apply replication
	msg := &replica.ReplicationMessage{
		Type:        replica.MsgCreateStore,
		StoreID:     "v1:local:shard1:test",
		CustomerID:  "cust1",
		Body:        []byte("data"),
		ExpiresAt:   time.Now().Add(time.Hour),
		LeaderEpoch: 1,
	}

	err := mgr.ApplyReplication(msg)
	if err != replica.ErrJoinInProgress {
		t.Errorf("expected ErrJoinInProgress, got %v", err)
	}
}

func TestHandleHeartbeat_DemotesToJoining(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
		{ID: "host2", Address: "localhost:8082", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)
	mgr.SetRole(replica.RolePrimary) // Start as primary at epoch 1

	initialEpoch := mgr.LeaderEpoch()

	// Receive heartbeat with higher epoch
	hb := &replica.HeartbeatMessage{
		HostID:      "host2",
		LeaderEpoch: initialEpoch + 5,
		Timestamp:   time.Now(),
	}

	mgr.HandleHeartbeat(hb)

	// Should transition to JOINING (not SECONDARY)
	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	if mgr.Role() != replica.RoleJoining {
		t.Errorf("expected JOINING after higher epoch heartbeat, got %v", mgr.Role())
	}

	// Epoch should be updated
	if mgr.LeaderEpoch() != initialEpoch+5 {
		t.Errorf("expected epoch %d, got %d", initialEpoch+5, mgr.LeaderEpoch())
	}
}

func TestHandleHeartbeat_SecondaryToJoining(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)
	mgr.SetRole(replica.RoleSecondary)

	initialEpoch := mgr.LeaderEpoch()

	// Receive heartbeat with higher epoch
	hb := &replica.HeartbeatMessage{
		HostID:      "host2",
		LeaderEpoch: initialEpoch + 3,
		Timestamp:   time.Now(),
	}

	mgr.HandleHeartbeat(hb)

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	if mgr.Role() != replica.RoleJoining {
		t.Errorf("expected JOINING after higher epoch heartbeat, got %v", mgr.Role())
	}
}

func TestTombstonesSnapshot(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)

	// Add some tombstones
	mgr.AddTombstone("store1")
	mgr.AddTombstone("store2")
	mgr.AddTombstone("store3")

	// Get snapshot
	snapshot := mgr.TombstonesSnapshot()
	if len(snapshot) != 3 {
		t.Errorf("expected 3 tombstones in snapshot, got %d", len(snapshot))
	}

	// Verify tombstones exist
	foundStores := make(map[string]bool)
	for _, ts := range snapshot {
		foundStores[ts.StoreID] = true
	}
	for _, id := range []string{"store1", "store2", "store3"} {
		if !foundStores[id] {
			t.Errorf("tombstone %s not found in snapshot", id)
		}
	}
}

func TestResetTombstones(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)

	// Add initial tombstones
	mgr.AddTombstone("old1")
	mgr.AddTombstone("old2")

	// Reset with new tombstones
	newTombstones := []replica.TombstoneEntry{
		{StoreID: "new1", DeletedAt: time.Now()},
		{StoreID: "new2", DeletedAt: time.Now()},
		{StoreID: "new3", DeletedAt: time.Now()},
	}
	mgr.ResetTombstones(newTombstones)

	// Old tombstones should be gone
	if mgr.IsTombstoned("old1") {
		t.Error("old tombstone should be removed after reset")
	}

	// New tombstones should exist
	for _, ts := range newTombstones {
		if !mgr.IsTombstoned(ts.StoreID) {
			t.Errorf("new tombstone %s should exist after reset", ts.StoreID)
		}
	}
}

func TestHandleHeartbeat_RetriesRecoveryWhenJoining(t *testing.T) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
		{ID: "host2", Address: "localhost:8082", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")
	config := replica.DefaultConfig("host1", "local")

	mgr := replica.NewManager(config, storeMgr, hasher)

	// Start in JOINING state (simulating failed initial recovery)
	// Note: StartRecovery doesn't call doRecovery if no primary found
	mgr.StartRecovery()

	if mgr.Role() != replica.RoleJoining {
		t.Fatalf("expected JOINING role, got %v", mgr.Role())
	}

	initialAttempts := mgr.RecoveryAttempts()

	// First heartbeat - should trigger recovery attempt
	hb1 := &replica.HeartbeatMessage{
		HostID:      "host2",
		LeaderEpoch: 1,
		Timestamp:   time.Now(),
	}
	mgr.HandleHeartbeat(hb1)

	// Give goroutine time to complete
	time.Sleep(50 * time.Millisecond)

	attemptsAfterFirst := mgr.RecoveryAttempts()
	if attemptsAfterFirst <= initialAttempts {
		t.Errorf("expected recovery attempt after first heartbeat: initial=%d, after=%d",
			initialAttempts, attemptsAfterFirst)
	}

	// Still JOINING (recovery will fail - no actual server)
	if mgr.Role() != replica.RoleJoining {
		t.Errorf("expected still JOINING after first heartbeat, got %v", mgr.Role())
	}

	// Wait for retry interval
	time.Sleep(1100 * time.Millisecond)

	// Second heartbeat - should trigger retry since enough time passed
	hb2 := &replica.HeartbeatMessage{
		HostID:      "host2",
		LeaderEpoch: 1,
		Timestamp:   time.Now(),
	}
	mgr.HandleHeartbeat(hb2)

	// Give goroutine time to complete
	time.Sleep(50 * time.Millisecond)

	attemptsAfterSecond := mgr.RecoveryAttempts()
	if attemptsAfterSecond <= attemptsAfterFirst {
		t.Errorf("expected recovery retry after second heartbeat: first=%d, second=%d",
			attemptsAfterFirst, attemptsAfterSecond)
	}

	// Still JOINING (recovery will fail - no actual server)
	if mgr.Role() != replica.RoleJoining {
		t.Errorf("expected still JOINING after retry heartbeat, got %v", mgr.Role())
	}
}
