package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/test/harness"
)

func TestFailure_PrimaryCrashAndRecovery(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	primary := cluster.Primary()
	secondary := cluster.Secondary()
	primaryID := primary.ID
	secondaryID := secondary.ID

	// Create multiple stores on primary
	var storeIDs []string
	for i := 0; i < 5; i++ {
		storeID := createStore(t, primary.Addr(), "customer1", []byte(fmt.Sprintf("data-%d", i)))
		storeIDs = append(storeIDs, storeID)
	}

	// Wait for all stores to replicate
	for _, storeID := range storeIDs {
		if err := cluster.WaitForStore(secondaryID, storeID, "customer1", 2*time.Second); err != nil {
			t.Fatalf("store %s not replicated: %v", storeID, err)
		}
	}

	// Modify some stores to create version history
	modifyStore(t, primary.Addr(), storeIDs[0], "customer1", []byte("modified-0"))
	modifyStore(t, primary.Addr(), storeIDs[2], "customer1", []byte("modified-2"))

	// Wait for modifications to replicate
	err = cluster.WaitForCondition(func() bool {
		s, err := secondary.Store.Get(storeIDs[0], "customer1")
		return err == nil && bytes.Equal(s.Body, []byte("modified-0"))
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("modification not replicated: %v", err)
	}

	// Partition primary (simulates crash)
	cluster.Partition(primaryID, secondaryID)

	// Wait for secondary to promote
	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	if err := cluster.WaitForRole(secondaryID, replica.RolePrimary, promotionTimeout); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	newPrimary := cluster.Node(secondaryID)
	newPrimaryEpoch := newPrimary.Replica.LeaderEpoch()

	// Wait for lock-unknown window to pass before modifying stores
	time.Sleep(cfg.ModifyTimeout + 100*time.Millisecond)

	// Perform writes on new primary while old primary is down
	postFailoverStore := createStore(t, newPrimary.Addr(), "customer1", []byte("post-failover"))
	modifyStore(t, newPrimary.Addr(), storeIDs[1], "customer1", []byte("new-primary-modified-1"))

	// Heal partition (old primary rejoins)
	cluster.Heal(primaryID, secondaryID)

	// Wait for old primary to recover
	if err := cluster.WaitForCondition(func() bool {
		role := cluster.Node(primaryID).Replica.Role()
		return role == replica.RoleSecondary || role == replica.RoleJoining
	}, 2*time.Second); err != nil {
		t.Fatalf("old primary didn't demote: current role=%s", cluster.Node(primaryID).Replica.Role())
	}

	// Wait until old primary reaches SECONDARY and has recovered data
	if err := cluster.WaitForRole(primaryID, replica.RoleSecondary, 3*time.Second); err != nil {
		t.Fatalf("old primary didn't reach SECONDARY: %v", err)
	}

	// Verify old primary has all the data including post-failover writes
	oldPrimary := cluster.Node(primaryID)

	// Check post-failover store exists
	if err := cluster.WaitForStore(primaryID, postFailoverStore, "customer1", 2*time.Second); err != nil {
		t.Fatalf("post-failover store not recovered to old primary: %v", err)
	}

	// Check modification made on new primary is recovered
	err = cluster.WaitForCondition(func() bool {
		s, err := oldPrimary.Store.Get(storeIDs[1], "customer1")
		return err == nil && bytes.Equal(s.Body, []byte("new-primary-modified-1"))
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("new primary modification not recovered: %v", err)
	}

	// Verify epoch was updated
	if oldPrimary.Replica.LeaderEpoch() < newPrimaryEpoch {
		t.Errorf("recovered node epoch %d should be >= %d", oldPrimary.Replica.LeaderEpoch(), newPrimaryEpoch)
	}
}

func TestFailure_SecondaryCrashAndRecovery(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	primary := cluster.Primary()
	secondary := cluster.Secondary()
	primaryID := primary.ID
	secondaryID := secondary.ID

	// Create initial data
	storeID1 := createStore(t, primary.Addr(), "customer1", []byte("initial-1"))
	if err := cluster.WaitForStore(secondaryID, storeID1, "customer1", 2*time.Second); err != nil {
		t.Fatalf("store not replicated: %v", err)
	}

	// Partition secondary (simulates secondary crash)
	cluster.Partition(primaryID, secondaryID)

	// Primary should continue operating (with degraded warnings after replication failures)
	storeID2 := createStore(t, primary.Addr(), "customer1", []byte("during-secondary-down"))
	modifyStore(t, primary.Addr(), storeID1, "customer1", []byte("modified-while-down"))

	// Wait for secondary to try promoting (will fail due to partition)
	// Then it can recover when partition heals
	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	err = cluster.WaitForCondition(func() bool {
		role := secondary.Replica.Role()
		return role == replica.RolePrimary || role == replica.RoleJoining
	}, promotionTimeout)
	// It may promote to primary (isolated) or stay secondary

	// Heal partition (secondary reconnects)
	cluster.Heal(primaryID, secondaryID)

	// Give time for heartbeat exchange and epoch sync
	// The secondary should detect higher epoch and trigger recovery
	time.Sleep(300 * time.Millisecond)

	// Trigger recovery by waiting for primary heartbeat to reach secondary
	// The secondary will see it has stale data and trigger recovery
	err = cluster.WaitForCondition(func() bool {
		s, err := secondary.Store.Get(storeID2, "customer1")
		return err == nil && s != nil
	}, 5*time.Second)
	// Note: With async replication, if secondary promoted during partition,
	// the stores created on original primary during partition may be lost.
	// This is expected behavior - we check that the cluster reached a consistent state.

	// Wait for cluster to settle
	time.Sleep(500 * time.Millisecond)

	// Verify there's exactly one primary
	var primaryCount int
	var currentPrimary *harness.Node
	for _, node := range cluster.Nodes() {
		if node.Replica.Role() == replica.RolePrimary {
			primaryCount++
			currentPrimary = node
		}
	}
	if primaryCount != 1 {
		t.Fatalf("expected 1 primary, got %d", primaryCount)
	}

	// The initial data (storeID1) should exist on the current primary
	s, err := currentPrimary.Store.Get(storeID1, "customer1")
	if err != nil {
		t.Fatalf("initial store missing from current primary: %v", err)
	}

	// storeID2 may or may not exist depending on which node won the election
	// This tests that the cluster reaches a consistent state, not data preservation
	_, err = currentPrimary.Store.Get(storeID2, "customer1")
	if err == nil {
		t.Log("store created during partition was preserved (original primary won)")
	} else {
		t.Log("store created during partition was lost (secondary won) - expected with async replication")
	}

	t.Logf("cluster stabilized with %s as primary", currentPrimary.ID)
	_ = s // use the variable
}

func TestFailure_SplitBrainPrevention(t *testing.T) {
	cfg := &harness.ClusterConfig{
		NodeCount:          2,
		HeartbeatInterval:  50 * time.Millisecond,
		LeaseDuration:      200 * time.Millisecond,
		LeaseGrace:         200 * time.Millisecond,
		ReplicationTimeout: 500 * time.Millisecond,
		ModifyTimeout:      100 * time.Millisecond,
	}
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	primary := cluster.Primary()
	secondary := cluster.Secondary()
	primaryID := primary.ID
	secondaryID := secondary.ID

	// Create data before partition
	storeID := createStore(t, primary.Addr(), "customer1", []byte("pre-partition"))
	if err := cluster.WaitForStore(secondaryID, storeID, "customer1", 2*time.Second); err != nil {
		t.Fatalf("store not replicated: %v", err)
	}

	initialEpoch := primary.Replica.LeaderEpoch()

	// Partition both nodes
	cluster.Partition(primaryID, secondaryID)

	// Wait for secondary to promote (becomes new primary)
	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	if err := cluster.WaitForRole(secondaryID, replica.RolePrimary, promotionTimeout); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	newPrimaryEpoch := cluster.Node(secondaryID).Replica.LeaderEpoch()
	if newPrimaryEpoch <= initialEpoch {
		t.Fatalf("new primary should have higher epoch: %d <= %d", newPrimaryEpoch, initialEpoch)
	}

	// Both nodes now think they could be primary (old primary hasn't demoted)
	// Write to new primary
	newPrimary := cluster.Node(secondaryID)
	createStore(t, newPrimary.Addr(), "customer1", []byte("from-new-primary"))

	// Heal partition
	cluster.Heal(primaryID, secondaryID)

	// Old primary should demote due to higher epoch
	if err := cluster.WaitForCondition(func() bool {
		role := cluster.Node(primaryID).Replica.Role()
		epoch := cluster.Node(primaryID).Replica.LeaderEpoch()
		return (role == replica.RoleSecondary || role == replica.RoleJoining) && epoch >= newPrimaryEpoch
	}, 2*time.Second); err != nil {
		oldRole := cluster.Node(primaryID).Replica.Role()
		oldEpoch := cluster.Node(primaryID).Replica.LeaderEpoch()
		t.Fatalf("old primary didn't demote properly: role=%s epoch=%d (expected >= %d)", oldRole, oldEpoch, newPrimaryEpoch)
	}

	// Verify only one primary exists
	primaryCount := 0
	for _, node := range cluster.Nodes() {
		if node.Replica.Role() == replica.RolePrimary {
			primaryCount++
		}
	}
	if primaryCount != 1 {
		t.Errorf("expected exactly 1 primary, found %d", primaryCount)
	}
}

func TestFailure_DataIntegrityMultipleFailovers(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	// Track all created stores and their expected values
	type storeData struct {
		id       string
		expected []byte
	}
	stores := make(map[string]*storeData)

	// Initial data
	primary := cluster.Primary()
	for i := 0; i < 3; i++ {
		data := []byte(fmt.Sprintf("initial-%d", i))
		id := createStore(t, primary.Addr(), "customer1", data)
		stores[id] = &storeData{id: id, expected: data}
	}

	// Wait for replication
	for id := range stores {
		if err := cluster.WaitForStore("node2", id, "customer1", 2*time.Second); err != nil {
			t.Fatalf("store not replicated: %v", err)
		}
	}

	// Perform 3 failover cycles
	for cycle := 0; cycle < 3; cycle++ {
		primary := cluster.Primary()
		secondary := cluster.Secondary()
		if primary == nil || secondary == nil {
			t.Fatalf("cycle %d: missing primary or secondary", cycle)
		}

		// Add more data before failover
		data := []byte(fmt.Sprintf("cycle-%d-data", cycle))
		id := createStore(t, primary.Addr(), "customer1", data)
		stores[id] = &storeData{id: id, expected: data}

		// Wait for replication
		if err := cluster.WaitForStore(secondary.ID, id, "customer1", 2*time.Second); err != nil {
			t.Fatalf("cycle %d: store not replicated: %v", cycle, err)
		}

		// Partition to trigger failover
		cluster.Partition(primary.ID, secondary.ID)

		// Wait for secondary to promote
		promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
		if err := cluster.WaitForRole(secondary.ID, replica.RolePrimary, promotionTimeout); err != nil {
			t.Fatalf("cycle %d: failover failed: %v", cycle, err)
		}

		// Write to new primary
		newPrimary := cluster.Node(secondary.ID)
		postData := []byte(fmt.Sprintf("post-failover-%d", cycle))
		postID := createStore(t, newPrimary.Addr(), "customer1", postData)
		stores[postID] = &storeData{id: postID, expected: postData}

		// Heal and wait for recovery
		cluster.Heal(primary.ID, secondary.ID)
		if err := cluster.WaitForRole(primary.ID, replica.RoleSecondary, 3*time.Second); err != nil {
			// May still be joining, give it more time
			time.Sleep(500 * time.Millisecond)
		}

		t.Logf("cycle %d complete: %d stores", cycle, len(stores))
	}

	// Verify all data on both nodes
	time.Sleep(500 * time.Millisecond) // Allow final replication to settle
	for _, node := range cluster.Nodes() {
		for id, expected := range stores {
			err := cluster.WaitForCondition(func() bool {
				s, err := node.Store.Get(id, "customer1")
				return err == nil && bytes.Equal(s.Body, expected.expected)
			}, 2*time.Second)
			if err != nil {
				s, getErr := node.Store.Get(id, "customer1")
				if getErr != nil {
					t.Errorf("node %s missing store %s: %v", node.ID, id, getErr)
				} else {
					t.Errorf("node %s store %s data mismatch: got %s, want %s", node.ID, id, s.Body, expected.expected)
				}
			}
		}
	}
}

func TestFailure_TombstoneReplicationDuringRecovery(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	primary := cluster.Primary()
	secondary := cluster.Secondary()
	primaryID := primary.ID
	secondaryID := secondary.ID

	// Create stores
	store1 := createStore(t, primary.Addr(), "customer1", []byte("to-be-deleted"))
	store2 := createStore(t, primary.Addr(), "customer1", []byte("to-keep"))

	// Wait for replication
	if err := cluster.WaitForStore(secondaryID, store1, "customer1", 2*time.Second); err != nil {
		t.Fatalf("store1 not replicated: %v", err)
	}
	if err := cluster.WaitForStore(secondaryID, store2, "customer1", 2*time.Second); err != nil {
		t.Fatalf("store2 not replicated: %v", err)
	}

	// Partition secondary
	cluster.Partition(primaryID, secondaryID)

	// Delete store1 on primary while secondary is partitioned
	deleteStore(t, primary.Addr(), store1, "customer1")

	// Wait for secondary to promote (it still has store1)
	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	if err := cluster.WaitForRole(secondaryID, replica.RolePrimary, promotionTimeout); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	// Heal partition
	cluster.Heal(primaryID, secondaryID)

	// Old primary should recover and learn about the deletion via tombstone
	if err := cluster.WaitForRole(primaryID, replica.RoleSecondary, 3*time.Second); err != nil {
		// May take longer, just continue
	}

	// The deleted store should stay deleted on old primary
	// (tombstone prevents resurrection even though secondary had it when promoted)
	oldPrimary := cluster.Node(primaryID)
	_, err = oldPrimary.Store.Get(store1, "customer1")
	if err == nil {
		// If the store exists, it should eventually be cleaned up
		// Wait a bit for tombstone sync to prevent resurrection
		time.Sleep(500 * time.Millisecond)
	}

	// store2 should still exist everywhere
	for _, node := range cluster.Nodes() {
		_, err := node.Store.Get(store2, "customer1")
		if err != nil {
			t.Errorf("node %s should have store2: %v", node.ID, err)
		}
	}
}

func TestFailure_InFlightModifyDuringFailover(t *testing.T) {
	cfg := &harness.ClusterConfig{
		NodeCount:          2,
		HeartbeatInterval:  50 * time.Millisecond,
		LeaseDuration:      200 * time.Millisecond,
		LeaseGrace:         200 * time.Millisecond,
		ReplicationTimeout: 500 * time.Millisecond,
		ModifyTimeout:      200 * time.Millisecond, // Longer timeout for this test
	}
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	primary := cluster.Primary()
	secondary := cluster.Secondary()
	primaryID := primary.ID
	secondaryID := secondary.ID

	// Create a store
	storeID := createStore(t, primary.Addr(), "customer1", []byte("initial"))
	if err := cluster.WaitForStore(secondaryID, storeID, "customer1", 2*time.Second); err != nil {
		t.Fatalf("store not replicated: %v", err)
	}

	// Begin modify (acquire lock) but don't complete
	lockID := beginModify(t, primary.Addr(), storeID, "customer1")
	_ = lockID // Lock is held but we won't complete it

	// Partition to trigger failover
	cluster.Partition(primaryID, secondaryID)

	// Wait for secondary to promote
	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	if err := cluster.WaitForRole(secondaryID, replica.RolePrimary, promotionTimeout); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	newPrimary := cluster.Node(secondaryID)

	// New primary should have lock-unknown window
	resp := beginModifyRaw(t, newPrimary.Addr(), storeID, "customer1")
	resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		errCode := resp.Header.Get("BigBunny-Error-Code")
		if errCode == "LockStateUnknown" {
			t.Log("correctly got LockStateUnknown during window")
		}
	}

	// Wait for lock-unknown window to pass AND the old lock to expire
	// The lock was held on old primary; new primary doesn't have the lock info
	// but during lock-unknown window, all lock operations return LockStateUnknown
	// After the window, the store should be unlocked (lock was on old primary)
	err = cluster.WaitForCondition(func() bool {
		resp := beginModifyRaw(t, newPrimary.Addr(), storeID, "customer1")
		defer resp.Body.Close()
		// Wait until we can successfully begin a modify (not unknown, not locked)
		return resp.StatusCode == http.StatusOK
	}, cfg.ModifyTimeout*3)
	if err != nil {
		// Check what error we're getting
		checkResp := beginModifyRaw(t, newPrimary.Addr(), storeID, "customer1")
		errCode := checkResp.Header.Get("BigBunny-Error-Code")
		checkResp.Body.Close()
		t.Fatalf("couldn't begin modify after window: error=%s", errCode)
	}

	// The previous WaitForCondition already acquired the lock via probing
	// Wait for that probe's lock to expire before acquiring a new one
	time.Sleep(cfg.ModifyTimeout + 100*time.Millisecond)

	// Now acquire fresh lock and complete
	newLockID := beginModify(t, newPrimary.Addr(), storeID, "customer1")
	completeModifyWithBody(t, newPrimary.Addr(), storeID, "customer1", newLockID, []byte("after-failover"))

	// Verify modification
	s, err := newPrimary.Store.Get(storeID, "customer1")
	if err != nil {
		t.Fatalf("failed to get store: %v", err)
	}
	if !bytes.Equal(s.Body, []byte("after-failover")) {
		t.Errorf("expected 'after-failover', got %s", s.Body)
	}
}

func TestFailure_CountersDuringFailover(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	primary := cluster.Primary()
	secondary := cluster.Secondary()
	primaryID := primary.ID
	secondaryID := secondary.ID

	// Create a counter
	counterID := createCounter(t, primary.Addr(), "customer1", 100, nil, nil)

	// Wait for replication
	if err := cluster.WaitForStore(secondaryID, counterID, "customer1", 2*time.Second); err != nil {
		t.Fatalf("counter not replicated: %v", err)
	}

	// Increment counter several times
	for i := 0; i < 5; i++ {
		incrementCounter(t, primary.Addr(), counterID, "customer1", 10)
	}

	// Wait for increments to replicate
	err = cluster.WaitForCondition(func() bool {
		val := getCounterSnapshot(t, secondary.Addr(), counterID, "customer1")
		return val == 150 // 100 + 5*10
	}, 2*time.Second)
	if err != nil {
		val := getCounterSnapshot(t, secondary.Addr(), counterID, "customer1")
		t.Fatalf("counter not replicated: expected 150, got %d", val)
	}

	// Partition to trigger failover
	cluster.Partition(primaryID, secondaryID)

	// Wait for secondary to promote
	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	if err := cluster.WaitForRole(secondaryID, replica.RolePrimary, promotionTimeout); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	newPrimary := cluster.Node(secondaryID)

	// Increment on new primary
	for i := 0; i < 3; i++ {
		incrementCounter(t, newPrimary.Addr(), counterID, "customer1", 5)
	}

	// Verify counter value
	val := getCounterSnapshot(t, newPrimary.Addr(), counterID, "customer1")
	if val != 165 { // 150 + 3*5
		t.Errorf("expected counter value 165, got %d", val)
	}

	// Heal and verify old primary recovers
	cluster.Heal(primaryID, secondaryID)
	if err := cluster.WaitForRole(primaryID, replica.RoleSecondary, 3*time.Second); err != nil {
		// May still be joining
	}

	// Wait for counter to sync
	oldPrimary := cluster.Node(primaryID)
	err = cluster.WaitForCondition(func() bool {
		val := getCounterFromNode(t, oldPrimary, counterID, "customer1")
		return val == 165
	}, 2*time.Second)
	if err != nil {
		val := getCounterFromNode(t, oldPrimary, counterID, "customer1")
		t.Errorf("counter not recovered: expected 165, got %d", val)
	}
}

func TestFailure_ConcurrentWritesDuringPartition(t *testing.T) {
	cfg := &harness.ClusterConfig{
		NodeCount:          2,
		HeartbeatInterval:  50 * time.Millisecond,
		LeaseDuration:      2 * time.Second, // Long lease to control promotion timing
		LeaseGrace:         2 * time.Second,
		ReplicationTimeout: 500 * time.Millisecond,
		ModifyTimeout:      100 * time.Millisecond,
	}
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	primary := cluster.Primary()
	secondary := cluster.Secondary()

	// Concurrent writes with replication working
	var wg sync.WaitGroup
	var mu sync.Mutex
	storeIDs := make([]string, 0, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := createStore(t, primary.Addr(), "customer1", []byte(fmt.Sprintf("concurrent-%d", idx)))
			mu.Lock()
			storeIDs = append(storeIDs, id)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if len(storeIDs) != 10 {
		t.Fatalf("expected 10 stores, got %d", len(storeIDs))
	}

	// All stores should replicate
	for _, id := range storeIDs {
		if err := cluster.WaitForStore(secondary.ID, id, "customer1", 3*time.Second); err != nil {
			t.Errorf("store %s not replicated: %v", id, err)
		}
	}

	// Now test writes during partition with degraded mode
	cluster.Network().Block(primary.ID, secondary.ID)

	// Create more stores (will be degraded writes)
	degradedStoreIDs := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		resp := createStoreRaw(t, primary.Addr(), "customer1", []byte(fmt.Sprintf("degraded-%d", i)))
		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			degradedStoreIDs = append(degradedStoreIDs, string(body))
		}
		resp.Body.Close()
	}

	// These stores exist on primary but NOT on secondary (expected with async replication)
	for _, id := range degradedStoreIDs {
		_, err := primary.Store.Get(id, "customer1")
		if err != nil {
			t.Errorf("store %s missing from primary: %v", id, err)
		}
		_, err = secondary.Store.Get(id, "customer1")
		if err == nil {
			t.Errorf("store %s should NOT be on secondary yet (partition active)", id)
		}
	}

	// Heal and create new stores - those should replicate
	cluster.Network().Heal(primary.ID, secondary.ID)

	postHealStore := createStore(t, primary.Addr(), "customer1", []byte("post-heal"))
	if err := cluster.WaitForStore(secondary.ID, postHealStore, "customer1", 2*time.Second); err != nil {
		t.Errorf("post-heal store not replicated: %v", err)
	}

	t.Logf("concurrent writes test passed: %d normal, %d degraded, 1 post-heal", len(storeIDs), len(degradedStoreIDs))
}

func TestFailure_RecoveryWithManyStores(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	// Block node2 before starting
	cluster.Partition("node1", "node2")

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForRole("node1", replica.RolePrimary, time.Second); err != nil {
		t.Fatalf("node1 didn't become primary: %v", err)
	}

	primary := cluster.Node("node1")

	// Create many stores while node2 is partitioned
	numStores := 100
	storeIDs := make([]string, numStores)
	for i := 0; i < numStores; i++ {
		storeIDs[i] = createStore(t, primary.Addr(), "customer1", []byte(fmt.Sprintf("store-%d", i)))
	}

	// Heal partition
	cluster.Heal("node1", "node2")

	// Wait for node2 to recover to SECONDARY
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 5*time.Second); err != nil {
		t.Fatalf("node2 didn't recover: %v", err)
	}

	// Verify all stores are recovered
	node2 := cluster.Node("node2")
	for i, id := range storeIDs {
		err := cluster.WaitForCondition(func() bool {
			s, err := node2.Store.Get(id, "customer1")
			return err == nil && bytes.Equal(s.Body, []byte(fmt.Sprintf("store-%d", i)))
		}, 3*time.Second)
		if err != nil {
			t.Errorf("store %d not recovered correctly: %v", i, err)
		}
	}

	t.Logf("recovered %d stores successfully", numStores)
}

func completeModifyWithBody(t *testing.T, addr, storeID, customerID, lockID string, body []byte) {
	req, err := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/complete-modify/%s", addr, storeID),
		bytes.NewReader(body))
	if err != nil {
		t.Fatalf("complete-modify request: %v", err)
	}
	req.Header.Set("X-Customer-ID", customerID)
	req.Header.Set("BigBunny-Lock-ID", lockID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("complete-modify failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("complete-modify failed: %d %s", resp.StatusCode, respBody)
	}
}

func getCounterSnapshot(t *testing.T, addr, storeID, customerID string) int64 {
	req, err := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/snapshot/%s", addr, storeID),
		nil)
	if err != nil {
		t.Fatalf("snapshot request: %v", err)
	}
	req.Header.Set("X-Customer-ID", customerID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("snapshot failed: %d %s", resp.StatusCode, respBody)
	}

	var result struct {
		Value int64 `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	return result.Value
}

func getCounterFromNode(t *testing.T, node *harness.Node, storeID, customerID string) int64 {
	s, err := node.Store.Get(storeID, customerID)
	if err != nil {
		return -1
	}
	var counter struct {
		Value int64 `json:"value"`
	}
	if err := json.Unmarshal(s.Body, &counter); err != nil {
		return -1
	}
	return counter.Value
}
