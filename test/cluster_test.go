package test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/test/harness"
)

func TestCluster_HappyPathReplication(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Stop()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for secondary to recover
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	primary := cluster.Primary()
	secondary := cluster.Secondary()
	if primary == nil || secondary == nil {
		t.Fatalf("missing primary or secondary: primary=%v secondary=%v", primary, secondary)
	}

	// Create a store on primary
	storeID := createStore(t, primary.Addr(), "customer1", []byte("initial data"))

	// Wait for replication to propagate
	if err := cluster.WaitForStore(secondary.ID, storeID, "customer1", 2*time.Second); err != nil {
		t.Fatalf("store not replicated to secondary: %v", err)
	}

	// Verify secondary has the store
	secondaryStore, err := secondary.Store.Get(storeID, "customer1")
	if err != nil {
		t.Fatalf("unexpected error getting store: %v", err)
	}
	if !bytes.Equal(secondaryStore.Body, []byte("initial data")) {
		t.Errorf("secondary body mismatch: got %s", secondaryStore.Body)
	}

	// Modify the store
	modifyStore(t, primary.Addr(), storeID, "customer1", []byte("modified data"))

	// Wait for modification to replicate
	err = cluster.WaitForCondition(func() bool {
		s, err := secondary.Store.Get(storeID, "customer1")
		return err == nil && bytes.Equal(s.Body, []byte("modified data"))
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("modification not replicated: %v", err)
	}

	// Delete the store
	deleteStore(t, primary.Addr(), storeID, "customer1")

	// Wait for delete to replicate
	err = cluster.WaitForCondition(func() bool {
		_, err := secondary.Store.Get(storeID, "customer1")
		return err != nil // store should be gone
	}, 2*time.Second)
	if err != nil {
		t.Error("store still exists on secondary after delete")
	}
}

func TestCluster_NamedStoreReplication(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Stop()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	primary := cluster.Primary()
	secondary := cluster.Secondary()

	// Create a named store
	storeID := createNamedStore(t, primary.Addr(), "customer1", "my-store", []byte("named store data"))

	// Wait for store replication
	if err := cluster.WaitForStore(secondary.ID, storeID, "customer1", 2*time.Second); err != nil {
		t.Fatalf("named store not replicated: %v", err)
	}

	// Wait for registry replication
	err = cluster.WaitForCondition(func() bool {
		entry, err := secondary.Registry.Lookup("customer1", "my-store")
		return err == nil && entry.StoreID == storeID
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("registry entry not replicated")
	}

	// Verify store data
	secondaryStore, err := secondary.Store.Get(storeID, "customer1")
	if err != nil {
		t.Fatalf("unexpected error getting store: %v", err)
	}
	if !bytes.Equal(secondaryStore.Body, []byte("named store data")) {
		t.Errorf("named store body mismatch: got %s", secondaryStore.Body)
	}

	// Lookup by name
	lookupID := lookupStoreByName(t, primary.Addr(), "customer1", "my-store")
	if lookupID != storeID {
		t.Errorf("lookup returned wrong ID: got %s, want %s", lookupID, storeID)
	}
}

func TestCluster_Failover(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Stop()

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
	storeID := createStore(t, primary.Addr(), "customer1", []byte("pre-partition data"))

	// Wait for replication before partitioning
	if err := cluster.WaitForStore(secondaryID, storeID, "customer1", 2*time.Second); err != nil {
		t.Fatalf("store not replicated before partition: %v", err)
	}

	// Partition the primary
	cluster.Partition(primaryID, secondaryID)

	// Wait for secondary to promote (includes lease expiry + grace window)
	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	if err := cluster.WaitForRole(secondaryID, replica.RolePrimary, promotionTimeout); err != nil {
		t.Fatalf("secondary didn't promote: %v (role=%s)", err, cluster.Node(secondaryID).Replica.Role())
	}

	// New primary should have higher epoch
	newPrimary := cluster.Node(secondaryID)
	oldPrimary := cluster.Node(primaryID)
	if newPrimary.Replica.LeaderEpoch() <= oldPrimary.Replica.LeaderEpoch() {
		t.Errorf("new primary epoch %d should be > old primary epoch %d",
			newPrimary.Replica.LeaderEpoch(), oldPrimary.Replica.LeaderEpoch())
	}

	// New primary should have the data
	newPrimaryStore, err := newPrimary.Store.Get(storeID, "customer1")
	if err != nil {
		t.Fatalf("store missing from new primary: %v", err)
	}
	if !bytes.Equal(newPrimaryStore.Body, []byte("pre-partition data")) {
		t.Errorf("data mismatch on new primary: %s", newPrimaryStore.Body)
	}

	// Writes to new primary should work
	storeID2 := createStore(t, newPrimary.Addr(), "customer1", []byte("post-failover data"))
	if storeID2 == "" {
		t.Fatal("failed to create store on new primary")
	}

	// Heal partition
	cluster.Heal(primaryID, secondaryID)

	// Old primary should demote to JOINING then SECONDARY
	if err := cluster.WaitForRole(primaryID, replica.RoleJoining, time.Second); err != nil {
		// May already be SECONDARY if recovery is fast
		if cluster.Node(primaryID).Replica.Role() != replica.RoleSecondary {
			t.Logf("old primary role: %s", cluster.Node(primaryID).Replica.Role())
		}
	}
}

func TestCluster_JoiningRecovery(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Stop()

	// Block node2 before starting (simulates failed initial recovery)
	cluster.Partition("node1", "node2")

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// node1 is primary
	if err := cluster.WaitForRole("node1", replica.RolePrimary, time.Second); err != nil {
		t.Fatalf("node1 didn't become primary: %v", err)
	}

	// node2 should be stuck in JOINING (partitioned from primary)
	node2 := cluster.Node("node2")
	if err := cluster.WaitForCondition(func() bool {
		return node2.Replica.Role() == replica.RoleJoining
	}, time.Second); err != nil {
		t.Errorf("node2 should be JOINING, got %s", node2.Replica.Role())
	}

	// Create data on primary while node2 is partitioned
	primary := cluster.Node("node1")
	storeID := createStore(t, primary.Addr(), "customer1", []byte("recovery test data"))
	if storeID == "" {
		t.Fatal("failed to create store on primary")
	}

	// Heal partition
	cluster.Heal("node1", "node2")

	// node2 should recover and reach SECONDARY
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("node2 didn't recover to SECONDARY: %v", err)
	}

	// Verify node2 has the data from recovery
	recoveredStore, err := node2.Store.Get(storeID, "customer1")
	if err != nil {
		t.Fatalf("store not recovered to node2: %v", err)
	}
	if !bytes.Equal(recoveredStore.Body, []byte("recovery test data")) {
		t.Errorf("recovered data mismatch: %s", recoveredStore.Body)
	}
}

func TestCluster_DegradedWrite(t *testing.T) {
	// Use longer lease timeouts so secondary doesn't promote during the test
	cfg := &harness.ClusterConfig{
		NodeCount:          2,
		HeartbeatInterval:  50 * time.Millisecond,
		LeaseDuration:      2 * time.Second, // Long lease to prevent promotion
		LeaseGrace:         2 * time.Second,
		ReplicationTimeout: 500 * time.Millisecond,
		ModifyTimeout:      100 * time.Millisecond,
	}
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Stop()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	primary := cluster.Primary()
	secondary := cluster.Secondary()

	// Block replication (one-way: primary can't reach secondary)
	cluster.Network().Block(primary.ID, secondary.ID)

	// Trigger multiple replication failures (each create attempts replication immediately)
	for i := 0; i < 5; i++ {
		_ = createStore(t, primary.Addr(), "customer1", []byte(fmt.Sprintf("fail%d", i)))
	}

	// Create a store - should succeed with degraded warning
	resp := createStoreRaw(t, primary.Addr(), "customer1", []byte("degraded write"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	warning := resp.Header.Get("BigBunny-Warning")
	if warning != "DegradedWrite" {
		t.Errorf("expected BigBunny-Warning: DegradedWrite, got %q", warning)
	}
}

func TestCluster_LockStateUnknown(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Stop()

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

	// Create and lock a store
	storeID := createStore(t, primary.Addr(), "customer1", []byte("lock test"))

	// Wait for store to replicate to secondary
	if err := cluster.WaitForStore(secondaryID, storeID, "customer1", time.Second); err != nil {
		t.Fatalf("store not replicated: %v", err)
	}

	// Begin modify to acquire lock
	lockID := beginModify(t, primary.Addr(), storeID, "customer1")

	// Partition the primary
	cluster.Partition(primaryID, secondaryID)

	// Wait for secondary to promote (includes lease expiry + grace)
	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	if err := cluster.WaitForRole(secondaryID, replica.RolePrimary, promotionTimeout); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	newPrimary := cluster.Node(secondaryID)

	// Immediately after promotion, begin-modify should return LockStateUnknown
	resp := beginModifyRaw(t, newPrimary.Addr(), storeID, "customer1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 409 during lock unknown window, got %d: %s", resp.StatusCode, body)
	}

	errorCode := resp.Header.Get("BigBunny-Error-Code")
	lockState := resp.Header.Get("BigBunny-Lock-State")
	if errorCode != "LockStateUnknown" {
		t.Errorf("expected error code LockStateUnknown, got %q", errorCode)
	}
	if lockState != "unknown" {
		t.Errorf("expected lock state unknown, got %q", lockState)
	}

	// Wait for lock unknown window to pass by polling begin-modify
	lockWindowTimeout := cfg.ModifyTimeout + 500*time.Millisecond
	err = cluster.WaitForCondition(func() bool {
		resp2 := beginModifyRaw(t, newPrimary.Addr(), storeID, "customer1")
		defer resp2.Body.Close()
		errorCode2 := resp2.Header.Get("BigBunny-Error-Code")
		return errorCode2 != "LockStateUnknown"
	}, lockWindowTimeout)
	if err != nil {
		t.Error("still getting LockStateUnknown after window should have passed")
	}

	// Clean up: complete the original modify on old primary (after healing)
	cluster.Heal(primaryID, secondaryID)
	_ = lockID // We don't use this for verification in this test
}

func TestCluster_EpochMonotonicity(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Stop()

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

	// Record initial epochs
	initialPrimaryEpoch := primary.Replica.LeaderEpoch()
	initialSecondaryEpoch := secondary.Replica.LeaderEpoch()
	t.Logf("initial epochs: primary=%d secondary=%d", initialPrimaryEpoch, initialSecondaryEpoch)

	// Partition and wait for failover
	cluster.Partition(primaryID, secondaryID)
	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	if err := cluster.WaitForRole(secondaryID, replica.RolePrimary, promotionTimeout); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	// New primary should have incremented epoch
	newPrimaryEpoch := cluster.Node(secondaryID).Replica.LeaderEpoch()
	if newPrimaryEpoch <= initialPrimaryEpoch {
		t.Errorf("new primary epoch %d should be > initial %d", newPrimaryEpoch, initialPrimaryEpoch)
	}
	t.Logf("after failover: new primary epoch=%d", newPrimaryEpoch)

	// Heal and wait for old primary to recover (becomes secondary with updated epoch)
	cluster.Heal(primaryID, secondaryID)
	if err := cluster.WaitForCondition(func() bool {
		return cluster.Node(primaryID).Replica.LeaderEpoch() >= newPrimaryEpoch
	}, 2*time.Second); err != nil {
		t.Fatalf("old primary didn't recover epoch: %v", err)
	}

	// Old primary should have updated epoch (not regressed)
	recoveredEpoch := cluster.Node(primaryID).Replica.LeaderEpoch()
	if recoveredEpoch < newPrimaryEpoch {
		t.Errorf("recovered node epoch %d should be >= new primary epoch %d", recoveredEpoch, newPrimaryEpoch)
	}
	t.Logf("after recovery: old primary epoch=%d", recoveredEpoch)
}

// Helper functions

func createStore(t *testing.T, addr, customerID string, body []byte) string {
	resp := createStoreRaw(t, addr, customerID, body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("create failed: %d %s", resp.StatusCode, respBody)
	}

	// Response is just the store ID as text, not JSON
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return string(respBody)
}

func createStoreRaw(t *testing.T, addr, customerID string, body []byte) *http.Response {
	req, err := http.NewRequest("POST", "http://"+addr+"/api/v1/create", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("X-Customer-ID", customerID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create request failed: %v", err)
	}
	return resp
}

func createNamedStore(t *testing.T, addr, customerID, name string, body []byte) string {
	req, err := http.NewRequest("POST", "http://"+addr+"/api/v1/create-by-name/"+name, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("X-Customer-ID", customerID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("create-by-name failed: %d %s", resp.StatusCode, respBody)
	}

	// Response is just the store ID as text
	respBody, _ := io.ReadAll(resp.Body)
	return string(respBody)
}

func lookupStoreByName(t *testing.T, addr, customerID, name string) string {
	req, err := http.NewRequest("POST", "http://"+addr+"/api/v1/lookup-id-by-name/"+name, nil)
	if err != nil {
		t.Fatalf("lookup request: %v", err)
	}
	req.Header.Set("X-Customer-ID", customerID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("lookup request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("lookup failed: %d %s", resp.StatusCode, respBody)
	}

	// Response is just the store ID as text
	respBody, _ := io.ReadAll(resp.Body)
	return string(respBody)
}

func modifyStore(t *testing.T, addr, storeID, customerID string, body []byte) {
	// Begin modify
	lockID := beginModify(t, addr, storeID, customerID)

	// Complete modify
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

func beginModify(t *testing.T, addr, storeID, customerID string) string {
	resp := beginModifyRaw(t, addr, storeID, customerID)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("begin-modify failed: %d %s", resp.StatusCode, respBody)
	}

	lockID := resp.Header.Get("BigBunny-Lock-ID")
	if lockID == "" {
		t.Fatal("begin-modify didn't return lock ID")
	}
	return lockID
}

func beginModifyRaw(t *testing.T, addr, storeID, customerID string) *http.Response {
	req, err := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/begin-modify/%s", addr, storeID),
		nil)
	if err != nil {
		t.Fatalf("begin-modify request: %v", err)
	}
	req.Header.Set("X-Customer-ID", customerID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("begin-modify request failed: %v", err)
	}
	return resp
}

func deleteStore(t *testing.T, addr, storeID, customerID string) {
	req, err := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/delete/%s", addr, storeID),
		nil)
	if err != nil {
		t.Fatalf("delete request: %v", err)
	}
	req.Header.Set("X-Customer-ID", customerID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("delete failed: %d %s", resp.StatusCode, respBody)
	}
}

func TestCluster_InternalAuth(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cfg.InternalToken = "test-secret-token-42"
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Stop()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for secondary to recover (this requires internal auth to work)
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY (recovery failed with internal auth): %v", err)
	}

	primary := cluster.Primary()
	secondary := cluster.Secondary()

	// Create a store on primary - replication requires internal auth
	storeID := createStore(t, primary.Addr(), "customer1", []byte("test data with auth"))

	// Wait for replication to propagate (heartbeat + replication both use internal auth)
	if err := cluster.WaitForStore(secondary.ID, storeID, "customer1", 2*time.Second); err != nil {
		t.Fatalf("store not replicated with internal auth: %v", err)
	}

	// Verify secondary has the store
	secondaryStore, err := secondary.Store.Get(storeID, "customer1")
	if err != nil {
		t.Fatalf("unexpected error getting store: %v", err)
	}
	if string(secondaryStore.Body) != "test data with auth" {
		t.Errorf("replicated data mismatch: got %q", secondaryStore.Body)
	}

	t.Logf("internal auth test passed: recovery, heartbeat, and replication all working")
}
