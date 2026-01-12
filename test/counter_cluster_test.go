package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/api"
	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/internal/store"
	"github.com/dip-proto/bigbunny/test/harness"
)

// TestCluster_CounterReplication verifies counter creation and increment operations replicate correctly
func TestCluster_CounterReplication(t *testing.T) {
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

	// Create counter with bounds on primary
	storeID := createCounter(t, primary.Addr(), "customer1", 50, int64Ptr(0), int64Ptr(100))

	// Wait for replication
	if err := cluster.WaitForStore(secondary.ID, storeID, "customer1", 2*time.Second); err != nil {
		t.Fatalf("counter not replicated: %v", err)
	}

	// Verify secondary has counter with correct data
	counterData, version, err := secondary.Store.GetCounter(storeID, "customer1")
	if err != nil {
		t.Fatalf("failed to get counter from secondary: %v", err)
	}
	if counterData.Value != 50 {
		t.Errorf("expected value 50, got %d", counterData.Value)
	}
	if counterData.Min == nil || *counterData.Min != 0 {
		t.Errorf("expected min=0, got %v", counterData.Min)
	}
	if counterData.Max == nil || *counterData.Max != 100 {
		t.Errorf("expected max=100, got %v", counterData.Max)
	}
	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}

	// Increment on primary
	incrementCounter(t, primary.Addr(), storeID, "customer1", 10)

	// Wait for increment to replicate
	err = cluster.WaitForCondition(func() bool {
		data, _, err := secondary.Store.GetCounter(storeID, "customer1")
		return err == nil && data.Value == 60
	}, 2*time.Second)
	if err != nil {
		t.Fatal("increment not replicated")
	}

	// Verify version incremented on secondary
	_, version, _ = secondary.Store.GetCounter(storeID, "customer1")
	if version != 2 {
		t.Errorf("expected version 2 after increment, got %d", version)
	}
}

// TestCluster_CounterBoundedLoss tests the documented bounded data loss behavior
func TestCluster_CounterBoundedLoss(t *testing.T) {
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

	// Create counter
	storeID := createCounter(t, primary.Addr(), "customer1", 100, nil, nil)

	// Wait for replication
	if err := cluster.WaitForStore(secondary.ID, storeID, "customer1", 2*time.Second); err != nil {
		t.Fatalf("counter not replicated: %v", err)
	}

	// Verify both nodes at 100
	primaryData, _, _ := primary.Store.GetCounter(storeID, "customer1")
	secondaryData, _, _ := secondary.Store.GetCounter(storeID, "customer1")
	if primaryData.Value != 100 || secondaryData.Value != 100 {
		t.Fatalf("initial values mismatch: primary=%d secondary=%d", primaryData.Value, secondaryData.Value)
	}

	// Partition network BEFORE increment (simulates crash before replication)
	cluster.Partition(primaryID, secondaryID)

	// Increment on primary (will not replicate due to partition)
	incrementCounter(t, primary.Addr(), storeID, "customer1", 5)

	// Verify primary has 105
	primaryData, _, _ = primary.Store.GetCounter(storeID, "customer1")
	if primaryData.Value != 105 {
		t.Fatalf("primary value should be 105, got %d", primaryData.Value)
	}

	// Wait for secondary to promote (primary appears dead due to partition)
	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	if err := cluster.WaitForRole(secondaryID, replica.RolePrimary, promotionTimeout); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	// Verify new primary (old secondary) still has 100 - increment was lost
	newPrimaryData, _, _ := cluster.Node(secondaryID).Store.GetCounter(storeID, "customer1")
	if newPrimaryData.Value != 100 {
		t.Errorf("expected bounded data loss: new primary should have 100, got %d", newPrimaryData.Value)
	}

	t.Logf("Bounded data loss confirmed: 5 increments lost during failover (105 -> 100)")
}

// TestCluster_CounterEpochFencing verifies counters respect epoch boundaries
func TestCluster_CounterEpochFencing(t *testing.T) {
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

	// Create counter
	storeID := createCounter(t, primary.Addr(), "customer1", 0, nil, nil)
	if err := cluster.WaitForStore(secondary.ID, storeID, "customer1", 2*time.Second); err != nil {
		t.Fatalf("counter not replicated: %v", err)
	}

	// Record initial epoch
	initialEpoch := primary.Replica.LeaderEpoch()
	t.Logf("initial primary epoch: %d", initialEpoch)

	// Force failover
	cluster.Partition(primaryID, secondaryID)
	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	if err := cluster.WaitForRole(secondaryID, replica.RolePrimary, promotionTimeout); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	newPrimaryEpoch := cluster.Node(secondaryID).Replica.LeaderEpoch()
	if newPrimaryEpoch <= initialEpoch {
		t.Fatalf("new primary epoch %d should be > initial %d", newPrimaryEpoch, initialEpoch)
	}
	t.Logf("new primary epoch after failover: %d", newPrimaryEpoch)

	// Increment on new primary (epoch should be correct)
	incrementCounter(t, cluster.Node(secondaryID).Addr(), storeID, "customer1", 10)

	// Verify counter on new primary
	data, _, _ := cluster.Node(secondaryID).Store.GetCounter(storeID, "customer1")
	if data.Value != 10 {
		t.Errorf("expected value 10 on new primary, got %d", data.Value)
	}

	// Verify LeaderEpoch was set correctly during increment
	st, _ := cluster.Node(secondaryID).Store.Get(storeID, "customer1")
	if st.LeaderEpoch != newPrimaryEpoch {
		t.Errorf("counter LeaderEpoch %d should match current epoch %d", st.LeaderEpoch, newPrimaryEpoch)
	}
}

// TestCluster_CounterSecondaryReads verifies secondary serves consistent counter reads
func TestCluster_CounterSecondaryReads(t *testing.T) {
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

	// Create counter on primary
	min := int64Ptr(0)
	max := int64Ptr(1000)
	storeID := createCounter(t, primary.Addr(), "customer1", 42, min, max)

	// Wait for replication
	if err := cluster.WaitForStore(secondary.ID, storeID, "customer1", 2*time.Second); err != nil {
		t.Fatalf("counter not replicated: %v", err)
	}

	// Read from secondary directly (not via API, but via Manager)
	counterData, version, err := secondary.Store.GetCounter(storeID, "customer1")
	if err != nil {
		t.Fatalf("failed to read counter from secondary: %v", err)
	}

	// Verify all fields deserialized correctly
	if counterData.Value != 42 {
		t.Errorf("value mismatch: expected 42, got %d", counterData.Value)
	}
	if counterData.Min == nil || *counterData.Min != 0 {
		t.Errorf("min mismatch: expected 0, got %v", counterData.Min)
	}
	if counterData.Max == nil || *counterData.Max != 1000 {
		t.Errorf("max mismatch: expected 1000, got %v", counterData.Max)
	}
	if version != 1 {
		t.Errorf("version mismatch: expected 1, got %d", version)
	}

	// Verify DataType is correct
	st, _ := secondary.Store.Get(storeID, "customer1")
	if st.DataType != store.DataTypeCounter {
		t.Errorf("DataType should be Counter, got %s", st.DataType)
	}
}

// TestCluster_CounterRecovery verifies counter state survives recovery protocol
// This tests that when a primary fails over and then rejoins, it correctly recovers
// counter state (including bounds) via snapshot protocol.
func TestCluster_CounterRecovery(t *testing.T) {
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

	// Create counters and perform increments
	counter1 := createCounter(t, primary.Addr(), "customer1", 10, nil, nil)
	counter2 := createCounter(t, primary.Addr(), "customer1", 20, int64Ptr(0), int64Ptr(100))
	counter3 := createCounter(t, primary.Addr(), "customer1", 30, nil, nil)

	// Wait for initial replication
	time.Sleep(200 * time.Millisecond)

	incrementCounter(t, primary.Addr(), counter1, "customer1", 5)
	incrementCounter(t, primary.Addr(), counter2, "customer1", 10)
	incrementCounter(t, primary.Addr(), counter3, "customer1", 15)

	// Wait for increments to replicate to secondary
	time.Sleep(200 * time.Millisecond)

	// Verify secondary has correct state before failover
	sec1, _, _ := cluster.Node(secondaryID).Store.GetCounter(counter1, "customer1")
	sec2, _, _ := cluster.Node(secondaryID).Store.GetCounter(counter2, "customer1")
	sec3, _, _ := cluster.Node(secondaryID).Store.GetCounter(counter3, "customer1")

	if sec1.Value != 15 || sec2.Value != 30 || sec3.Value != 45 {
		t.Fatalf("pre-failover values incorrect: %d, %d, %d", sec1.Value, sec2.Value, sec3.Value)
	}

	// Force failover: partition primary, wait for secondary to promote
	cluster.Partition(primaryID, secondaryID)

	promotionTimeout := cfg.LeaseDuration + cfg.LeaseGrace + time.Second
	if err := cluster.WaitForRole(secondaryID, replica.RolePrimary, promotionTimeout); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	// Do more increments on new primary (old secondary)
	incrementCounter(t, cluster.Node(secondaryID).Addr(), counter1, "customer1", 100)
	incrementCounter(t, cluster.Node(secondaryID).Addr(), counter2, "customer1", 20)
	incrementCounter(t, cluster.Node(secondaryID).Addr(), counter3, "customer1", 5)

	// Heal partition - old primary should recover and become secondary
	cluster.Heal(primaryID, secondaryID)

	// Wait for old primary to recover via snapshot and become secondary
	if err := cluster.WaitForRole(primaryID, replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("old primary didn't recover to SECONDARY: %v", err)
	}

	// Verify recovered node has correct counter values
	data1, _, err1 := cluster.Node(primaryID).Store.GetCounter(counter1, "customer1")
	data2, _, err2 := cluster.Node(primaryID).Store.GetCounter(counter2, "customer1")
	data3, _, err3 := cluster.Node(primaryID).Store.GetCounter(counter3, "customer1")

	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("failed to get counters after recovery: %v, %v, %v", err1, err2, err3)
	}

	if data1.Value != 115 { // 10+5+100
		t.Errorf("counter1: expected 115, got %d", data1.Value)
	}
	if data2.Value != 50 { // 20+10+20
		t.Errorf("counter2: expected 50, got %d (bounds: [0,100])", data2.Value)
	}
	if data3.Value != 50 { // 30+15+5
		t.Errorf("counter3: expected 50, got %d", data3.Value)
	}

	// Verify bounds preserved after recovery
	if data2.Min == nil || *data2.Min != 0 || data2.Max == nil || *data2.Max != 100 {
		t.Errorf("counter2 bounds not preserved after recovery")
	}
}

// Helper functions

func createCounter(t *testing.T, addr, customerID string, value int64, min, max *int64) string {
	reqBody := map[string]any{
		"type":  "counter",
		"value": value,
	}
	if min != nil {
		reqBody["min"] = *min
	}
	if max != nil {
		reqBody["max"] = *max
	}

	bodyBytes, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "http://"+addr+"/api/v1/create", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("create counter request: %v", err)
	}
	req.Header.Set("X-Customer-ID", customerID)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create counter request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("create counter failed: %d %s", resp.StatusCode, respBody)
	}

	storeID, _ := io.ReadAll(resp.Body)
	return string(storeID)
}

func incrementCounter(t *testing.T, addr, storeID, customerID string, delta int64) *api.CounterResponse {
	reqBody := map[string]any{"delta": delta}
	bodyBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/increment/%s", addr, storeID), bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("increment request: %v", err)
	}
	req.Header.Set("X-Customer-ID", customerID)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("increment request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("increment failed: %d %s", resp.StatusCode, respBody)
	}

	var result api.CounterResponse
	json.NewDecoder(resp.Body).Decode(&result)
	return &result
}

func int64Ptr(v int64) *int64 {
	return &v
}
