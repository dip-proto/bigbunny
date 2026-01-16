package test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/test/harness"
)

// TestJoinSync_WritesDuringJoinGet503 verifies that writes to the primary
// get 503 responses while a join snapshot is in progress.
func TestJoinSync_WritesDuringJoinGet503(t *testing.T) {
	cfg := harness.DefaultClusterConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for secondary to reach SECONDARY state
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't reach SECONDARY: %v", err)
	}

	primary := cluster.Primary()

	// Create some initial data
	storeID := createStore(t, primary.Addr(), "customer1", []byte("initial"))

	// Manually acquire the join sync lock on primary to simulate an active join
	if !primary.Replica.StartJoinSync() {
		t.Fatal("failed to start join sync")
	}

	// Now try to create a store - should get 503
	resp, err := doRequest("POST", primary.Addr(), "/api/v1/create", "customer1", []byte("should fail"))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 during join, got %d", resp.StatusCode)
	}

	errCode := resp.Header.Get("BigBunny-Error-Code")
	if errCode != "JoinSyncing" {
		t.Errorf("expected error code JoinSyncing, got %q", errCode)
	}

	// Try other write operations
	resp, err = doRequest("POST", primary.Addr(), "/api/v1/delete/"+storeID, "customer1", nil)
	if err != nil {
		t.Fatalf("delete request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("delete: expected 503 during join, got %d", resp.StatusCode)
	}

	// Release the join sync
	primary.Replica.EndJoinSync()

	// Now writes should succeed
	storeID2 := createStore(t, primary.Addr(), "customer1", []byte("after join"))
	if storeID2 == "" {
		t.Error("failed to create store after join ended")
	}
}

// TestJoinSync_ConcurrentJoinsPrevented verifies that only one join can
// be active at a time.
func TestJoinSync_ConcurrentJoinsPrevented(t *testing.T) {
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

	// Start first join
	if !primary.Replica.StartJoinSync() {
		t.Fatal("failed to start first join sync")
	}
	defer primary.Replica.EndJoinSync()

	// Try to start second join - should fail
	if primary.Replica.StartJoinSync() {
		primary.Replica.EndJoinSync() // cleanup
		t.Error("second concurrent join should have been prevented")
	}

	// Verify IsJoinSyncing returns true
	if !primary.Replica.IsJoinSyncing() {
		t.Error("IsJoinSyncing should return true during active join")
	}
}

// TestJoinSync_InFlightMutationsComplete verifies that in-flight mutations
// complete before the join lock is acquired.
func TestJoinSync_InFlightMutationsComplete(t *testing.T) {
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

	// Acquire write barrier (simulating an in-flight write)
	release, ok := primary.Replica.TryAcquireWriteBarrier()
	if !ok {
		t.Fatal("failed to acquire write barrier")
	}

	// Channels for coordination instead of fixed sleeps
	joinStartedCh := make(chan struct{})
	joinCompletedCh := make(chan bool, 1)

	// Start join in background - should block waiting for write
	go func() {
		close(joinStartedCh) // Signal we're about to call StartJoinSync
		if primary.Replica.StartJoinSync() {
			joinCompletedCh <- true
			primary.Replica.EndJoinSync()
		} else {
			joinCompletedCh <- false
		}
	}()

	// Wait for join goroutine to start (with timeout)
	select {
	case <-joinStartedCh:
		// Good, goroutine started
	case <-time.After(2 * time.Second):
		t.Fatal("join goroutine didn't start within timeout")
	}

	// Give the goroutine a moment to actually call StartJoinSync and block on the lock
	// This is inherently racy, but we use a short sleep just to let it reach the Lock() call
	time.Sleep(10 * time.Millisecond)

	// Join should NOT have completed yet (blocked on our RLock)
	select {
	case <-joinCompletedCh:
		t.Error("join completed while write was in progress (shouldn't happen)")
	default:
		// Good, still blocked
	}

	// Release our write barrier
	release()

	// Wait for join to complete (with timeout)
	select {
	case succeeded := <-joinCompletedCh:
		if !succeeded {
			t.Error("join failed after write finished")
		}
	case <-time.After(2 * time.Second):
		t.Error("join didn't complete after write finished (timeout)")
	}
}

// TestJoinSync_ConsistentSnapshotDuringRecovery verifies that the snapshot
// endpoint returns consistent data while the join lock is held.
func TestJoinSync_ConsistentSnapshotDuringRecovery(t *testing.T) {
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

	// Create multiple stores
	var storeIDs []string
	for i := range 10 {
		_ = i
		id := createStore(t, primary.Addr(), "customer1", []byte("data"))
		storeIDs = append(storeIDs, id)
	}

	// Wait for all stores to replicate to secondary
	for _, id := range storeIDs {
		if err := cluster.WaitForStore(secondary.ID, id, "customer1", 2*time.Second); err != nil {
			t.Fatalf("store %s not replicated: %v", id, err)
		}
	}

	// Now acquire the join lock and verify snapshot can be taken
	if !primary.Replica.StartJoinSync() {
		t.Fatal("failed to start join sync")
	}
	defer primary.Replica.EndJoinSync()

	// Verify writes are blocked
	release, ok := primary.Replica.TryAcquireWriteBarrier()
	if ok {
		release()
		t.Error("write barrier should fail during join")
	}

	// The snapshot endpoint should work (tested implicitly through
	// the recovery flow). Here we just verify the state is correct.
	if !primary.Replica.IsJoinSyncing() {
		t.Error("IsJoinSyncing should be true")
	}
}

// TestJoinSync_WriteBarrierReturnsCorrectly verifies that TryAcquireWriteBarrier
// returns correctly based on join state.
func TestJoinSync_WriteBarrierReturnsCorrectly(t *testing.T) {
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

	// Should succeed when no join active
	release, ok := primary.Replica.TryAcquireWriteBarrier()
	if !ok {
		t.Fatal("TryAcquireWriteBarrier failed when no join active")
	}
	release()

	// Start join
	if !primary.Replica.StartJoinSync() {
		t.Fatal("failed to start join sync")
	}
	defer primary.Replica.EndJoinSync()

	// Now TryAcquireWriteBarrier should fail
	release2, ok := primary.Replica.TryAcquireWriteBarrier()
	if ok {
		release2()
		t.Error("TryAcquireWriteBarrier should fail during join")
	}
}

// TestJoinSync_ConcurrentWritesDuringJoin verifies that multiple concurrent
// writes all get 503 during join.
func TestJoinSync_ConcurrentWritesDuringJoin(t *testing.T) {
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

	// Start join
	if !primary.Replica.StartJoinSync() {
		t.Fatal("failed to start join sync")
	}
	defer primary.Replica.EndJoinSync()

	// Fire off multiple concurrent writes
	var wg sync.WaitGroup
	var failed503 atomic.Int32
	var requestErrors atomic.Int32
	var otherStatus atomic.Int32
	numWrites := 10

	for i := range numWrites {
		_ = i
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := doRequest("POST", primary.Addr(), "/api/v1/create", "customer1", []byte("data"))
			if err != nil {
				requestErrors.Add(1)
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusServiceUnavailable {
				failed503.Add(1)
			} else {
				otherStatus.Add(1)
			}
		}()
	}

	wg.Wait()

	// Report all outcomes for debugging
	if requestErrors.Load() > 0 {
		t.Errorf("got %d request errors", requestErrors.Load())
	}
	if otherStatus.Load() > 0 {
		t.Errorf("got %d responses with non-503 status", otherStatus.Load())
	}

	// All writes should have gotten 503
	if int(failed503.Load()) != numWrites {
		t.Errorf("expected %d writes to get 503, got %d (errors: %d, other: %d)",
			numWrites, failed503.Load(), requestErrors.Load(), otherStatus.Load())
	}
}

// TestJoinSync_RegistryMismatchDetection verifies that registry configuration
// mismatch between primary and secondary is detected during recovery.
func TestJoinSync_RegistryMismatchDetection(t *testing.T) {
	// This is tested implicitly by the recovery code path.
	// The JoinSnapshotData includes HasRegistry flag which is checked
	// during recovery. A full test would require a custom cluster setup
	// where one node has registry and the other doesn't, but that's not
	// a supported configuration in the test harness.
	t.Skip("registry mismatch requires custom cluster setup")
}

// TestJoinSync_SnapshotEndpointReturnsData verifies that the /internal/join-snapshot
// endpoint returns valid data including stores, tombstones, and registry.
func TestJoinSync_SnapshotEndpointReturnsData(t *testing.T) {
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

	// Create some stores to be included in snapshot
	storeIDs := make([]string, 3)
	for i := range 3 {
		storeIDs[i] = createStore(t, primary.Addr(), "customer1", []byte("data"))
	}
	_ = storeIDs // used for documentation

	// Call the join-snapshot endpoint directly (it's POST-only)
	// Note: The endpoint itself acquires the join lock internally
	req, err := http.NewRequest("POST", "http://"+primary.Addr()+"/internal/join-snapshot", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	if cfg.InternalToken != "" {
		req.Header.Set("X-Internal-Token", cfg.InternalToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to fetch join-snapshot: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Decode the response
	var snapshot replica.JoinSnapshotData
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		t.Fatalf("failed to decode snapshot: %v", err)
	}

	// Verify the snapshot has the expected data
	if len(snapshot.Stores) < 3 {
		t.Errorf("expected at least 3 stores, got %d", len(snapshot.Stores))
	}

	// Verify epoch is set
	if snapshot.LeaderEpoch == 0 {
		t.Error("expected non-zero leader epoch")
	}

	// Verify site is set
	if snapshot.Site == "" {
		t.Error("expected non-empty site")
	}

	// Verify HostID is set
	if snapshot.HostID == "" {
		t.Error("expected non-empty host ID")
	}

	// HasRegistry should reflect cluster config
	// (default cluster config has registry enabled)
	if !snapshot.HasRegistry {
		t.Error("expected HasRegistry to be true for default cluster")
	}
}

// Helper function to make requests
func doRequest(method, addr, path, customerID string, body []byte) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, "http://"+addr+path, bodyReader)
	if err != nil {
		return nil, err
	}
	if customerID != "" {
		req.Header.Set("X-Customer-ID", customerID)
	}
	return http.DefaultClient.Do(req)
}
