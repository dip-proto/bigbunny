package test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/api"
	"github.com/dip-proto/bigbunny/internal/auth"
	"github.com/dip-proto/bigbunny/internal/registry"
	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
)

// TestConcurrentLockAcquisition tests multiple goroutines trying to acquire the same lock.
// Only one should succeed, others should get ErrStoreLocked.
func TestConcurrentLockAcquisition(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:concurrent-test",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	if err := mgr.Create(s); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	const numClients = 10
	var wg sync.WaitGroup
	successCount := int32(0)
	lockedCount := int32(0)

	// Launch 10 clients trying to acquire the lock simultaneously
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			lockID := fmt.Sprintf("lock-%d", clientID)
			_, err := mgr.AcquireLock(s.ID, s.CustomerID, lockID, 500*time.Millisecond)

			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else if err == store.ErrStoreLocked {
				atomic.AddInt32(&lockedCount, 1)
			} else {
				t.Errorf("unexpected error from client %d: %v", clientID, err)
			}
		}(i)
	}

	wg.Wait()

	// Exactly one client should have succeeded
	if successCount != 1 {
		t.Errorf("expected exactly 1 successful lock acquisition, got %d", successCount)
	}

	// The rest should have been locked out
	if lockedCount != numClients-1 {
		t.Errorf("expected %d clients to get ErrStoreLocked, got %d", numClients-1, lockedCount)
	}
}

// TestConcurrentLockWithRelease tests that after a lock is released,
// another client can acquire it.
func TestConcurrentLockWithRelease(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:release-test",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	const numRounds = 5
	successChan := make(chan int, numRounds*10)

	var wg sync.WaitGroup

	// Run multiple rounds of lock/release
	for round := 0; round < numRounds; round++ {
		// Launch 10 clients per round
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(roundID, clientID int) {
				defer wg.Done()

				lockID := fmt.Sprintf("lock-r%d-c%d", roundID, clientID)
				st, err := mgr.AcquireLock(s.ID, s.CustomerID, lockID, 500*time.Millisecond)

				if err == nil {
					successChan <- roundID*100 + clientID

					// Hold the lock briefly
					time.Sleep(10 * time.Millisecond)

					// Complete the lock to release it
					mgr.CompleteLock(s.ID, s.CustomerID, lockID, []byte(fmt.Sprintf("round-%d", roundID)), time.Time{})
				} else if err != store.ErrStoreLocked {
					t.Errorf("unexpected error: %v", err)
				}

				_ = st
			}(round, i)
		}

		// Wait for this round to complete before starting next
		wg.Wait()
	}

	close(successChan)

	// Verify we got exactly numRounds successful acquisitions
	successCount := 0
	for range successChan {
		successCount++
	}

	if successCount != numRounds {
		t.Errorf("expected %d successful lock acquisitions across rounds, got %d", numRounds, successCount)
	}

	// Verify final state
	got, err := mgr.Get(s.ID, s.CustomerID)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	// Should have the last round's data
	expectedBody := fmt.Sprintf("round-%d", numRounds-1)
	if string(got.Body) != expectedBody {
		t.Errorf("expected body %q, got %q", expectedBody, got.Body)
	}
}

// TestConcurrentLockTimeout tests that locks expire after timeout.
func TestConcurrentLockTimeout(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:timeout-test",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	// Client 1 acquires lock with short timeout
	_, err := mgr.AcquireLock(s.ID, s.CustomerID, "lock1", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	// Client 2 tries immediately - should fail
	_, err = mgr.AcquireLock(s.ID, s.CustomerID, "lock2", 500*time.Millisecond)
	if err != store.ErrStoreLocked {
		t.Errorf("expected ErrStoreLocked immediately after lock, got %v", err)
	}

	// Wait for first lock to expire
	time.Sleep(100 * time.Millisecond)

	// Client 2 tries again - should succeed now
	_, err = mgr.AcquireLock(s.ID, s.CustomerID, "lock2", 500*time.Millisecond)
	if err != nil {
		t.Errorf("expected lock acquisition after timeout, got %v", err)
	}
}

// TestConcurrentAPILockContention tests lock contention at the API level
// with multiple HTTP clients trying to begin-modify on the same store.
func TestConcurrentAPILockContention(t *testing.T) {
	// Set up test server
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

	sockPath := "/tmp/bbd-test-concurrent.sock"
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to create socket: %v", err)
	}
	defer func() {
		listener.Close()
	}()

	server := &http.Server{Handler: mux}
	go server.Serve(listener)
	defer server.Close()

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sockPath)
			},
		},
	}

	// Create a store
	req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("initial"))
	req.Header.Set("X-Customer-ID", "customer123")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	storeID, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	const numClients = 20
	var wg sync.WaitGroup
	successCount := int32(0)
	conflictCount := int32(0)
	lockIDs := make([]string, numClients)

	// Launch concurrent begin-modify requests
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			req, _ := http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+string(storeID), nil)
			req.Header.Set("X-Customer-ID", "customer123")

			resp, err := client.Do(req)
			if err != nil {
				t.Errorf("client %d request failed: %v", clientID, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				atomic.AddInt32(&successCount, 1)
				lockIDs[clientID] = resp.Header.Get("BigBunny-Lock-ID")
			} else if resp.StatusCode == http.StatusConflict {
				atomic.AddInt32(&conflictCount, 1)
			} else {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("client %d unexpected status %d: %s", clientID, resp.StatusCode, body)
			}
		}(i)
	}

	wg.Wait()

	// Exactly one client should have succeeded
	if successCount != 1 {
		t.Errorf("expected exactly 1 successful lock, got %d", successCount)
	}

	// The rest should have gotten conflicts
	if conflictCount != numClients-1 {
		t.Errorf("expected %d conflicts, got %d", numClients-1, conflictCount)
	}

	// Find the successful lock ID
	var successfulLockID string
	for _, lid := range lockIDs {
		if lid != "" {
			successfulLockID = lid
			break
		}
	}

	if successfulLockID == "" {
		t.Fatal("no successful lock ID found")
	}

	// Complete the lock
	req, _ = http.NewRequest("POST", "http://localhost/api/v1/complete-modify/"+string(storeID), strings.NewReader("modified"))
	req.Header.Set("X-Customer-ID", "customer123")
	req.Header.Set("BigBunny-Lock-ID", successfulLockID)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("complete-modify failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("complete-modify status %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Verify the store was updated
	req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+string(storeID), nil)
	req.Header.Set("X-Customer-ID", "customer123")
	resp, _ = client.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if string(body) != "modified" {
		t.Errorf("expected body 'modified', got %q", body)
	}
}

// TestConcurrentLockRaceWithExpiry tests race conditions around lock expiry.
func TestConcurrentLockRaceWithExpiry(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:expiry-race-test",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	// Acquire lock with short timeout
	_, err := mgr.AcquireLock(s.ID, s.CustomerID, "lock1", 30*time.Millisecond)
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	var wg sync.WaitGroup
	successCount := int32(0)

	// Launch multiple clients just before lock expires
	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// Try to acquire immediately and again in 20ms
			for j := 0; j < 2; j++ {
				lockID := fmt.Sprintf("lock-%d-%d", clientID, j)
				_, err := mgr.AcquireLock(s.ID, s.CustomerID, lockID, 500*time.Millisecond)

				if err == nil {
					atomic.AddInt32(&successCount, 1)
					// Release immediately
					mgr.ClearLock(s.ID, lockID)
				}

				time.Sleep(20 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// At least one client should have succeeded after expiry
	if successCount < 1 {
		t.Errorf("expected at least 1 successful acquisition after expiry, got %d", successCount)
	}

	// But not all should succeed simultaneously (they should serialize)
	if successCount > 10 {
		t.Errorf("too many simultaneous successes: %d (locks should serialize)", successCount)
	}
}

// TestConcurrentLockWithCancel tests concurrent lock acquisition with some clients canceling.
func TestConcurrentLockWithCancel(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:cancel-test",
		CustomerID: "cust1",
		Body:       []byte("initial"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	const numIterations = 10
	successCount := 0

	for i := 0; i < numIterations; i++ {
		var wg sync.WaitGroup
		lockReleased := make(chan struct{})

		// Client 1 acquires lock
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := mgr.AcquireLock(s.ID, s.CustomerID, "lock-holder", 500*time.Millisecond)
			if err == nil {
				time.Sleep(10 * time.Millisecond)
				mgr.ReleaseLock(s.ID, s.CustomerID, "lock-holder")
				close(lockReleased)
			}
		}()

		// Multiple clients try to acquire after first releases
		for j := 0; j < 5; j++ {
			wg.Add(1)
			go func(clientID int) {
				defer wg.Done()
				// Wait for lock to be released
				<-lockReleased
				// Try to acquire
				_, err := mgr.AcquireLock(s.ID, s.CustomerID, fmt.Sprintf("lock-waiter-%d", clientID), 500*time.Millisecond)
				if err == nil {
					mgr.CompleteLock(s.ID, s.CustomerID, fmt.Sprintf("lock-waiter-%d", clientID), []byte("updated"), time.Time{})
				}
			}(j)
		}

		wg.Wait()

		// Check if anyone succeeded in updating
		got, _ := mgr.Get(s.ID, s.CustomerID)
		if string(got.Body) == "updated" {
			successCount++
			// Reset for next iteration
			mgr.CreateOrUpdate(&store.Store{
				ID:         s.ID,
				CustomerID: s.CustomerID,
				Body:       []byte("initial"),
				ExpiresAt:  time.Now().Add(time.Hour),
				Version:    got.Version + 1,
			})
		}
	}

	// Most iterations should have resulted in a successful update after cancel
	if successCount < numIterations/2 {
		t.Errorf("expected at least %d successful updates after cancel, got %d", numIterations/2, successCount)
	}
}

// TestConcurrentMultipleStoresLocking tests that locks on different stores don't interfere.
func TestConcurrentMultipleStoresLocking(t *testing.T) {
	mgr := store.NewManager()

	const numStores = 5
	storeIDs := make([]string, numStores)

	// Create multiple stores
	for i := 0; i < numStores; i++ {
		storeID := fmt.Sprintf("v1:local:shard1:multi-store-%d", i)
		s := &store.Store{
			ID:         storeID,
			CustomerID: "cust1",
			Body:       []byte(fmt.Sprintf("store-%d", i)),
			ExpiresAt:  time.Now().Add(time.Hour),
		}
		if err := mgr.Create(s); err != nil {
			t.Fatalf("create store %d failed: %v", i, err)
		}
		storeIDs[i] = storeID
	}

	var wg sync.WaitGroup
	successCounts := make([]int32, numStores)

	// For each store, launch multiple clients trying to lock it
	for storeIdx := 0; storeIdx < numStores; storeIdx++ {
		for clientIdx := 0; clientIdx < 5; clientIdx++ {
			wg.Add(1)
			go func(si, ci int) {
				defer wg.Done()

				lockID := fmt.Sprintf("lock-s%d-c%d", si, ci)
				_, err := mgr.AcquireLock(storeIDs[si], "cust1", lockID, 500*time.Millisecond)

				if err == nil {
					atomic.AddInt32(&successCounts[si], 1)
					// Hold briefly then release
					time.Sleep(10 * time.Millisecond)
					mgr.ClearLock(storeIDs[si], lockID)
				}
			}(storeIdx, clientIdx)
		}
	}

	wg.Wait()

	// Each store should have exactly one successful lock
	for i := 0; i < numStores; i++ {
		if successCounts[i] != 1 {
			t.Errorf("store %d: expected 1 successful lock, got %d", i, successCounts[i])
		}
	}
}

// TestConcurrentWrongLockID tests that completing/canceling with wrong lock ID fails under concurrency.
func TestConcurrentWrongLockID(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:wrong-lock-test",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	// Client 1 acquires the lock
	_, err := mgr.AcquireLock(s.ID, s.CustomerID, "correct-lock", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	var wg sync.WaitGroup
	wrongIDErrors := int32(0)

	// Multiple clients try to complete/cancel with wrong lock IDs
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			wrongLockID := fmt.Sprintf("wrong-lock-%d", clientID)

			// Try to complete with wrong ID
			_, err := mgr.CompleteLock(s.ID, s.CustomerID, wrongLockID, []byte("hacked"), time.Time{})
			if err == store.ErrLockMismatch {
				atomic.AddInt32(&wrongIDErrors, 1)
			}
		}(i)
	}

	wg.Wait()

	// All wrong lock ID attempts should fail
	if wrongIDErrors != 10 {
		t.Errorf("expected 10 wrong lock ID errors, got %d", wrongIDErrors)
	}

	// Verify data unchanged
	got, _ := mgr.Get(s.ID, s.CustomerID)
	if string(got.Body) != "data" {
		t.Errorf("data was modified despite wrong lock IDs: got %q", got.Body)
	}

	// Complete with correct lock ID should work
	_, err = mgr.CompleteLock(s.ID, s.CustomerID, "correct-lock", []byte("correct"), time.Time{})
	if err != nil {
		t.Errorf("complete with correct lock failed: %v", err)
	}
}
