package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/api"
	"github.com/dip-proto/bigbunny/internal/auth"
	"github.com/dip-proto/bigbunny/internal/registry"
	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
)

func setupTestServer(t *testing.T) (*http.Client, *store.Manager, *replica.Manager, func()) {
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
		DefaultTTL:    14 * 24 * time.Hour, // 14 days
		MaxBodySize:   2 * 1024,
		ModifyTimeout: 500 * time.Millisecond,
		Cipher:        cipher,
	}
	apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher, nil)

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	sockPath := "/tmp/bbd-ttl-test-" + strconv.FormatInt(time.Now().UnixNano(), 10) + ".sock"
	os.Remove(sockPath)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to create socket: %v", err)
	}

	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sockPath)
			},
		},
	}

	cleanup := func() {
		_ = server.Close()
		os.Remove(sockPath)
		replicaMgr.Stop()
	}

	return client, storeMgr, replicaMgr, cleanup
}

func getStoreByID(storeMgr *store.Manager, storeID string) *store.Store {
	allStores := storeMgr.Snapshot()
	for _, st := range allStores {
		if st.ID == storeID {
			return st
		}
	}
	return nil
}

func TestTTLWithHeader(t *testing.T) {
	client, storeMgr, _, cleanup := setupTestServer(t)
	defer cleanup()

	t.Run("CreateWithTTL", func(t *testing.T) {
		before := time.Now()
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader([]byte("test data")))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Not-Valid-After", "3600") // 1 hour

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
		}

		storeIDBytes, _ := io.ReadAll(resp.Body)
		storeID := string(storeIDBytes)

		after := time.Now()

		s := getStoreByID(storeMgr, storeID)
		if s == nil {
			t.Fatal("store not found")
		}

		// Expiry should be roughly now + 3600s
		expectedMin := before.Add(3600 * time.Second)
		expectedMax := after.Add(3600 * time.Second)

		if s.ExpiresAt.Before(expectedMin) || s.ExpiresAt.After(expectedMax) {
			t.Errorf("expected expiry between %v and %v, got %v", expectedMin, expectedMax, s.ExpiresAt)
		}
	})

	t.Run("CreateWithoutTTL_UsesDefault", func(t *testing.T) {
		before := time.Now()
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader([]byte("test data")))
		req.Header.Set("X-Customer-ID", "cust1")
		// No BigBunny-Not-Valid-After header

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		storeIDBytes, _ := io.ReadAll(resp.Body)
		storeID := string(storeIDBytes)

		after := time.Now()

		s := getStoreByID(storeMgr, storeID)
		if s == nil {
			t.Fatal("store not found")
		}

		// Default TTL is 14 days
		expectedMin := before.Add(14 * 24 * time.Hour)
		expectedMax := after.Add(14 * 24 * time.Hour)

		if s.ExpiresAt.Before(expectedMin) || s.ExpiresAt.After(expectedMax) {
			t.Errorf("expected default expiry between %v and %v, got %v", expectedMin, expectedMax, s.ExpiresAt)
		}
	})
}

func TestTTLOnUpdate(t *testing.T) {
	client, storeMgr, _, cleanup := setupTestServer(t)
	defer cleanup()

	t.Run("UpdateWithHeader_ResetsTTL", func(t *testing.T) {
		// Create store with 3600s TTL
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader([]byte("initial")))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Not-Valid-After", "3600")

		resp, _ := client.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		storeID := string(storeIDBytes)
		resp.Body.Close()

		// Get initial expiry
		s1 := getStoreByID(storeMgr, storeID)
		initialExpiry := s1.ExpiresAt

		// Wait a bit
		time.Sleep(100 * time.Millisecond)

		// Update with new TTL
		before := time.Now()
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, bytes.NewReader([]byte("updated")))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Not-Valid-After", "7200") // Different TTL

		resp, _ = client.Do(req)
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("update failed: %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()
		after := time.Now()

		// Get new expiry
		s2 := getStoreByID(storeMgr, storeID)
		newExpiry := s2.ExpiresAt

		// New expiry should be later than initial expiry (TTL was reset)
		if !newExpiry.After(initialExpiry) {
			t.Errorf("expected new expiry %v to be after initial expiry %v", newExpiry, initialExpiry)
		}

		// New expiry should be roughly now + 7200s
		expectedMin := before.Add(7200 * time.Second)
		expectedMax := after.Add(7200 * time.Second)
		if newExpiry.Before(expectedMin) || newExpiry.After(expectedMax) {
			t.Errorf("expected expiry between %v and %v, got %v", expectedMin, expectedMax, newExpiry)
		}
	})

	t.Run("UpdateWithoutHeader_PreservesTTL", func(t *testing.T) {
		// Create store with TTL
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader([]byte("initial")))
		req.Header.Set("X-Customer-ID", "cust2")
		req.Header.Set("BigBunny-Not-Valid-After", "3600")

		resp, _ := client.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		storeID := string(storeIDBytes)
		resp.Body.Close()

		// Get initial expiry
		s1 := getStoreByID(storeMgr, storeID)
		initialExpiry := s1.ExpiresAt

		// Wait a bit
		time.Sleep(100 * time.Millisecond)

		// Update WITHOUT TTL header
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, bytes.NewReader([]byte("updated")))
		req.Header.Set("X-Customer-ID", "cust2")
		// NO BigBunny-Not-Valid-After header

		resp, _ = client.Do(req)
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("update failed: %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()

		// Get new expiry - should be unchanged
		s2 := getStoreByID(storeMgr, storeID)
		newExpiry := s2.ExpiresAt

		if !newExpiry.Equal(initialExpiry) {
			t.Errorf("expected preserved expiry %v, got %v", initialExpiry, newExpiry)
		}
	})
}

func TestTTLOnCounterOperations(t *testing.T) {
	client, storeMgr, _, cleanup := setupTestServer(t)
	defer cleanup()

	t.Run("IncrementWithHeader_ResetsTTL", func(t *testing.T) {
		// Create counter
		createBody, _ := json.Marshal(map[string]interface{}{
			"type":  "counter",
			"value": 0,
		})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(createBody))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("BigBunny-Not-Valid-After", "3600")

		resp, _ := client.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		storeID := strings.TrimSpace(string(storeIDBytes))
		resp.Body.Close()

		// Get initial expiry
		s1 := getStoreByID(storeMgr, storeID)
		initialExpiry := s1.ExpiresAt

		// Wait a bit
		time.Sleep(100 * time.Millisecond)

		// Increment with new TTL
		before := time.Now()
		incBody, _ := json.Marshal(map[string]int64{"delta": 5})
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("BigBunny-Not-Valid-After", "7200") // New TTL

		resp, _ = client.Do(req)
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("increment failed: %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()
		after := time.Now()

		// Get new expiry
		s2 := getStoreByID(storeMgr, storeID)
		newExpiry := s2.ExpiresAt

		// New expiry should be later than initial
		if !newExpiry.After(initialExpiry) {
			t.Errorf("expected new expiry %v to be after initial expiry %v", newExpiry, initialExpiry)
		}

		// New expiry should be roughly now + 7200s
		expectedMin := before.Add(7200 * time.Second)
		expectedMax := after.Add(7200 * time.Second)
		if newExpiry.Before(expectedMin) || newExpiry.After(expectedMax) {
			t.Errorf("expected expiry between %v and %v, got %v", expectedMin, expectedMax, newExpiry)
		}
	})

	t.Run("IncrementWithoutHeader_PreservesTTL", func(t *testing.T) {
		// Create counter
		createBody, _ := json.Marshal(map[string]interface{}{
			"type":  "counter",
			"value": 10,
		})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(createBody))
		req.Header.Set("X-Customer-ID", "cust2")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("BigBunny-Not-Valid-After", "3600")

		resp, _ := client.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		storeID := strings.TrimSpace(string(storeIDBytes))
		resp.Body.Close()

		// Get initial expiry
		s1 := getStoreByID(storeMgr, storeID)
		initialExpiry := s1.ExpiresAt

		// Wait a bit
		time.Sleep(100 * time.Millisecond)

		// Increment WITHOUT TTL header
		incBody, _ := json.Marshal(map[string]int64{"delta": 3})
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust2")
		req.Header.Set("Content-Type", "application/json")
		// NO BigBunny-Not-Valid-After header

		resp, _ = client.Do(req)
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("increment failed: %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()

		// Expiry should be unchanged
		s2 := getStoreByID(storeMgr, storeID)
		newExpiry := s2.ExpiresAt

		if !newExpiry.Equal(initialExpiry) {
			t.Errorf("expected preserved expiry %v, got %v", initialExpiry, newExpiry)
		}
	})

	t.Run("DecrementWithHeader_ResetsTTL", func(t *testing.T) {
		// Create counter
		createBody, _ := json.Marshal(map[string]interface{}{
			"type":  "counter",
			"value": 100,
		})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(createBody))
		req.Header.Set("X-Customer-ID", "cust3")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("BigBunny-Not-Valid-After", "3600")

		resp, _ := client.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		storeID := strings.TrimSpace(string(storeIDBytes))
		resp.Body.Close()

		s1 := getStoreByID(storeMgr, storeID)
		initialExpiry := s1.ExpiresAt

		time.Sleep(100 * time.Millisecond)

		// Decrement with new TTL
		before := time.Now()
		decBody, _ := json.Marshal(map[string]int64{"delta": 20})
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/decrement/"+storeID, bytes.NewReader(decBody))
		req.Header.Set("X-Customer-ID", "cust3")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("BigBunny-Not-Valid-After", "300") // 5 minutes

		resp, _ = client.Do(req)
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("decrement failed: %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()
		after := time.Now()

		s2 := getStoreByID(storeMgr, storeID)
		newExpiry := s2.ExpiresAt

		// Expiry should be different (reset)
		if newExpiry.Equal(initialExpiry) {
			t.Error("expected expiry to change after decrement with header")
		}

		// Should be roughly now + 300s
		expectedMin := before.Add(300 * time.Second)
		expectedMax := after.Add(300 * time.Second)
		if newExpiry.Before(expectedMin) || newExpiry.After(expectedMax) {
			t.Errorf("expected expiry between %v and %v, got %v", expectedMin, expectedMax, newExpiry)
		}
	})

	t.Run("SetCounterWithHeader_ResetsTTL", func(t *testing.T) {
		// Create counter
		createBody, _ := json.Marshal(map[string]interface{}{
			"type":  "counter",
			"value": 50,
		})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(createBody))
		req.Header.Set("X-Customer-ID", "cust4")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("BigBunny-Not-Valid-After", "3600")

		resp, _ := client.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		storeID := strings.TrimSpace(string(storeIDBytes))
		resp.Body.Close()

		s1 := getStoreByID(storeMgr, storeID)
		initialExpiry := s1.ExpiresAt

		time.Sleep(100 * time.Millisecond)

		// Set counter value with new TTL
		before := time.Now()
		updateBody, _ := json.Marshal(map[string]int64{"value": 75})
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, bytes.NewReader(updateBody))
		req.Header.Set("X-Customer-ID", "cust4")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("BigBunny-Not-Valid-After", "1800") // 30 minutes

		resp, _ = client.Do(req)
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("update failed: %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()
		after := time.Now()

		s2 := getStoreByID(storeMgr, storeID)
		newExpiry := s2.ExpiresAt

		// Expiry should be different
		if newExpiry.Equal(initialExpiry) {
			t.Error("expected expiry to change after update with header")
		}

		// Should be roughly now + 1800s
		expectedMin := before.Add(1800 * time.Second)
		expectedMax := after.Add(1800 * time.Second)
		if newExpiry.Before(expectedMin) || newExpiry.After(expectedMax) {
			t.Errorf("expected expiry between %v and %v, got %v", expectedMin, expectedMax, newExpiry)
		}
	})
}

func TestTTLInSnapshot(t *testing.T) {
	client, _, _, cleanup := setupTestServer(t)
	defer cleanup()

	t.Run("SnapshotReturnsRemainingTTL", func(t *testing.T) {
		// Create store with 120s TTL
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader([]byte("test")))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Not-Valid-After", "120")

		resp, _ := client.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		storeID := strings.TrimSpace(string(storeIDBytes))
		resp.Body.Close()

		// Wait a bit
		time.Sleep(2 * time.Second)

		// Snapshot
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")

		resp, _ = client.Do(req)
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("snapshot failed: %d: %s", resp.StatusCode, body)
		}

		ttlHeader := resp.Header.Get("BigBunny-Not-Valid-After")
		resp.Body.Close()

		if ttlHeader == "" {
			t.Fatal("missing TTL header in snapshot response")
		}

		remainingTTL, _ := strconv.ParseInt(ttlHeader, 10, 64)

		// Should be less than 120 (we waited 2s) but more than 115 (allowing some slack)
		if remainingTTL >= 120 || remainingTTL < 115 {
			t.Errorf("expected remaining TTL between 115 and 119, got %d", remainingTTL)
		}
	})
}
