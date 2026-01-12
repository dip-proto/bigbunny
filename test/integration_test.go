package test

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
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

func TestAPIIntegration(t *testing.T) {
	// Set up components
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
	apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher)

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	// Start UDS server
	sockPath := "/tmp/bbd-test.sock"
	os.Remove(sockPath)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to create socket: %v", err)
	}
	defer os.Remove(sockPath)

	server := &http.Server{Handler: mux}
	go server.Serve(listener)
	defer server.Close()

	// Create HTTP client that uses UDS
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sockPath)
			},
		},
	}

	// Test: Create store
	t.Run("Create", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("test data"))
		req.Header.Set("X-Customer-ID", "customer123")
		req.Header.Set("BigBunny-Not-Valid-After", "3600")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("create request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("create failed: %d - %s", resp.StatusCode, body)
		}

		storeID, _ := io.ReadAll(resp.Body)
		// Encrypted store ID format: v1:{keyID}:{base64_ciphertext}
		if !strings.HasPrefix(string(storeID), "v1:") {
			t.Errorf("unexpected store ID format: %s", storeID)
		}
		parts := strings.Split(string(storeID), ":")
		if len(parts) != 3 {
			t.Errorf("expected 3 parts in store ID, got %d: %s", len(parts), storeID)
		}
	})

	// Test: Create and Snapshot
	t.Run("CreateAndSnapshot", func(t *testing.T) {
		// Create
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("snapshot test"))
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ := client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Snapshot
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("snapshot failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if string(body) != "snapshot test" {
			t.Errorf("body mismatch: got %q", body)
		}
	})

	// Test: Modify flow
	t.Run("ModifyFlow", func(t *testing.T) {
		// Create
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("initial"))
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ := client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Begin modify
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("begin-modify failed: %v", err)
		}

		lockID := resp.Header.Get("BigBunny-Lock-ID")
		if lockID == "" {
			t.Fatal("no lock ID returned")
		}
		resp.Body.Close()

		// Complete modify
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/complete-modify/"+string(storeID), strings.NewReader("modified"))
		req.Header.Set("X-Customer-ID", "customer123")
		req.Header.Set("BigBunny-Lock-ID", lockID)
		resp, err = client.Do(req)
		if err != nil {
			t.Fatalf("complete-modify failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("complete-modify status %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()

		// Verify
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ = client.Do(req)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if string(body) != "modified" {
			t.Errorf("body not updated: got %q", body)
		}
	})

	// Test: One-shot update (no manual lock management)
	t.Run("Update", func(t *testing.T) {
		// Create
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("initial value"))
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ := client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Update (one-shot, no lock management needed)
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/update/"+string(storeID), strings.NewReader("updated value"))
		req.Header.Set("X-Customer-ID", "customer123")
		req.Header.Set("BigBunny-Not-Valid-After", "7200")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("update failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("update status %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()

		// Verify update
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ = client.Do(req)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if string(body) != "updated value" {
			t.Errorf("body not updated: got %q, want %q", body, "updated value")
		}
	})

	// Test: Update on locked store returns StoreLocked
	t.Run("UpdateLocked", func(t *testing.T) {
		// Create
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("lock test"))
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ := client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Acquire lock via begin-modify
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ = client.Do(req)
		lockID := resp.Header.Get("BigBunny-Lock-ID")
		resp.Body.Close()

		// Try to update while locked - should fail with StoreLocked
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/update/"+string(storeID), strings.NewReader("should fail"))
		req.Header.Set("X-Customer-ID", "customer123")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("update request failed: %v", err)
		}
		if resp.StatusCode != http.StatusConflict {
			t.Errorf("expected 409 for locked store, got %d", resp.StatusCode)
		}
		if ec := resp.Header.Get("BigBunny-Error-Code"); ec != "StoreLocked" {
			t.Errorf("expected error code StoreLocked, got %q", ec)
		}
		resp.Body.Close()

		// Release the lock
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/cancel-modify/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "customer123")
		req.Header.Set("BigBunny-Lock-ID", lockID)
		resp, _ = client.Do(req)
		resp.Body.Close()
	})

	// Test: Delete
	t.Run("Delete", func(t *testing.T) {
		// Create
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("to delete"))
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ := client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Delete
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/delete/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("delete failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("delete status: %d", resp.StatusCode)
		}
		resp.Body.Close()

		// Verify gone
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ = client.Do(req)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404, got %d", resp.StatusCode)
		}
		resp.Body.Close()
	})

	// Test: Named store operations
	t.Run("NamedStoreOps", func(t *testing.T) {
		name := "named-store"

		// Create by name
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create-by-name/"+name, strings.NewReader("named body"))
		req.Header.Set("X-Customer-ID", "customer123")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("create-by-name failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("create-by-name status %d: %s", resp.StatusCode, body)
		}
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Create by name again without reuse should conflict
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/create-by-name/"+name, strings.NewReader("named body 2"))
		req.Header.Set("X-Customer-ID", "customer123")
		resp, err = client.Do(req)
		if err != nil {
			t.Fatalf("create-by-name duplicate failed: %v", err)
		}
		if resp.StatusCode != http.StatusConflict {
			t.Errorf("expected 409 on duplicate create-by-name, got %d", resp.StatusCode)
		}
		resp.Body.Close()

		// Create by name with reuse should return same ID
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/create-by-name/"+name, strings.NewReader("named body 3"))
		req.Header.Set("X-Customer-ID", "customer123")
		req.Header.Set("BigBunny-Reuse-If-Exists", "true")
		resp, err = client.Do(req)
		if err != nil {
			t.Fatalf("create-by-name reuse failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("create-by-name reuse status %d: %s", resp.StatusCode, body)
		}
		reusedID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(reusedID) != string(storeID) {
			t.Errorf("reuse returned different store ID: got %s, want %s", reusedID, storeID)
		}

		// Lookup by name
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/lookup-id-by-name/"+name, nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, err = client.Do(req)
		if err != nil {
			t.Fatalf("lookup-id-by-name failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("lookup-id-by-name status %d: %s", resp.StatusCode, body)
		}
		lookupID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(lookupID) != string(storeID) {
			t.Errorf("lookup returned different store ID: got %s, want %s", lookupID, storeID)
		}

		// Delete by name
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/delete-by-name/"+name, nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, err = client.Do(req)
		if err != nil {
			t.Fatalf("delete-by-name failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("delete-by-name status %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()

		// Lookup should now 404
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/lookup-id-by-name/"+name, nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, err = client.Do(req)
		if err != nil {
			t.Fatalf("lookup after delete failed: %v", err)
		}
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404 after delete-by-name, got %d", resp.StatusCode)
		}
		resp.Body.Close()

		// Snapshot by store ID should be gone
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ = client.Do(req)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404 for deleted store, got %d", resp.StatusCode)
		}
		resp.Body.Close()
	})

	// Test: Unauthorized access
	t.Run("Unauthorized", func(t *testing.T) {
		// Create as customer123
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("private"))
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ := client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Try to access as different customer
		// With encrypted store IDs, the store ID is cryptographically bound to customer123,
		// so when other-customer tries to decrypt it, decryption fails (400 Bad Request)
		// rather than reaching the customer authorization check (403).
		// This is actually more secure - the wrong customer can't even parse the ID.
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "other-customer")
		resp, _ = client.Do(req)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 (invalid store ID - wrong customer), got %d", resp.StatusCode)
		}
		resp.Body.Close()
	})

	// Test: Error codes are set on errors
	t.Run("ErrorCodes", func(t *testing.T) {
		// Invalid store ID format returns 400
		// With encrypted store IDs, "nonexistent-store" is not valid format
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/snapshot/nonexistent-store", nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ := client.Do(req)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 (invalid store ID format), got %d", resp.StatusCode)
		}
		resp.Body.Close()

		// StoreLocked error code
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("lock test"))
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ = client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Acquire lock
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ = client.Do(req)
		resp.Body.Close()

		// Try to acquire again - should get StoreLocked
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+string(storeID), nil)
		req.Header.Set("X-Customer-ID", "customer123")
		resp, _ = client.Do(req)
		if resp.StatusCode != http.StatusConflict {
			t.Errorf("expected 409 for locked store, got %d", resp.StatusCode)
		}
		if ec := resp.Header.Get("BigBunny-Error-Code"); ec != "StoreLocked" {
			t.Errorf("expected error code StoreLocked, got %q", ec)
		}
		resp.Body.Close()
	})
}

func TestLeaderChangedErrorCode(t *testing.T) {
	// Set up a secondary node
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	replicaCfg := replica.DefaultConfig("test-host", "test-site")
	replicaMgr := replica.NewManager(replicaCfg, storeMgr, hasher)
	registryMgr := registry.NewManager()
	replicaMgr.SetRegistry(registryMgr)
	replicaMgr.SetRole(replica.RoleSecondary) // Set as secondary
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
	apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher)

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	sockPath := "/tmp/bbd-test-secondary.sock"
	os.Remove(sockPath)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to create socket: %v", err)
	}
	defer os.Remove(sockPath)

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

	// Try to create on secondary - should get LeaderChanged
	// Use X-BB-No-Forward to test the error path (disable automatic forwarding)
	req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("test"))
	req.Header.Set("X-Customer-ID", "customer123")
	req.Header.Set("X-BB-No-Forward", "true")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 for secondary, got %d", resp.StatusCode)
	}
	if ec := resp.Header.Get("BigBunny-Error-Code"); ec != "LeaderChanged" {
		t.Errorf("expected error code LeaderChanged, got %q", ec)
	}
	if ra := resp.Header.Get("Retry-After"); ra == "" {
		t.Error("expected Retry-After header")
	}
}

func TestCapacityExceeded(t *testing.T) {
	// Set up store with small memory limit
	storeMgr := store.NewManagerWithLimit(512) // 512 bytes limit
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
	apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher)

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	sockPath := "/tmp/bbd-test-capacity.sock"
	os.Remove(sockPath)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to create socket: %v", err)
	}
	defer os.Remove(sockPath)

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

	// First create should succeed (256 overhead + small body)
	req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("first"))
	req.Header.Set("X-Customer-ID", "customer123")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("first create status %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Second create should fail with CapacityExceeded (507)
	req, _ = http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("second"))
	req.Header.Set("X-Customer-ID", "customer123")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("second create failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInsufficientStorage {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("expected 507 for capacity exceeded, got %d: %s", resp.StatusCode, body)
	}
	if ec := resp.Header.Get("BigBunny-Error-Code"); ec != "CapacityExceeded" {
		t.Errorf("expected error code CapacityExceeded, got %q", ec)
	}
}

func TestMemoryTrackingOnDelete(t *testing.T) {
	// Limit of 300 allows one store (256 overhead + body < 300)
	// but not two stores (512 overhead + bodies > 300)
	storeMgr := store.NewManagerWithLimit(300)
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
	apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher)

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	sockPath := "/tmp/bbd-test-memtrack.sock"
	os.Remove(sockPath)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to create socket: %v", err)
	}
	defer os.Remove(sockPath)

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
	req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("data"))
	req.Header.Set("X-Customer-ID", "customer123")
	resp, _ := client.Do(req)
	storeID, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// Memory should be used (256 overhead + 4 bytes body = 260)
	memAfterCreate := storeMgr.MemoryUsage()
	if memAfterCreate == 0 {
		t.Error("expected memory usage > 0 after create")
	}

	// Second create should fail (at capacity)
	req, _ = http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("more"))
	req.Header.Set("X-Customer-ID", "customer123")
	resp, _ = client.Do(req)
	if resp.StatusCode != http.StatusInsufficientStorage {
		t.Errorf("expected 507, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Delete the first store
	req, _ = http.NewRequest("POST", "http://localhost/api/v1/delete/"+string(storeID), nil)
	req.Header.Set("X-Customer-ID", "customer123")
	resp, _ = client.Do(req)
	resp.Body.Close()

	// Memory should be freed
	if storeMgr.MemoryUsage() != 0 {
		t.Errorf("expected memory usage 0 after delete, got %d", storeMgr.MemoryUsage())
	}

	// Now create should succeed again
	req, _ = http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("new"))
	req.Header.Set("X-Customer-ID", "customer123")
	resp, _ = client.Do(req)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("create after delete failed: %d - %s", resp.StatusCode, body)
	}
	resp.Body.Close()
}

func TestCapacityExceededOnCompleteModify(t *testing.T) {
	// Small limit that allows one store but not a much larger body
	storeMgr := store.NewManagerWithLimit(300)
	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	replicaCfg := replica.DefaultConfig("test-host", "test-site")
	replicaMgr := replica.NewManager(replicaCfg, storeMgr, hasher)
	replicaMgr.SetRole(replica.RolePrimary)
	replicaMgr.Start()
	defer replicaMgr.Stop()

	cipher := auth.NewCipher(auth.DevKeySet())
	apiCfg := &api.Config{
		Site:          "test-site",
		HostID:        "test-host",
		DefaultTTL:    time.Hour,
		MaxBodySize:   2 * 1024,
		ModifyTimeout: 5 * time.Second,
		Cipher:        cipher,
	}
	apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher)

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	sockPath := "/tmp/bbd-test-modify-cap.sock"
	os.Remove(sockPath)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to create socket: %v", err)
	}
	defer os.Remove(sockPath)

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

	// Create with tiny body
	req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("x"))
	req.Header.Set("X-Customer-ID", "customer123")
	resp, _ := client.Do(req)
	storeID, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// Begin modify
	req, _ = http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+string(storeID), nil)
	req.Header.Set("X-Customer-ID", "customer123")
	resp, _ = client.Do(req)
	lockID := resp.Header.Get("BigBunny-Lock-ID")
	resp.Body.Close()

	// Try to complete with body that exceeds capacity
	largeBody := strings.Repeat("x", 100) // Much larger than what fits
	req, _ = http.NewRequest("POST", "http://localhost/api/v1/complete-modify/"+string(storeID), strings.NewReader(largeBody))
	req.Header.Set("X-Customer-ID", "customer123")
	req.Header.Set("BigBunny-Lock-ID", lockID)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("complete-modify failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInsufficientStorage {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("expected 507 for capacity exceeded on modify, got %d: %s", resp.StatusCode, body)
	}
	if ec := resp.Header.Get("BigBunny-Error-Code"); ec != "CapacityExceeded" {
		t.Errorf("expected error code CapacityExceeded, got %q", ec)
	}
}
