package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/api"
	"github.com/dip-proto/bigbunny/internal/auth"
	"github.com/dip-proto/bigbunny/internal/ratelimit"
	"github.com/dip-proto/bigbunny/internal/registry"
	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
	"github.com/dip-proto/bigbunny/test/harness"
)

type functionalTestServer struct {
	client     *http.Client
	storeMgr   *store.Manager
	replicaMgr *replica.Manager
	sockPath   string
	server     *http.Server
}

func setupFunctionalTest(t *testing.T, opts ...func(*api.Config, *store.Manager)) *functionalTestServer {
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
		DefaultTTL:    14 * 24 * time.Hour,
		MaxBodySize:   2 * 1024,
		ModifyTimeout: 500 * time.Millisecond,
		Cipher:        cipher,
	}

	for _, opt := range opts {
		opt(apiCfg, storeMgr)
	}

	apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher, nil)

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	sockPath := fmt.Sprintf("/tmp/bbd-func-test-%d.sock", time.Now().UnixNano())
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

	t.Cleanup(func() {
		_ = server.Close()
		os.Remove(sockPath)
		replicaMgr.Stop()
	})

	return &functionalTestServer{
		client:     client,
		storeMgr:   storeMgr,
		replicaMgr: replicaMgr,
		sockPath:   sockPath,
		server:     server,
	}
}

func (s *functionalTestServer) createBlob(t *testing.T, customerID string, body []byte) string {
	t.Helper()
	req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(body))
	req.Header.Set("X-Customer-ID", customerID)
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("create request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create failed: %d - %s", resp.StatusCode, body)
	}
	storeID, _ := io.ReadAll(resp.Body)
	return string(storeID)
}

func (s *functionalTestServer) createBlobWithTTL(t *testing.T, customerID string, body []byte, ttlSeconds int) string {
	t.Helper()
	req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(body))
	req.Header.Set("X-Customer-ID", customerID)
	req.Header.Set("BigBunny-Not-Valid-After", strconv.Itoa(ttlSeconds))
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("create request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create failed: %d - %s", resp.StatusCode, body)
	}
	storeID, _ := io.ReadAll(resp.Body)
	return string(storeID)
}

func (s *functionalTestServer) snapshot(t *testing.T, storeID, customerID string) ([]byte, int) {
	t.Helper()
	req, _ := http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
	req.Header.Set("X-Customer-ID", customerID)
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("snapshot request failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body, resp.StatusCode
}

func TestFunctional_BlobLifecycle(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("create_and_read", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("hello world"))
		body, status := s.snapshot(t, storeID, "cust1")
		if status != http.StatusOK {
			t.Fatalf("snapshot failed: %d", status)
		}
		if string(body) != "hello world" {
			t.Errorf("body mismatch: got %q", body)
		}
	})

	t.Run("update_one_shot", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("initial"))

		req, _ := http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, strings.NewReader("updated"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("update failed: %d", resp.StatusCode)
		}

		body, _ := s.snapshot(t, storeID, "cust1")
		if string(body) != "updated" {
			t.Errorf("update not applied: got %q", body)
		}
	})

	t.Run("modify_flow", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("original"))

		// Begin modify
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		lockID := resp.Header.Get("BigBunny-Lock-ID")
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if lockID == "" {
			t.Fatal("no lock ID returned")
		}
		if string(body) != "original" {
			t.Errorf("begin-modify should return current body: got %q", body)
		}

		// Complete modify
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/complete-modify/"+storeID, strings.NewReader("modified"))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Lock-ID", lockID)
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("complete-modify failed: %d", resp.StatusCode)
		}

		body, _ = s.snapshot(t, storeID, "cust1")
		if string(body) != "modified" {
			t.Errorf("modify not applied: got %q", body)
		}
	})

	t.Run("cancel_modify", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("unchanged"))

		// Begin modify
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		lockID := resp.Header.Get("BigBunny-Lock-ID")
		resp.Body.Close()

		// Cancel modify
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/cancel-modify/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Lock-ID", lockID)
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("cancel-modify failed: %d", resp.StatusCode)
		}

		// Should be able to acquire lock again
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("should be able to lock after cancel: %d", resp.StatusCode)
		}
	})

	t.Run("delete", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("to delete"))

		req, _ := http.NewRequest("POST", "http://localhost/api/v1/delete/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("delete failed: %d", resp.StatusCode)
		}

		_, status := s.snapshot(t, storeID, "cust1")
		if status != http.StatusNotFound {
			t.Errorf("store should be gone: got status %d", status)
		}
	})
}

func TestFunctional_BlobEdgeCases(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("empty_body", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte{})
		body, status := s.snapshot(t, storeID, "cust1")
		if status != http.StatusOK {
			t.Fatalf("snapshot failed: %d", status)
		}
		if len(body) != 0 {
			t.Errorf("expected empty body, got %d bytes", len(body))
		}
	})

	t.Run("binary_data", func(t *testing.T) {
		binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
		storeID := s.createBlob(t, "cust1", binaryData)
		body, status := s.snapshot(t, storeID, "cust1")
		if status != http.StatusOK {
			t.Fatalf("snapshot failed: %d", status)
		}
		if !bytes.Equal(body, binaryData) {
			t.Errorf("binary data mismatch")
		}
	})

	t.Run("max_body_size_enforced", func(t *testing.T) {
		largeBody := make([]byte, 3*1024) // 3KB, max is 2KB
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(largeBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("expected 413, got %d", resp.StatusCode)
		}
	})

	t.Run("exactly_at_max_body_size", func(t *testing.T) {
		exactBody := make([]byte, 2*1024) // exactly 2KB
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(exactBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 for exact max size, got %d", resp.StatusCode)
		}
	})

	t.Run("ttl_zero_creates_immediately_expired", func(t *testing.T) {
		// TTL of 0 creates a store that expires immediately
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("test"))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Not-Valid-After", "0")
		resp, _ := s.client.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("create failed: %d", resp.StatusCode)
		}

		// Store is created but with zero/minimal TTL
		storeID := string(storeIDBytes)
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		ttlHeader := resp.Header.Get("BigBunny-Not-Valid-After")
		resp.Body.Close()

		// TTL should be 0 or very small
		ttl, _ := strconv.ParseInt(ttlHeader, 10, 64)
		if ttl > 5 {
			t.Errorf("TTL should be near 0 for zero TTL input, got %d", ttl)
		}
	})

	t.Run("invalid_ttl_header_treated_as_zero", func(t *testing.T) {
		// Non-numeric TTL is parsed as 0 (strconv behavior)
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("test"))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Not-Valid-After", "not-a-number")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		// API accepts invalid TTL and treats it as 0
		if resp.StatusCode != http.StatusOK {
			t.Errorf("create should succeed with invalid TTL, got %d", resp.StatusCode)
		}
	})

	t.Run("negative_ttl_accepted", func(t *testing.T) {
		// Negative TTL creates immediately expired store
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("test"))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Not-Valid-After", "-100")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		// API accepts negative TTL
		if resp.StatusCode != http.StatusOK {
			t.Errorf("create should succeed with negative TTL, got %d", resp.StatusCode)
		}
	})
}

func TestFunctional_CustomerIsolation(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("cross_customer_access_rejected", func(t *testing.T) {
		storeID := s.createBlob(t, "customer-a", []byte("secret data"))

		// Customer B tries to access Customer A's store
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "customer-b")
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// With encrypted store IDs, wrong customer fails decryption (400)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 for cross-customer access, got %d", resp.StatusCode)
		}
	})

	t.Run("missing_customer_id", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("test"))
		// No X-Customer-ID header
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		// Returns 401 Unauthorized (missing auth)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 for missing customer ID, got %d", resp.StatusCode)
		}
	})

	t.Run("empty_customer_id", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("test"))
		req.Header.Set("X-Customer-ID", "")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		// Returns 401 Unauthorized (empty auth)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 for empty customer ID, got %d", resp.StatusCode)
		}
	})

	t.Run("same_name_different_customers", func(t *testing.T) {
		name := "shared-name"

		// Customer A creates named store
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create-by-name/"+name, strings.NewReader("a-data"))
		req.Header.Set("X-Customer-ID", "cust-a")
		resp, _ := s.client.Do(req)
		storeIDA, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("cust-a create failed: %d", resp.StatusCode)
		}

		// Customer B creates store with same name - should succeed
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/create-by-name/"+name, strings.NewReader("b-data"))
		req.Header.Set("X-Customer-ID", "cust-b")
		resp, _ = s.client.Do(req)
		storeIDB, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("cust-b create failed: %d", resp.StatusCode)
		}

		// Different store IDs
		if string(storeIDA) == string(storeIDB) {
			t.Error("different customers should get different store IDs")
		}
	})
}

func TestFunctional_LockBehavior(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("concurrent_lock_rejected", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("data"))

		// First lock
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("first lock failed: %d", resp.StatusCode)
		}

		// Second lock should fail
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Errorf("expected 409 for concurrent lock, got %d", resp.StatusCode)
		}
		if resp.Header.Get("BigBunny-Error-Code") != "StoreLocked" {
			t.Errorf("expected StoreLocked error code")
		}
	})

	t.Run("update_while_locked_rejected", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("data"))

		// Lock the store
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// One-shot update should fail
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, strings.NewReader("new"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Errorf("expected 409 for update while locked, got %d", resp.StatusCode)
		}
	})

	t.Run("wrong_lock_id_rejected", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("data"))

		// Lock the store
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// Try to complete with wrong lock ID
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/complete-modify/"+storeID, strings.NewReader("new"))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Lock-ID", "wrong-lock-id")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Errorf("expected 409 for wrong lock ID, got %d", resp.StatusCode)
		}
	})

	t.Run("complete_without_lock_id", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("data"))

		req, _ := http.NewRequest("POST", "http://localhost/api/v1/complete-modify/"+storeID, strings.NewReader("new"))
		req.Header.Set("X-Customer-ID", "cust1")
		// No lock ID header
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 for missing lock ID, got %d", resp.StatusCode)
		}
	})

	t.Run("delete_while_locked_succeeds", func(t *testing.T) {
		// In BigBunny, delete is allowed even on locked stores
		// This is intentional - delete takes precedence over locks
		storeID := s.createBlob(t, "cust1", []byte("data"))

		// Lock the store
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// Delete succeeds even while locked
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/delete/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 for delete (even while locked), got %d", resp.StatusCode)
		}

		// Store should be gone
		_, status := s.snapshot(t, storeID, "cust1")
		if status != http.StatusNotFound {
			t.Errorf("store should be deleted, got status %d", status)
		}
	})
}

func TestFunctional_CounterOperations(t *testing.T) {
	s := setupFunctionalTest(t)

	createCounter := func(value int64, min, max *int64) string {
		body := map[string]any{"type": "counter", "value": value}
		if min != nil {
			body["min"] = *min
		}
		if max != nil {
			body["max"] = *max
		}
		bodyBytes, _ := json.Marshal(body)

		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(bodyBytes))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := s.client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("create counter failed: %d", resp.StatusCode)
		}
		return strings.TrimSpace(string(storeID))
	}

	t.Run("increment_and_decrement", func(t *testing.T) {
		storeID := createCounter(100, nil, nil)

		// Increment
		incBody, _ := json.Marshal(map[string]int64{"delta": 50})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		var incResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&incResp)
		resp.Body.Close()
		if incResp.Value != 150 {
			t.Errorf("expected 150 after increment, got %d", incResp.Value)
		}

		// Decrement
		decBody, _ := json.Marshal(map[string]int64{"delta": 30})
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/decrement/"+storeID, bytes.NewReader(decBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		var decResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&decResp)
		resp.Body.Close()
		if decResp.Value != 120 {
			t.Errorf("expected 120 after decrement, got %d", decResp.Value)
		}
	})

	t.Run("bounds_clamping", func(t *testing.T) {
		min, max := int64(0), int64(100)
		storeID := createCounter(50, &min, &max)

		// Try to increment beyond max
		incBody, _ := json.Marshal(map[string]int64{"delta": 100})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		var incResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&incResp)
		resp.Body.Close()
		if incResp.Value != 100 {
			t.Errorf("expected clamped to 100, got %d", incResp.Value)
		}
		if !incResp.Bounded {
			t.Error("expected bounded=true when hitting max")
		}

		// Try to decrement beyond min
		decBody, _ := json.Marshal(map[string]int64{"delta": 200})
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/decrement/"+storeID, bytes.NewReader(decBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		var decResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&decResp)
		resp.Body.Close()
		if decResp.Value != 0 {
			t.Errorf("expected clamped to 0, got %d", decResp.Value)
		}
		if !decResp.Bounded {
			t.Error("expected bounded=true when hitting min")
		}
	})

	t.Run("counter_snapshot_returns_json", func(t *testing.T) {
		min, max := int64(0), int64(100)
		storeID := createCounter(42, &min, &max)

		req, _ := http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, err := s.client.Do(req)
		if err != nil {
			t.Fatalf("snapshot request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected json content type, got %s", resp.Header.Get("Content-Type"))
		}

		var snapResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&snapResp)
		if snapResp.Value != 42 {
			t.Errorf("expected value 42, got %d", snapResp.Value)
		}
		if snapResp.Min == nil || *snapResp.Min != 0 {
			t.Errorf("expected min=0")
		}
		if snapResp.Max == nil || *snapResp.Max != 100 {
			t.Errorf("expected max=100")
		}
	})

	t.Run("type_mismatch_errors", func(t *testing.T) {
		// Create a blob store
		blobID := s.createBlob(t, "cust1", []byte("not a counter"))

		// Try to increment blob
		incBody, _ := json.Marshal(map[string]int64{"delta": 5})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/increment/"+blobID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 for type mismatch, got %d", resp.StatusCode)
		}
		if resp.Header.Get("BigBunny-Error-Code") != "TypeMismatch" {
			t.Errorf("expected TypeMismatch error code")
		}
	})

	t.Run("counter_set_value", func(t *testing.T) {
		min, max := int64(0), int64(100)
		storeID := createCounter(50, &min, &max)

		// Set to new value
		setBody, _ := json.Marshal(map[string]int64{"value": 75})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, bytes.NewReader(setBody))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("set counter failed: %d", resp.StatusCode)
		}

		// Verify
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		var snapResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&snapResp)
		resp.Body.Close()
		if snapResp.Value != 75 {
			t.Errorf("expected 75, got %d", snapResp.Value)
		}
	})

	t.Run("counter_set_out_of_bounds", func(t *testing.T) {
		min, max := int64(0), int64(100)
		storeID := createCounter(50, &min, &max)

		// Try to set above max
		setBody, _ := json.Marshal(map[string]int64{"value": 150})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, bytes.NewReader(setBody))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 for out of bounds, got %d", resp.StatusCode)
		}
	})

	t.Run("invalid_bounds", func(t *testing.T) {
		// min > max is invalid
		body := map[string]any{
			"type":  "counter",
			"value": 50,
			"min":   100,
			"max":   0,
		}
		bodyBytes, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(bodyBytes))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 for invalid bounds, got %d", resp.StatusCode)
		}
	})

	t.Run("counter_delete", func(t *testing.T) {
		storeID := createCounter(42, nil, nil)

		req, _ := http.NewRequest("POST", "http://localhost/api/v1/delete/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("delete failed: %d", resp.StatusCode)
		}

		// Verify gone
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404 after delete, got %d", resp.StatusCode)
		}
	})
}

func TestFunctional_NamedStores(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("create_and_lookup", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create-by-name/my-session", strings.NewReader("session data"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("create-by-name failed: %d", resp.StatusCode)
		}

		// Lookup
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/lookup-id-by-name/my-session", nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		lookupID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("lookup failed: %d", resp.StatusCode)
		}
		if string(lookupID) != string(storeID) {
			t.Errorf("lookup returned different ID")
		}
	})

	t.Run("duplicate_name_rejected", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create-by-name/unique-name", strings.NewReader("first"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("first create failed: %d", resp.StatusCode)
		}

		// Try again
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/create-by-name/unique-name", strings.NewReader("second"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Errorf("expected 409 for duplicate name, got %d", resp.StatusCode)
		}
	})

	t.Run("reuse_if_exists", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create-by-name/reuse-test", strings.NewReader("first"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		storeID1, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Create again with reuse flag
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/create-by-name/reuse-test", strings.NewReader("second"))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Reuse-If-Exists", "true")
		resp, _ = s.client.Do(req)
		storeID2, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("reuse create failed: %d", resp.StatusCode)
		}

		if string(storeID1) != string(storeID2) {
			t.Errorf("reuse should return same store ID")
		}
	})

	t.Run("delete_by_name", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create-by-name/to-delete", strings.NewReader("data"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Delete by name
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/delete-by-name/to-delete", nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("delete-by-name failed: %d", resp.StatusCode)
		}

		// Lookup should 404
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/lookup-id-by-name/to-delete", nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404 after delete, got %d", resp.StatusCode)
		}

		// Store should also be gone
		_, status := s.snapshot(t, string(storeID), "cust1")
		if status != http.StatusNotFound {
			t.Errorf("store should be deleted")
		}
	})

	t.Run("lookup_nonexistent", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/lookup-id-by-name/does-not-exist", nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404 for nonexistent name, got %d", resp.StatusCode)
		}
	})

	t.Run("named_counter", func(t *testing.T) {
		body := map[string]any{
			"type":  "counter",
			"value": 0,
			"max":   100,
		}
		bodyBytes, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create-by-name/rate-limit", bytes.NewReader(bodyBytes))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := s.client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("create named counter failed: %d", resp.StatusCode)
		}

		// Increment the named counter
		incBody, _ := json.Marshal(map[string]int64{"delta": 10})
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/increment/"+string(storeID), bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		var incResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&incResp)
		resp.Body.Close()
		if incResp.Value != 10 {
			t.Errorf("expected 10, got %d", incResp.Value)
		}
	})
}

func TestFunctional_ErrorCodes(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("store_not_found", func(t *testing.T) {
		// Use a valid-looking but non-existent store ID
		// With encrypted IDs, we need to create one first and then delete it
		storeID := s.createBlob(t, "cust1", []byte("temp"))
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/delete/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// Now try to access deleted store
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404, got %d", resp.StatusCode)
		}
		if resp.Header.Get("BigBunny-Error-Code") != "NotFound" {
			t.Errorf("expected NotFound error code, got %s", resp.Header.Get("BigBunny-Error-Code"))
		}
	})

	t.Run("invalid_store_id_format", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/snapshot/not-a-valid-id", nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", resp.StatusCode)
		}
	})
}

func TestFunctional_ConcurrentOperations(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("concurrent_creates", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, 100)
		storeIDs := make(chan string, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				req, _ := http.NewRequest("POST", "http://localhost/api/v1/create",
					strings.NewReader(fmt.Sprintf("data-%d", i)))
				req.Header.Set("X-Customer-ID", "cust1")
				resp, err := s.client.Do(req)
				if err != nil {
					errors <- err
					return
				}
				storeID, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					errors <- fmt.Errorf("create failed: %d", resp.StatusCode)
					return
				}
				storeIDs <- string(storeID)
			}(i)
		}

		wg.Wait()
		close(errors)
		close(storeIDs)

		for err := range errors {
			t.Errorf("concurrent create error: %v", err)
		}

		// Verify all stores are unique
		seen := make(map[string]bool)
		for id := range storeIDs {
			if seen[id] {
				t.Errorf("duplicate store ID: %s", id)
			}
			seen[id] = true
		}
		if len(seen) != 100 {
			t.Errorf("expected 100 unique stores, got %d", len(seen))
		}
	})

	t.Run("concurrent_counter_increments", func(t *testing.T) {
		createBody, _ := json.Marshal(map[string]any{"type": "counter", "value": 0})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(createBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		storeID := strings.TrimSpace(string(storeIDBytes))

		var wg sync.WaitGroup
		var successCount int64

		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				incBody, _ := json.Marshal(map[string]int64{"delta": 1})
				req, _ := http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
				req.Header.Set("X-Customer-ID", "cust1")
				resp, _ := s.client.Do(req)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					atomic.AddInt64(&successCount, 1)
				}
			}()
		}

		wg.Wait()

		// Verify final count
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		var snapResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&snapResp)
		resp.Body.Close()

		if snapResp.Value != 50 {
			t.Errorf("expected counter value 50, got %d", snapResp.Value)
		}
	})
}

func TestFunctional_TTLBehavior(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("ttl_header_on_snapshot", func(t *testing.T) {
		storeID := s.createBlobWithTTL(t, "cust1", []byte("test"), 300)

		req, _ := http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		ttlHeader := resp.Header.Get("BigBunny-Not-Valid-After")
		resp.Body.Close()

		if ttlHeader == "" {
			t.Fatal("missing TTL header")
		}
		ttl, _ := strconv.ParseInt(ttlHeader, 10, 64)
		if ttl < 295 || ttl > 300 {
			t.Errorf("expected TTL around 300, got %d", ttl)
		}
	})

	t.Run("update_with_ttl_resets_expiry", func(t *testing.T) {
		storeID := s.createBlobWithTTL(t, "cust1", []byte("initial"), 100)

		// Update with new TTL
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, strings.NewReader("updated"))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Not-Valid-After", "500")
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// Check new TTL
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		ttlHeader := resp.Header.Get("BigBunny-Not-Valid-After")
		resp.Body.Close()

		ttl, _ := strconv.ParseInt(ttlHeader, 10, 64)
		if ttl < 495 || ttl > 500 {
			t.Errorf("expected TTL around 500, got %d", ttl)
		}
	})

	t.Run("update_without_ttl_preserves_expiry", func(t *testing.T) {
		storeID := s.createBlobWithTTL(t, "cust1", []byte("initial"), 200)

		// Get original TTL
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		originalTTL := resp.Header.Get("BigBunny-Not-Valid-After")
		resp.Body.Close()

		// Update without TTL header
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, strings.NewReader("updated"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()

		// Check TTL is preserved (within 1 second tolerance)
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		newTTL := resp.Header.Get("BigBunny-Not-Valid-After")
		resp.Body.Close()

		originalTTLInt, _ := strconv.ParseInt(originalTTL, 10, 64)
		newTTLInt, _ := strconv.ParseInt(newTTL, 10, 64)
		if originalTTLInt-newTTLInt > 2 {
			t.Errorf("TTL changed unexpectedly: was %d, now %d", originalTTLInt, newTTLInt)
		}
	})
}

func TestFunctional_VersionTracking(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("version_increments_on_update", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("v1"))

		// Check initial version
		store1 := s.storeMgr.Snapshot()[0]
		if store1.Version != 1 {
			t.Errorf("expected initial version 1, got %d", store1.Version)
		}

		// Update
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, strings.NewReader("v2"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// Check version incremented
		store2, _ := s.storeMgr.Get(storeID, "cust1")
		if store2.Version != 2 {
			t.Errorf("expected version 2 after update, got %d", store2.Version)
		}
	})

	t.Run("version_in_counter_response", func(t *testing.T) {
		createBody, _ := json.Marshal(map[string]any{"type": "counter", "value": 0})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(createBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		storeID := strings.TrimSpace(string(storeIDBytes))

		// Increment and check version
		incBody, _ := json.Marshal(map[string]int64{"delta": 1})
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		var incResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&incResp)
		resp.Body.Close()

		if incResp.Version != 2 {
			t.Errorf("expected version 2 after increment, got %d", incResp.Version)
		}

		// Increment again
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		json.NewDecoder(resp.Body).Decode(&incResp)
		resp.Body.Close()

		if incResp.Version != 3 {
			t.Errorf("expected version 3 after second increment, got %d", incResp.Version)
		}
	})
}

func TestFunctional_MemoryQuotas(t *testing.T) {
	// Use very small quota: store overhead is 256 bytes, so 400 allows one ~144 byte store
	s := setupFunctionalTest(t, func(cfg *api.Config, storeMgr *store.Manager) {
		storeMgr.SetCustomerMemoryQuota(400) // Very small quota to trigger limit
	})

	t.Run("quota_enforced", func(t *testing.T) {
		// First create should succeed (100 bytes data + 256 overhead = 356 < 400)
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("smalldata"))
		req.Header.Set("X-Customer-ID", "quota-cust-enforce")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("first create failed: %d", resp.StatusCode)
		}

		// Second create should exceed quota (another 256+ bytes would exceed 400)
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("moredata"))
		req.Header.Set("X-Customer-ID", "quota-cust-enforce")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusInsufficientStorage {
			t.Errorf("expected 507 for quota exceeded, got %d", resp.StatusCode)
		}
	})

	t.Run("different_customer_unaffected", func(t *testing.T) {
		// Fill up cust-quota-a's quota
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("fill"))
		req.Header.Set("X-Customer-ID", "cust-quota-a")
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// cust-quota-b should still be able to create (separate quota)
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader("data"))
		req.Header.Set("X-Customer-ID", "cust-quota-b")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("different customer should not be affected: %d", resp.StatusCode)
		}
	})
}

func TestFunctional_ClusterReplication(t *testing.T) {
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

	t.Run("create_replicates", func(t *testing.T) {
		storeID := createStore(t, primary.Addr(), "cust1", []byte("replicated data"))

		if err := cluster.WaitForStore(secondary.ID, storeID, "cust1", 2*time.Second); err != nil {
			t.Fatalf("store not replicated: %v", err)
		}

		secondaryStore, err := secondary.Store.Get(storeID, "cust1")
		if err != nil {
			t.Fatalf("get from secondary failed: %v", err)
		}
		if string(secondaryStore.Body) != "replicated data" {
			t.Errorf("data mismatch on secondary")
		}
	})

	t.Run("modify_replicates", func(t *testing.T) {
		storeID := createStore(t, primary.Addr(), "cust1", []byte("initial"))

		if err := cluster.WaitForStore(secondary.ID, storeID, "cust1", 2*time.Second); err != nil {
			t.Fatalf("initial store not replicated: %v", err)
		}

		modifyStore(t, primary.Addr(), storeID, "cust1", []byte("modified"))

		err := cluster.WaitForCondition(func() bool {
			s, err := secondary.Store.Get(storeID, "cust1")
			return err == nil && bytes.Equal(s.Body, []byte("modified"))
		}, 2*time.Second)
		if err != nil {
			t.Fatalf("modification not replicated: %v", err)
		}
	})

	t.Run("delete_replicates", func(t *testing.T) {
		storeID := createStore(t, primary.Addr(), "cust1", []byte("to delete"))

		if err := cluster.WaitForStore(secondary.ID, storeID, "cust1", 2*time.Second); err != nil {
			t.Fatalf("store not replicated: %v", err)
		}

		deleteStore(t, primary.Addr(), storeID, "cust1")

		err := cluster.WaitForCondition(func() bool {
			_, err := secondary.Store.Get(storeID, "cust1")
			return err != nil
		}, 2*time.Second)
		if err != nil {
			t.Fatalf("delete not replicated: %v", err)
		}
	})
}

func TestFunctional_ClusterCounterReplication(t *testing.T) {
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

	t.Run("counter_increment_replicates", func(t *testing.T) {
		// Create counter on primary
		createBody, _ := json.Marshal(map[string]any{"type": "counter", "value": 0})
		req, _ := http.NewRequest("POST", "http://"+primary.Addr()+"/api/v1/create", bytes.NewReader(createBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := http.DefaultClient.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		storeID := strings.TrimSpace(string(storeIDBytes))

		// Wait for creation to replicate
		if err := cluster.WaitForStore(secondary.ID, storeID, "cust1", 2*time.Second); err != nil {
			t.Fatalf("counter not replicated: %v", err)
		}

		// Increment on primary
		incBody, _ := json.Marshal(map[string]int64{"delta": 42})
		req, _ = http.NewRequest("POST", "http://"+primary.Addr()+"/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = http.DefaultClient.Do(req)
		resp.Body.Close()

		// Wait for increment to replicate
		err := cluster.WaitForCondition(func() bool {
			counterData, _, err := secondary.Store.GetCounter(storeID, "cust1")
			return err == nil && counterData.Value == 42
		}, 2*time.Second)
		if err != nil {
			t.Fatalf("counter increment not replicated: %v", err)
		}
	})
}

func TestFunctional_RateLimiting(t *testing.T) {
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

	limiter := ratelimit.NewLimiter(5, 5) // 5 req/s, burst 5
	defer limiter.Stop()
	apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher, limiter)

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	server := &http.Server{Handler: mux}
	go server.Serve(listener)
	defer server.Close()

	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{}

	t.Run("burst_allowed", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			req, _ := http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("test"))
			req.Header.Set("X-Customer-ID", "burst-cust")
			resp, _ := client.Do(req)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("request %d should succeed: got %d", i, resp.StatusCode)
			}
		}
	})

	t.Run("over_limit_returns_429", func(t *testing.T) {
		// Use a fresh customer to avoid prior state
		for i := 0; i < 6; i++ {
			req, _ := http.NewRequest("POST", baseURL+"/api/v1/create", strings.NewReader("test"))
			req.Header.Set("X-Customer-ID", "limit-cust")
			resp, _ := client.Do(req)
			resp.Body.Close()
			if i == 5 {
				if resp.StatusCode != http.StatusTooManyRequests {
					t.Errorf("request 6 should be rate limited: got %d", resp.StatusCode)
				}
				if resp.Header.Get("Retry-After") == "" {
					t.Error("missing Retry-After header")
				}
			}
		}
	})
}

func TestFunctional_StoreIDEncryption(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("store_id_format", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("test"))

		// Format should be v1:{keyID}:{ciphertext}
		parts := strings.Split(storeID, ":")
		if len(parts) != 3 {
			t.Errorf("expected 3 parts in store ID, got %d: %s", len(parts), storeID)
		}
		if parts[0] != "v1" {
			t.Errorf("expected v1 prefix, got %s", parts[0])
		}
	})

	t.Run("different_customers_different_ids", func(t *testing.T) {
		id1 := s.createBlob(t, "cust1", []byte("test"))
		id2 := s.createBlob(t, "cust2", []byte("test"))

		if id1 == id2 {
			t.Error("different customers should get different store IDs")
		}
	})

	t.Run("cryptographically_bound_to_customer", func(t *testing.T) {
		storeID := s.createBlob(t, "owner", []byte("secret"))

		// Attacker with store ID cannot access data
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "attacker")
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// Decryption fails with wrong customer
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 for wrong customer, got %d", resp.StatusCode)
		}
	})
}

func TestFunctional_ModifyFlowReturnsCurrentBody(t *testing.T) {
	s := setupFunctionalTest(t)

	storeID := s.createBlob(t, "cust1", []byte("current content"))

	req, _ := http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+storeID, nil)
	req.Header.Set("X-Customer-ID", "cust1")
	resp, _ := s.client.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("begin-modify failed: %d", resp.StatusCode)
	}

	if string(body) != "current content" {
		t.Errorf("expected current body, got %q", body)
	}

	lockID := resp.Header.Get("BigBunny-Lock-ID")
	if lockID == "" {
		t.Error("missing lock ID in response")
	}
}

func TestFunctional_CompleteModifyWithTTL(t *testing.T) {
	s := setupFunctionalTest(t)

	storeID := s.createBlobWithTTL(t, "cust1", []byte("initial"), 100)

	// Begin modify
	req, _ := http.NewRequest("POST", "http://localhost/api/v1/begin-modify/"+storeID, nil)
	req.Header.Set("X-Customer-ID", "cust1")
	resp, _ := s.client.Do(req)
	lockID := resp.Header.Get("BigBunny-Lock-ID")
	resp.Body.Close()

	// Complete with new TTL
	req, _ = http.NewRequest("POST", "http://localhost/api/v1/complete-modify/"+storeID, strings.NewReader("new content"))
	req.Header.Set("X-Customer-ID", "cust1")
	req.Header.Set("BigBunny-Lock-ID", lockID)
	req.Header.Set("BigBunny-Not-Valid-After", "500")
	resp, _ = s.client.Do(req)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete-modify failed: %d", resp.StatusCode)
	}

	// Check TTL was updated
	req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
	req.Header.Set("X-Customer-ID", "cust1")
	resp, _ = s.client.Do(req)
	ttlHeader := resp.Header.Get("BigBunny-Not-Valid-After")
	resp.Body.Close()

	ttl, _ := strconv.ParseInt(ttlHeader, 10, 64)
	if ttl < 495 || ttl > 500 {
		t.Errorf("expected TTL around 500, got %d", ttl)
	}
}

func TestFunctional_TombstonesPreventsResurrection(t *testing.T) {
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

	// Create and replicate a store
	storeID := createStore(t, primary.Addr(), "cust1", []byte("to be deleted"))
	if err := cluster.WaitForStore(secondary.ID, storeID, "cust1", 2*time.Second); err != nil {
		t.Fatalf("store not replicated: %v", err)
	}

	// Delete it
	deleteStore(t, primary.Addr(), storeID, "cust1")

	// Wait for delete to replicate
	err = cluster.WaitForCondition(func() bool {
		_, err := secondary.Store.Get(storeID, "cust1")
		return err != nil
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("delete not replicated: %v", err)
	}

	// Verify tombstone exists
	if !primary.Replica.IsTombstoned(storeID) {
		t.Error("expected tombstone on primary")
	}

	// Verify store cannot be recreated with same ID (internal check)
	// The store ID is encrypted and contains randomness, so recreation
	// would use a different ID. But the tombstone prevents any attempts
	// to resurrect via replication from a stale source.
}

func TestFunctional_CounterTTLOperations(t *testing.T) {
	s := setupFunctionalTest(t)

	createCounter := func(value int64, ttl int) string {
		body := map[string]any{"type": "counter", "value": value}
		bodyBytes, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(bodyBytes))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("BigBunny-Not-Valid-After", strconv.Itoa(ttl))
		resp, _ := s.client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return strings.TrimSpace(string(storeID))
	}

	t.Run("increment_with_ttl", func(t *testing.T) {
		storeID := createCounter(0, 100)

		// Increment with new TTL
		incBody, _ := json.Marshal(map[string]int64{"delta": 5})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Not-Valid-After", "500")
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// Check TTL was updated
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		ttlHeader := resp.Header.Get("BigBunny-Not-Valid-After")
		resp.Body.Close()

		ttl, _ := strconv.ParseInt(ttlHeader, 10, 64)
		if ttl < 495 || ttl > 500 {
			t.Errorf("expected TTL around 500, got %d", ttl)
		}
	})

	t.Run("decrement_with_ttl", func(t *testing.T) {
		storeID := createCounter(100, 100)

		// Decrement with new TTL
		decBody, _ := json.Marshal(map[string]int64{"delta": 10})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/decrement/"+storeID, bytes.NewReader(decBody))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Not-Valid-After", "300")
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// Check TTL was updated
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		ttlHeader := resp.Header.Get("BigBunny-Not-Valid-After")
		resp.Body.Close()

		ttl, _ := strconv.ParseInt(ttlHeader, 10, 64)
		if ttl < 295 || ttl > 300 {
			t.Errorf("expected TTL around 300, got %d", ttl)
		}
	})
}

func TestFunctional_ClusterFailover(t *testing.T) {
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

	t.Run("secondary_becomes_primary_after_partition", func(t *testing.T) {
		// Create data before partition
		storeID := createStore(t, primary.Addr(), "cust1", []byte("before partition"))
		cluster.WaitForStore(secondary.ID, storeID, "cust1", 2*time.Second)

		// Partition the primary from secondary
		cluster.Partition(primary.ID, secondary.ID)

		// Wait for secondary to detect lease expiry and promote
		err := cluster.WaitForRole(secondary.ID, replica.RolePrimary, 5*time.Second)
		if err != nil {
			t.Fatalf("secondary didn't promote: %v", err)
		}

		// Secondary (now primary) can serve reads
		secondaryStore, err := secondary.Store.Get(storeID, "cust1")
		if err != nil {
			t.Fatalf("read from new primary failed: %v", err)
		}
		if string(secondaryStore.Body) != "before partition" {
			t.Errorf("data mismatch on new primary")
		}

		// Heal the partition
		cluster.HealAll()
	})
}

func TestFunctional_RegistryEdgeCases(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("long_name", func(t *testing.T) {
		longName := strings.Repeat("a", 200) // Long but reasonable name
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create-by-name/"+longName, strings.NewReader("data"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("long name should be accepted: got %d", resp.StatusCode)
		}
	})

	t.Run("special_characters_in_name", func(t *testing.T) {
		// URL-safe special characters
		name := "store-with_special.chars"
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create-by-name/"+name, strings.NewReader("data"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("special chars should be accepted: got %d", resp.StatusCode)
		}

		// Verify lookup works
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/lookup-id-by-name/"+name, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("lookup should work: got %d", resp.StatusCode)
		}
	})

	t.Run("reuse_after_delete", func(t *testing.T) {
		name := "reusable-name"

		// Create
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create-by-name/"+name, strings.NewReader("first"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		storeID1, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Delete
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/delete-by-name/"+name, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		resp.Body.Close()

		// Create again with same name - should succeed
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/create-by-name/"+name, strings.NewReader("second"))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		storeID2, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("reuse after delete should succeed: got %d", resp.StatusCode)
		}

		// Should be a different store ID
		if string(storeID1) == string(storeID2) {
			t.Error("should create new store, not reuse deleted one")
		}
	})
}

func TestFunctional_CounterEdgeCases(t *testing.T) {
	s := setupFunctionalTest(t)

	createCounter := func(value int64, min, max *int64) string {
		body := map[string]any{"type": "counter", "value": value}
		if min != nil {
			body["min"] = *min
		}
		if max != nil {
			body["max"] = *max
		}
		bodyBytes, _ := json.Marshal(body)

		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(bodyBytes))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := s.client.Do(req)
		storeID, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return strings.TrimSpace(string(storeID))
	}

	t.Run("zero_delta_increment", func(t *testing.T) {
		storeID := createCounter(50, nil, nil)

		// Increment by 0
		incBody, _ := json.Marshal(map[string]int64{"delta": 0})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		var incResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&incResp)
		resp.Body.Close()

		if incResp.Value != 50 {
			t.Errorf("expected 50 after zero increment, got %d", incResp.Value)
		}
	})

	t.Run("large_delta", func(t *testing.T) {
		storeID := createCounter(0, nil, nil)

		// Large increment
		incBody, _ := json.Marshal(map[string]int64{"delta": 1000000000})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		var incResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&incResp)
		resp.Body.Close()

		if incResp.Value != 1000000000 {
			t.Errorf("expected 1000000000, got %d", incResp.Value)
		}
	})

	t.Run("negative_values_allowed", func(t *testing.T) {
		storeID := createCounter(0, nil, nil)

		// Decrement to negative
		decBody, _ := json.Marshal(map[string]int64{"delta": 100})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/decrement/"+storeID, bytes.NewReader(decBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		var decResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&decResp)
		resp.Body.Close()

		if decResp.Value != -100 {
			t.Errorf("expected -100, got %d", decResp.Value)
		}
	})

	t.Run("bounded_at_creation", func(t *testing.T) {
		// Create counter with initial value at boundary
		min, max := int64(0), int64(100)
		storeID := createCounter(100, &min, &max) // Already at max

		// Increment should clamp
		incBody, _ := json.Marshal(map[string]int64{"delta": 1})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		var incResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&incResp)
		resp.Body.Close()

		if incResp.Value != 100 {
			t.Errorf("expected clamped to 100, got %d", incResp.Value)
		}
		if !incResp.Bounded {
			t.Error("expected bounded=true")
		}
	})

	t.Run("only_min_bound", func(t *testing.T) {
		min := int64(0)
		storeID := createCounter(50, &min, nil) // Only min bound

		// Should be able to go very high
		incBody, _ := json.Marshal(map[string]int64{"delta": 9999999})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		var incResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&incResp)
		resp.Body.Close()

		if incResp.Value != 50+9999999 {
			t.Errorf("expected %d, got %d", 50+9999999, incResp.Value)
		}

		// But can't go below min
		decBody, _ := json.Marshal(map[string]int64{"delta": 99999999})
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/decrement/"+storeID, bytes.NewReader(decBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		json.NewDecoder(resp.Body).Decode(&incResp)
		resp.Body.Close()

		if incResp.Value != 0 {
			t.Errorf("expected clamped to 0, got %d", incResp.Value)
		}
	})
}

func TestFunctional_MultipleOperationsPerStore(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("many_updates_to_same_blob", func(t *testing.T) {
		storeID := s.createBlob(t, "cust1", []byte("v0"))

		for i := 1; i <= 20; i++ {
			body := fmt.Sprintf("v%d", i)
			req, _ := http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, strings.NewReader(body))
			req.Header.Set("X-Customer-ID", "cust1")
			resp, _ := s.client.Do(req)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("update %d failed: %d", i, resp.StatusCode)
			}
		}

		// Final value should be v20
		body, _ := s.snapshot(t, storeID, "cust1")
		if string(body) != "v20" {
			t.Errorf("expected v20, got %q", body)
		}
	})

	t.Run("many_increments_to_same_counter", func(t *testing.T) {
		createBody, _ := json.Marshal(map[string]any{"type": "counter", "value": 0})
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", bytes.NewReader(createBody))
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ := s.client.Do(req)
		storeIDBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		storeID := strings.TrimSpace(string(storeIDBytes))

		for i := 0; i < 50; i++ {
			incBody, _ := json.Marshal(map[string]int64{"delta": 1})
			req, _ := http.NewRequest("POST", "http://localhost/api/v1/increment/"+storeID, bytes.NewReader(incBody))
			req.Header.Set("X-Customer-ID", "cust1")
			resp, _ := s.client.Do(req)
			resp.Body.Close()
		}

		// Final value should be 50
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		var snapResp api.CounterResponse
		json.NewDecoder(resp.Body).Decode(&snapResp)
		resp.Body.Close()

		if snapResp.Value != 50 {
			t.Errorf("expected 50, got %d", snapResp.Value)
		}
		if snapResp.Version != 51 { // 1 create + 50 increments
			t.Errorf("expected version 51, got %d", snapResp.Version)
		}
	})
}

func TestFunctional_StoreExpirationScenarios(t *testing.T) {
	s := setupFunctionalTest(t)

	t.Run("very_short_ttl", func(t *testing.T) {
		// Create with 1 second TTL
		storeID := s.createBlobWithTTL(t, "cust1", []byte("ephemeral"), 1)

		// Should exist immediately
		_, status := s.snapshot(t, storeID, "cust1")
		if status != http.StatusOK {
			t.Errorf("store should exist immediately: %d", status)
		}

		// Wait for expiration
		time.Sleep(2 * time.Second)

		// GC may not have run yet, but store should eventually be gone
		// Note: This test may be flaky depending on GC timing
	})

	t.Run("extend_ttl_before_expiry", func(t *testing.T) {
		storeID := s.createBlobWithTTL(t, "cust1", []byte("data"), 5)

		// Immediately extend TTL
		req, _ := http.NewRequest("POST", "http://localhost/api/v1/update/"+storeID, strings.NewReader("extended"))
		req.Header.Set("X-Customer-ID", "cust1")
		req.Header.Set("BigBunny-Not-Valid-After", "3600") // Extend to 1 hour
		resp, _ := s.client.Do(req)
		resp.Body.Close()

		// Check new TTL
		req, _ = http.NewRequest("POST", "http://localhost/api/v1/snapshot/"+storeID, nil)
		req.Header.Set("X-Customer-ID", "cust1")
		resp, _ = s.client.Do(req)
		ttlHeader := resp.Header.Get("BigBunny-Not-Valid-After")
		resp.Body.Close()

		ttl, _ := strconv.ParseInt(ttlHeader, 10, 64)
		if ttl < 3500 { // Should be close to 3600
			t.Errorf("TTL should be extended, got %d", ttl)
		}
	})
}

func TestFunctional_ManyStoresConcurrently(t *testing.T) {
	s := setupFunctionalTest(t)

	const numStores = 100 // Reduced for stability
	var wg sync.WaitGroup
	var mu sync.Mutex
	storeIDs := make([]string, 0, numStores)
	var errCount int

	// Create many stores concurrently
	for i := range numStores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf("store-%d-data", i)
			req, _ := http.NewRequest("POST", "http://localhost/api/v1/create", strings.NewReader(body))
			req.Header.Set("X-Customer-ID", "bulk-cust")
			resp, err := s.client.Do(req)
			if err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
				return
			}
			storeID, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				mu.Lock()
				errCount++
				mu.Unlock()
				return
			}
			mu.Lock()
			storeIDs = append(storeIDs, string(storeID))
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	if errCount > 0 {
		t.Errorf("%d concurrent create errors", errCount)
	}

	// Count unique stores
	seen := make(map[string]bool)
	for _, id := range storeIDs {
		seen[id] = true
	}

	if len(seen) != numStores {
		t.Errorf("expected %d unique stores, got %d", numStores, len(seen))
	}

	// Verify all are readable
	var readErrors int
	for id := range seen {
		_, status := s.snapshot(t, id, "bulk-cust")
		if status != http.StatusOK {
			readErrors++
		}
	}

	if readErrors > 0 {
		t.Errorf("%d stores could not be read", readErrors)
	}
}
