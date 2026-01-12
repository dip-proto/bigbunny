package test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func setupCounterTestServer(rateLimiter *ratelimit.Limiter) (*api.Server, *store.Manager, *http.ServeMux) {
	storeMgr := store.NewManager()
	hosts := []*routing.Host{
		{ID: "test-host", Address: "localhost:9999", Healthy: true},
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	replicaCfg := replica.DefaultConfig("test-host", "test-site")
	replicaMgr := replica.NewManager(replicaCfg, storeMgr, hasher)
	replicaMgr.SetRole(replica.RolePrimary)

	// Initialize registry for named stores
	registryMgr := registry.NewManager()
	replicaMgr.SetRegistry(registryMgr)

	replicaMgr.Start()

	cipher := auth.NewCipher(auth.DevKeySet())
	cfg := &api.Config{
		Site:          "test-site",
		HostID:        "test-host",
		DefaultTTL:    time.Hour,
		MaxBodySize:   2 * 1024,
		ModifyTimeout: 500 * time.Millisecond,
		Cipher:        cipher,
	}
	srv := api.NewServer(cfg, storeMgr, replicaMgr, hasher, rateLimiter)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	return srv, storeMgr, mux
}

func TestAPICounterCreate(t *testing.T) {
	_, storeMgr, mux := setupCounterTestServer(nil)

	// Create counter with bounds
	min := int64(0)
	max := int64(100)
	reqBody := map[string]interface{}{
		"type":  "counter",
		"value": 50,
		"min":   min,
		"max":   max,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/create", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Customer-ID", "cust1")
	req.Header.Set("BigBunny-Not-Valid-After", "3600")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	storeID := w.Body.String()
	if storeID == "" {
		t.Fatal("expected store ID in response")
	}

	// Verify counter was created with correct data
	counterData, version, err := storeMgr.GetCounter(storeID, "cust1")
	if err != nil {
		t.Fatalf("failed to get counter: %v", err)
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
}

func TestAPICounterIncrement(t *testing.T) {
	_, _, mux := setupCounterTestServer(nil)

	// Create counter via API
	createBody := map[string]interface{}{"type": "counter", "value": 10}
	createBytes, _ := json.Marshal(createBody)
	createReq := httptest.NewRequest("POST", "/api/v1/create", bytes.NewReader(createBytes))
	createReq.Header.Set("X-Customer-ID", "cust1")
	createW := httptest.NewRecorder()
	mux.ServeHTTP(createW, createReq)
	storeID := createW.Body.String()

	// Increment by 5
	reqBody := map[string]interface{}{"delta": 5}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/increment/"+storeID, bytes.NewReader(bodyBytes))
	req.Header.Set("X-Customer-ID", "cust1")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp api.CounterResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Value != 15 {
		t.Errorf("expected value 15, got %d", resp.Value)
	}
	if resp.Version != 2 {
		t.Errorf("expected version 2, got %d", resp.Version)
	}
	if resp.Bounded {
		t.Error("expected bounded=false")
	}
}

func TestAPICounterBoundsEnforcement(t *testing.T) {
	_, _, mux := setupCounterTestServer(nil)

	// Create counter with bounds [0, 100] via API
	createBody := map[string]interface{}{"type": "counter", "value": 90, "min": 0, "max": 100}
	createBytes, _ := json.Marshal(createBody)
	createReq := httptest.NewRequest("POST", "/api/v1/create", bytes.NewReader(createBytes))
	createReq.Header.Set("X-Customer-ID", "cust1")
	createW := httptest.NewRecorder()
	mux.ServeHTTP(createW, createReq)
	storeID := createW.Body.String()

	// Increment beyond max
	reqBody := map[string]interface{}{"delta": 20}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/increment/"+storeID, bytes.NewReader(bodyBytes))
	req.Header.Set("X-Customer-ID", "cust1")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var resp api.CounterResponse
	json.NewDecoder(w.Body).Decode(&resp)

	if resp.Value != 100 {
		t.Errorf("expected value clamped to 100, got %d", resp.Value)
	}
	if !resp.Bounded {
		t.Error("expected bounded=true when hitting max")
	}
}

func TestAPICounterSnapshot(t *testing.T) {
	_, _, mux := setupCounterTestServer(nil)

	// Create counter via API
	createBody := map[string]interface{}{"type": "counter", "value": 42, "min": 0, "max": 100}
	createBytes, _ := json.Marshal(createBody)
	createReq := httptest.NewRequest("POST", "/api/v1/create", bytes.NewReader(createBytes))
	createReq.Header.Set("X-Customer-ID", "cust1")
	createW := httptest.NewRecorder()
	mux.ServeHTTP(createW, createReq)
	storeID := createW.Body.String()

	// Get snapshot
	req := httptest.NewRequest("POST", "/api/v1/snapshot/"+storeID, nil)
	req.Header.Set("X-Customer-ID", "cust1")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", contentType)
	}

	var resp api.CounterResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Value != 42 {
		t.Errorf("expected value 42, got %d", resp.Value)
	}
	if resp.Min == nil || *resp.Min != 0 {
		t.Errorf("expected min=0, got %v", resp.Min)
	}
	if resp.Max == nil || *resp.Max != 100 {
		t.Errorf("expected max=100, got %v", resp.Max)
	}
}

func TestAPICounterTypeMismatch(t *testing.T) {
	_, _, mux := setupCounterTestServer(nil)

	// Create blob store via API (no type specified = blob)
	createReq := httptest.NewRequest("POST", "/api/v1/create", bytes.NewReader([]byte("not a counter")))
	createReq.Header.Set("X-Customer-ID", "cust1")
	createW := httptest.NewRecorder()
	mux.ServeHTTP(createW, createReq)
	storeID := createW.Body.String()

	// Try to increment blob store
	reqBody := map[string]interface{}{"delta": 5}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/increment/"+storeID, bytes.NewReader(bodyBytes))
	req.Header.Set("X-Customer-ID", "cust1")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}

	errorCode := w.Header().Get("BigBunny-Error-Code")
	if errorCode != "TypeMismatch" {
		t.Errorf("expected error code TypeMismatch, got %s", errorCode)
	}
}

func TestAPICounterRateLimiting(t *testing.T) {
	// Create rate limiter: 2 req/s, burst of 4 (1 create + 3 increments)
	rateLimiter := ratelimit.NewLimiter(2, 4)
	_, _, mux := setupCounterTestServer(rateLimiter)

	// Create counter via API (counts as 1 request)
	createBody := map[string]interface{}{"type": "counter", "value": 0}
	createBytes, _ := json.Marshal(createBody)
	createReq := httptest.NewRequest("POST", "/api/v1/create", bytes.NewReader(createBytes))
	createReq.Header.Set("X-Customer-ID", "cust1")
	createW := httptest.NewRecorder()
	mux.ServeHTTP(createW, createReq)
	storeID := createW.Body.String()

	// Make 3 more requests (should all succeed - within burst of 4)
	for i := 0; i < 3; i++ {
		reqBody := map[string]interface{}{"delta": 1}
		bodyBytes, _ := json.Marshal(reqBody)
		req := httptest.NewRequest("POST", "/api/v1/increment/"+storeID, bytes.NewReader(bodyBytes))
		req.Header.Set("X-Customer-ID", "cust1")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("request %d failed: status %d", i+1, w.Code)
		}
	}

	// 5th request (4 + 1) should be rate limited
	reqBody := map[string]interface{}{"delta": 1}
	bodyBytes, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/api/v1/increment/"+storeID, bytes.NewReader(bodyBytes))
	req.Header.Set("X-Customer-ID", "cust1")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusTooManyRequests {
		t.Errorf("expected status 429 (rate limited), got %d", w.Code)
	}

	retryAfter := w.Header().Get("Retry-After")
	if retryAfter == "" {
		t.Error("expected Retry-After header")
	}
}

func TestAPINamedCounterCreate(t *testing.T) {
	_, _, mux := setupCounterTestServer(nil)

	// Create named counter with bounds via API
	reqBody := map[string]interface{}{
		"type":  "counter",
		"value": 50,
		"min":   0,
		"max":   100,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/create-by-name/test-counter", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Customer-ID", "cust1")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	storeID := w.Body.String()
	if storeID == "" {
		t.Fatal("expected store ID in response")
	}

	// Verify the counter was created by trying to increment it
	incReq := map[string]interface{}{"delta": 5}
	incBytes, _ := json.Marshal(incReq)
	incReqHTTP := httptest.NewRequest("POST", "/api/v1/increment/"+storeID, bytes.NewReader(incBytes))
	incReqHTTP.Header.Set("X-Customer-ID", "cust1")
	incReqHTTP.Header.Set("Content-Type", "application/json")
	wInc := httptest.NewRecorder()

	mux.ServeHTTP(wInc, incReqHTTP)

	if wInc.Code != http.StatusOK {
		t.Fatalf("increment failed: status %d: %s", wInc.Code, wInc.Body.String())
	}

	var resp api.CounterResponse
	if err := json.NewDecoder(wInc.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Value != 55 {
		t.Errorf("expected value 55, got %d", resp.Value)
	}
	if resp.Min == nil || *resp.Min != 0 {
		t.Errorf("expected min=0, got %v", resp.Min)
	}
	if resp.Max == nil || *resp.Max != 100 {
		t.Errorf("expected max=100, got %v", resp.Max)
	}
}

func TestAPINamedCounterDuplicate(t *testing.T) {
	_, _, mux := setupCounterTestServer(nil)

	// Create first named counter
	reqBody := map[string]interface{}{
		"type":  "counter",
		"value": 10,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/create-by-name/dup-counter", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Customer-ID", "cust1")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("first create failed: status %d: %s", w.Code, w.Body.String())
	}

	// Try to create again with same name - should fail
	req2 := httptest.NewRequest("POST", "/api/v1/create-by-name/dup-counter", bytes.NewReader(bodyBytes))
	req2.Header.Set("X-Customer-ID", "cust1")
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()

	mux.ServeHTTP(w2, req2)

	if w2.Code != http.StatusConflict {
		t.Errorf("expected status 409 Conflict, got %d", w2.Code)
	}
}

func TestAPINamedCounterReuseIfExists(t *testing.T) {
	_, _, mux := setupCounterTestServer(nil)

	// Create first named counter
	reqBody := map[string]interface{}{
		"type":  "counter",
		"value": 10,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/create-by-name/reuse-counter", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Customer-ID", "cust1")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("first create failed: status %d: %s", w.Code, w.Body.String())
	}

	storeID1 := w.Body.String()

	// Try to create again with same name but with reuse flag - should return same store ID
	req2 := httptest.NewRequest("POST", "/api/v1/create-by-name/reuse-counter", bytes.NewReader(bodyBytes))
	req2.Header.Set("X-Customer-ID", "cust1")
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("BigBunny-Reuse-If-Exists", "true")
	w2 := httptest.NewRecorder()

	mux.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("reuse create failed: status %d: %s", w2.Code, w2.Body.String())
	}

	storeID2 := w2.Body.String()

	if storeID1 != storeID2 {
		t.Errorf("expected same store ID, got %s and %s", storeID1, storeID2)
	}
}
