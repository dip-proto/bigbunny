package api

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dip-proto/bigbunny/internal/auth"
	"github.com/dip-proto/bigbunny/internal/ratelimit"
	"github.com/dip-proto/bigbunny/internal/registry"
	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
)

type Server struct {
	store            *store.Manager
	replica          *replica.Manager
	hasher           *routing.RendezvousHasher
	config           *Config
	cipher           auth.StoreIDCipher
	forwardingClient *http.Client       // Shared HTTP client for request forwarding (with connection pooling)
	rateLimiter      *ratelimit.Limiter // Per-customer rate limiter (nil if rate limiting disabled)
}

type Config struct {
	Site          string
	HostID        string
	DefaultTTL    time.Duration
	MaxBodySize   int64
	ModifyTimeout time.Duration
	Cipher        auth.StoreIDCipher
	InternalToken string // Shared secret for internal endpoints (forwarding)
}

// CreateRequest is the JSON request body for creating stores (optional, backwards compatible).
// If body is not valid JSON, it's treated as blob data.
type CreateRequest struct {
	Type  string  `json:"type"`           // "blob" or "counter"
	Value *int64  `json:"value"`          // For counter type: initial value
	Min   *int64  `json:"min,omitempty"`  // For counter type: optional minimum
	Max   *int64  `json:"max,omitempty"`  // For counter type: optional maximum
	Data  *string `json:"data,omitempty"` // For blob type: base64-encoded data (optional)
}

// CounterResponse is returned from counter operations (increment, get).
type CounterResponse struct {
	Value   int64  `json:"value"`
	Version uint64 `json:"version"`
	Bounded bool   `json:"bounded,omitempty"` // True if increment was clamped
	Min     *int64 `json:"min,omitempty"`
	Max     *int64 `json:"max,omitempty"`
}

// IncrementRequest is the JSON request body for increment/decrement operations.
type IncrementRequest struct {
	Delta int64 `json:"delta"`
}

func DefaultConfig() *Config {
	return &Config{
		Site:          "local",
		HostID:        "host1",
		DefaultTTL:    14 * 24 * time.Hour, // 2 weeks
		MaxBodySize:   2 * 1024,            // 2 KiB
		ModifyTimeout: 500 * time.Millisecond,
	}
}

func NewServer(cfg *Config, storeMgr *store.Manager, replicaMgr *replica.Manager, hasher *routing.RendezvousHasher, rateLimiter *ratelimit.Limiter) *Server {
	return &Server{
		store:   storeMgr,
		replica: replicaMgr,
		hasher:  hasher,
		config:  cfg,
		cipher:  cfg.Cipher,
		forwardingClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		rateLimiter: rateLimiter,
	}
}

func (s *Server) Shutdown() {
	s.forwardingClient.CloseIdleConnections()
}

func (s *Server) openStoreID(storeID, customerID string) (*auth.StoreIDComponents, error) {
	return s.cipher.Open(storeID, customerID)
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	// Public API (for components via UDS)
	mux.HandleFunc("POST /api/v1/create", s.handleCreate)
	mux.HandleFunc("POST /api/v1/create-by-name/{name}", s.handleCreateByName)
	mux.HandleFunc("POST /api/v1/delete/{storeID}", s.handleDelete)
	mux.HandleFunc("POST /api/v1/delete-by-name/{name}", s.handleDeleteByName)
	mux.HandleFunc("POST /api/v1/lookup-id-by-name/{name}", s.handleLookupIDByName)
	mux.HandleFunc("POST /api/v1/snapshot/{storeID}", s.handleSnapshot)
	mux.HandleFunc("POST /api/v1/begin-modify/{storeID}", s.handleBeginModify)
	mux.HandleFunc("POST /api/v1/complete-modify/{storeID}", s.handleCompleteModify)
	mux.HandleFunc("POST /api/v1/cancel-modify/{storeID}", s.handleCancelModify)
	mux.HandleFunc("POST /api/v1/update/{storeID}", s.handleUpdate)
	mux.HandleFunc("POST /api/v1/increment/{storeID}", s.handleIncrement)
	mux.HandleFunc("POST /api/v1/decrement/{storeID}", s.handleDecrement)

	// Internal API (for replication)
	mux.HandleFunc("POST /internal/replicate", s.handleReplicate)
	mux.HandleFunc("POST /internal/replicate-registry", s.handleReplicateRegistry)
	mux.HandleFunc("POST /internal/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("POST /internal/snapshot", s.handleInternalSnapshot)
	mux.HandleFunc("POST /internal/registry/snapshot", s.handleRegistrySnapshot)

	// Internal registry API (for named store operations)
	mux.HandleFunc("POST /internal/registry/reserve", s.handleRegistryReserve)
	mux.HandleFunc("POST /internal/registry/commit", s.handleRegistryCommit)
	mux.HandleFunc("POST /internal/registry/abort", s.handleRegistryAbort)
	mux.HandleFunc("GET /internal/registry/lookup", s.handleRegistryLookup)
	mux.HandleFunc("POST /internal/registry/delete", s.handleRegistryDelete)

	// Status (available on both UDS and TCP)
	mux.HandleFunc("GET /status", s.handleStatus)
}

// RegisterOpsRoutes registers operational endpoints that should only be available locally (UDS).
// These endpoints allow force-promotion and lock release without authentication.
func (s *Server) RegisterOpsRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /internal/promote", s.handlePromote)
	mux.HandleFunc("POST /internal/release-lock/{storeID}", s.handleForceReleaseLock)
}

// forwardToLeader forwards the request to the current primary node.
// Preserves all headers and body, adds internal auth and loop prevention headers.
func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request, leaderAddr string) {
	// Build target URL
	targetURL := "http://" + leaderAddr + r.URL.Path
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	// Create forwarded request
	fwdReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, "failed to create forwarding request", http.StatusBadGateway)
		return
	}

	// Copy all request headers
	for key, values := range r.Header {
		for _, value := range values {
			fwdReq.Header.Add(key, value)
		}
	}

	// Add internal headers
	// X-Internal-Token for authentication (uses same token as inter-node replication)
	internalToken := s.getInternalToken()
	if internalToken != "" {
		fwdReq.Header.Set("X-Internal-Token", internalToken)
	}
	// X-BB-No-Forward prevents forwarding loops
	fwdReq.Header.Set("X-BB-No-Forward", "true")

	// Execute forwarded request
	resp, err := s.forwardingClient.Do(fwdReq)
	if err != nil {
		http.Error(w, "forwarding failed: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	// Copy all response headers (preserves BigBunny-Error-Code, etc.)
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Copy response status and body
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("error copying forwarded response body: %v", err)
	}
}

// getInternalToken retrieves the internal token for forwarding requests.
func (s *Server) getInternalToken() string {
	return s.config.InternalToken
}

func (s *Server) requirePrimary(w http.ResponseWriter, r *http.Request) bool {
	// If I'm primary, handle locally
	if s.replica.Role() == replica.RolePrimary {
		return true
	}

	// If request already forwarded, can't forward again (prevent loops)
	// Return error since we're not primary and can't forward
	if r.Header.Get("X-BB-No-Forward") == "true" {
		writeRetryableError(w, ErrCodeLeaderChanged, "not primary (forwarding disabled)", time.Second)
		return false
	}

	// I'm not primary - try to forward
	leaderAddr := s.replica.GetLeaderAddress()
	if leaderAddr == "" {
		// Don't know who leader is yet (shouldn't happen with deterministic fallback)
		writeRetryableError(w, ErrCodeLeaderChanged, "leader unknown", time.Second)
		return false
	}

	// Forward to leader
	// Note: X-BB-No-Forward header prevents infinite loops if we somehow
	// forward to ourselves (would indicate a bug in leader tracking)
	s.forwardToLeader(w, r, leaderAddr)
	return false // Signal: don't handle locally, already forwarded
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	now := time.Now()

	if !s.requirePrimary(w, r) {
		return
	}

	customerID, ok := s.requireCustomerID(w, r)
	if !ok {
		return
	}

	body, ok := s.readLimitedBody(w, r)
	if !ok {
		return
	}

	ttl := s.parseTTLHeader(r)
	expiresAt := now.Add(ttl)

	// Try to parse as JSON CreateRequest
	var req CreateRequest
	isCounter := false
	if err := json.Unmarshal(body, &req); err == nil && req.Type != "" {
		if req.Type == "counter" {
			isCounter = true
			if req.Value == nil {
				http.Error(w, "missing value for counter", http.StatusBadRequest)
				return
			}
		}
	}

	shardID, err := routing.GenerateShardID()
	if err != nil {
		http.Error(w, "failed to generate shard ID", http.StatusInternalServerError)
		return
	}

	storeID, err := routing.GenerateEncryptedStoreID(s.cipher, s.config.Site, shardID, customerID)
	if err != nil {
		http.Error(w, "failed to generate store ID", http.StatusInternalServerError)
		return
	}

	if isCounter {
		// Create counter store
		_, err := s.store.CreateCounter(storeID, shardID, customerID, *req.Value, req.Min, req.Max, expiresAt, s.replica.LeaderEpoch())
		if err != nil {
			writeHTTPError(w, err, "failed to create counter")
			return
		}

		// Replicate counter creation
		counterData := store.CounterData{Value: *req.Value, Min: req.Min, Max: req.Max}
		counterBody, _ := json.Marshal(counterData)
		s.replica.QueueReplication(&replica.ReplicationMessage{
			Type:       replica.MsgCreateStore,
			StoreID:    storeID,
			ShardID:    shardID,
			CustomerID: customerID,
			DataType:   uint8(store.DataTypeCounter),
			Body:       counterBody,
			ExpiresAt:  expiresAt,
			Version:    1,
		})
	} else {
		// Create blob store (backwards compatible)
		st := &store.Store{
			ID:          storeID,
			ShardID:     shardID,
			CustomerID:  customerID,
			DataType:    store.DataTypeBlob,
			Body:        body,
			ExpiresAt:   expiresAt,
			Version:     1,
			LeaderEpoch: s.replica.LeaderEpoch(),
			Role:        store.RolePrimary,
		}

		if err := s.store.Create(st); err != nil {
			writeHTTPError(w, err, "failed to create store")
			return
		}

		s.replica.QueueReplication(&replica.ReplicationMessage{
			Type:       replica.MsgCreateStore,
			StoreID:    storeID,
			ShardID:    shardID,
			CustomerID: customerID,
			DataType:   uint8(st.DataType),
			Body:       body,
			ExpiresAt:  st.ExpiresAt,
			Version:    st.Version,
		})
	}

	w.Header().Set("Content-Type", "text/plain")
	s.checkDegradedWrite(w)
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(storeID)); err != nil {
		log.Printf("error writing response body: %v", err)
	}
}

func (s *Server) handleCreateByName(w http.ResponseWriter, r *http.Request) {
	now := time.Now()

	if !s.requirePrimary(w, r) {
		return
	}

	customerID, ok := s.requireCustomerID(w, r)
	if !ok {
		return
	}

	name := strings.TrimSpace(r.PathValue("name"))
	if name == "" {
		http.Error(w, "missing name", http.StatusBadRequest)
		return
	}

	reg := s.replica.Registry()
	if reg == nil {
		http.Error(w, "registry not initialized", http.StatusInternalServerError)
		return
	}

	reuseIfExists := parseBoolHeader(r.Header.Get("BigBunny-Reuse-If-Exists"))
	if reuseIfExists {
		existing, err := reg.Lookup(customerID, name)
		if err == nil {
			switch existing.State {
			case registry.StateActive:
				w.Header().Set("Content-Type", "text/plain")
				w.WriteHeader(http.StatusOK)
				if _, err := w.Write([]byte(existing.StoreID)); err != nil {
					log.Printf("error writing response body: %v", err)
				}
				return
			case registry.StateCreating:
				http.Error(w, "name reservation in progress", http.StatusConflict)
				return
			case registry.StateDeleting:
				http.Error(w, "name deletion in progress", http.StatusConflict)
				return
			}
		} else if err == registry.ErrUnauthorized {
			http.Error(w, "unauthorized", http.StatusForbidden)
			return
		} else if err != registry.ErrEntryNotFound {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	entry, err := reg.Reserve(customerID, name, s.replica.LeaderEpoch())
	if err != nil {
		switch err {
		case registry.ErrNameExists:
			http.Error(w, "name already exists", http.StatusConflict)
		case registry.ErrNameReserved:
			http.Error(w, "name reservation in progress", http.StatusConflict)
		case registry.ErrNameDeleting:
			http.Error(w, "name deletion in progress", http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	s.replica.QueueRegistryReplication(&replica.RegistryReplicationMessage{
		Type:          replica.MsgRegistryReserve,
		CustomerID:    entry.CustomerID,
		Name:          entry.Name,
		State:         int(entry.State),
		ReservationID: entry.ReservationID,
		Version:       entry.Version,
	})

	body, err := io.ReadAll(io.LimitReader(r.Body, s.config.MaxBodySize+1))
	if err != nil {
		s.abortRegistryReservation(entry)
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > s.config.MaxBodySize {
		s.abortRegistryReservation(entry)
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}

	ttl := s.parseTTLHeader(r)

	shardID, err := routing.GenerateShardID()
	if err != nil {
		s.abortRegistryReservation(entry)
		http.Error(w, "failed to generate shard ID", http.StatusInternalServerError)
		return
	}

	storeID, err := routing.GenerateEncryptedStoreID(s.cipher, s.config.Site, shardID, customerID)
	if err != nil {
		s.abortRegistryReservation(entry)
		http.Error(w, "failed to generate store ID", http.StatusInternalServerError)
		return
	}

	// Detect if this is a counter or blob based on content type and body
	var st *store.Store
	contentType := r.Header.Get("Content-Type")
	if contentType == "application/json" && len(body) > 0 {
		var createReq CreateRequest
		if err := json.Unmarshal(body, &createReq); err == nil && createReq.Type == "counter" {
			// Get initial value (default to 0 if not specified)
			var initialValue int64
			if createReq.Value != nil {
				initialValue = *createReq.Value
			}

			// Create counter store
			counterStore, err := s.store.CreateCounter(
				storeID,
				shardID,
				customerID,
				initialValue,
				createReq.Min,
				createReq.Max,
				now.Add(ttl),
				s.replica.LeaderEpoch(),
			)
			if err != nil {
				s.abortRegistryReservation(entry)
				switch err {
				case store.ErrInvalidBounds:
					writeErrorWithCode(w, ErrCodeInvalidBounds, "invalid bounds", http.StatusBadRequest)
				case store.ErrValueOutOfBounds:
					writeErrorWithCode(w, ErrCodeValueOutOfBounds, "value out of bounds", http.StatusBadRequest)
				case store.ErrCapacityExceeded:
					writeErrorWithCode(w, ErrCodeCapacityExceeded, "capacity exceeded", http.StatusInsufficientStorage)
				default:
					http.Error(w, "failed to create counter", http.StatusInternalServerError)
				}
				return
			}
			counterStore.PendingName = name
			st = counterStore
		}
	}

	// If not a counter, create as blob
	if st == nil {
		st = &store.Store{
			ID:          storeID,
			ShardID:     shardID,
			CustomerID:  customerID,
			DataType:    store.DataTypeBlob,
			Body:        body,
			ExpiresAt:   now.Add(ttl),
			Version:     1,
			LeaderEpoch: s.replica.LeaderEpoch(),
			Role:        store.RolePrimary,
			PendingName: name,
		}

		if err := s.store.Create(st); err != nil {
			s.abortRegistryReservation(entry)
			switch err {
			case store.ErrStoreExists:
				http.Error(w, "store already exists", http.StatusConflict)
			case store.ErrCapacityExceeded:
				writeErrorWithCode(w, ErrCodeCapacityExceeded, "capacity exceeded", http.StatusInsufficientStorage)
			default:
				http.Error(w, "failed to create store", http.StatusInternalServerError)
			}
			return
		}
	}

	s.replica.QueueReplication(&replica.ReplicationMessage{
		Type:        replica.MsgCreateStore,
		StoreID:     storeID,
		ShardID:     shardID,
		CustomerID:  customerID,
		DataType:    uint8(st.DataType),
		Body:        body,
		ExpiresAt:   st.ExpiresAt,
		Version:     st.Version,
		PendingName: name,
	})

	committed, err := reg.Commit(customerID, name, entry.ReservationID, storeID, st.ExpiresAt, s.replica.LeaderEpoch())
	if err != nil {
		_ = s.store.ForceDelete(storeID)
		s.replica.AddTombstone(storeID)
		s.replica.QueueReplication(&replica.ReplicationMessage{
			Type:       replica.MsgDeleteStore,
			StoreID:    storeID,
			ShardID:    shardID,
			CustomerID: customerID,
			Tombstone:  true,
		})

		s.abortRegistryReservation(entry)

		switch err {
		case registry.ErrEntryNotFound:
			http.Error(w, "reservation expired", http.StatusConflict)
		case registry.ErrUnauthorized:
			http.Error(w, "unauthorized", http.StatusForbidden)
		case registry.ErrInvalidState:
			http.Error(w, "invalid state for commit", http.StatusConflict)
		case registry.ErrReservationMismatch:
			http.Error(w, "reservation ID mismatch", http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	s.replica.QueueRegistryReplication(&replica.RegistryReplicationMessage{
		Type:          replica.MsgRegistryCommit,
		CustomerID:    committed.CustomerID,
		Name:          committed.Name,
		StoreID:       committed.StoreID,
		State:         int(committed.State),
		ExpiresAt:     committed.ExpiresAt,
		ReservationID: committed.ReservationID,
		Version:       committed.Version,
	})

	_ = s.store.ClearPendingName(storeID, customerID)

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(storeID)); err != nil {
		log.Printf("error writing response body: %v", err)
	}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if !s.requirePrimary(w, r) {
		return
	}

	storeID := r.PathValue("storeID")
	customerID, ok := s.requireCustomerID(w, r)
	if !ok {
		return
	}

	shardID, ok := s.parseStoreIDWithShard(w, storeID, customerID)
	if !ok {
		return
	}

	if err := s.store.Delete(storeID, customerID); err != nil {
		writeHTTPError(w, err, "failed to delete store")
		return
	}

	s.replica.AddTombstone(storeID)
	s.replica.QueueReplication(&replica.ReplicationMessage{
		Type:       replica.MsgDeleteStore,
		StoreID:    storeID,
		ShardID:    shardID,
		CustomerID: customerID,
		Tombstone:  true,
	})

	s.checkDegradedWrite(w)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteByName(w http.ResponseWriter, r *http.Request) {
	if !s.requirePrimary(w, r) {
		return
	}

	customerID, ok := s.requireCustomerID(w, r)
	if !ok {
		return
	}

	name := strings.TrimSpace(r.PathValue("name"))
	if name == "" {
		http.Error(w, "missing name", http.StatusBadRequest)
		return
	}

	reg, ok := s.getRegistry(w)
	if !ok {
		return
	}

	entry, err := reg.Lookup(customerID, name)
	if err != nil {
		writeHTTPError(w, err, "")
		return
	}

	if entry.State == registry.StateCreating {
		http.Error(w, "name reservation in progress", http.StatusConflict)
		return
	}

	deleting, err := reg.MarkDeleting(customerID, name, s.replica.LeaderEpoch())
	if err != nil {
		writeHTTPError(w, err, "")
		return
	}

	s.replica.QueueRegistryReplication(&replica.RegistryReplicationMessage{
		Type:          replica.MsgRegistryCommit,
		CustomerID:    deleting.CustomerID,
		Name:          deleting.Name,
		StoreID:       deleting.StoreID,
		State:         int(deleting.State),
		ExpiresAt:     deleting.ExpiresAt,
		ReservationID: deleting.ReservationID,
		Version:       deleting.Version,
	})

	storeErr := error(nil)
	if deleting.StoreID != "" {
		storeErr = s.store.Delete(deleting.StoreID, customerID)
	}

	if storeErr != nil && storeErr != store.ErrStoreNotFound {
		if err := reg.RevertToActive(customerID, name); err == nil {
			if active, err := reg.Lookup(customerID, name); err == nil {
				s.replica.QueueRegistryReplication(&replica.RegistryReplicationMessage{
					Type:          replica.MsgRegistryCommit,
					CustomerID:    active.CustomerID,
					Name:          active.Name,
					StoreID:       active.StoreID,
					State:         int(active.State),
					ExpiresAt:     active.ExpiresAt,
					ReservationID: active.ReservationID,
					Version:       active.Version,
				})
			}
		}
		writeHTTPError(w, storeErr, "failed to delete store")
		return
	}

	if deleting.StoreID != "" {
		var shardID string
		if components, err := s.openStoreID(deleting.StoreID, customerID); err == nil && components != nil {
			shardID = components.ShardID
		}
		s.replica.AddTombstone(deleting.StoreID)
		s.replica.QueueReplication(&replica.ReplicationMessage{
			Type:       replica.MsgDeleteStore,
			StoreID:    deleting.StoreID,
			ShardID:    shardID,
			CustomerID: customerID,
			Tombstone:  true,
		})
	}

	if err := reg.Delete(customerID, name); err != nil {
		writeHTTPError(w, err, "")
		return
	}

	s.replica.QueueRegistryReplication(&replica.RegistryReplicationMessage{
		Type:       replica.MsgRegistryDelete,
		CustomerID: customerID,
		Name:       name,
	})

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleLookupIDByName(w http.ResponseWriter, r *http.Request) {
	customerID, ok := s.requireCustomerID(w, r)
	if !ok {
		return
	}

	name := strings.TrimSpace(r.PathValue("name"))
	if name == "" {
		http.Error(w, "missing name", http.StatusBadRequest)
		return
	}

	reg, ok := s.getRegistry(w)
	if !ok {
		return
	}

	entry, err := reg.Lookup(customerID, name)
	if err != nil {
		writeHTTPError(w, err, "")
		return
	}

	switch entry.State {
	case registry.StateActive:
		// Lazy cleanup: check if the referenced store still exists and is not expired
		st, err := s.store.Get(entry.StoreID, customerID)
		if err == store.ErrStoreNotFound || err == store.ErrStoreExpired {
			// Store is gone or expired - clean up the registry entry
			if delErr := reg.Delete(customerID, name); delErr == nil {
				s.replica.QueueRegistryReplication(&replica.RegistryReplicationMessage{
					Type:       replica.MsgRegistryDelete,
					CustomerID: customerID,
					Name:       name,
				})
			}
			writeErrorWithCode(w, ErrCodeNotFound, "name not found", http.StatusNotFound)
			return
		}
		if err != nil {
			// Some other error (e.g., unauthorized)
			switch err {
			case store.ErrUnauthorized:
				writeErrorWithCode(w, ErrCodeUnauthorized, "unauthorized", http.StatusForbidden)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		_ = st // store exists and is valid
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(entry.StoreID)); err != nil {
			log.Printf("error writing response body: %v", err)
		}
	case registry.StateCreating:
		writeRetryableError(w, ErrCodeNameCreating, "name reservation in progress", time.Second)
	case registry.StateDeleting:
		writeRetryableError(w, ErrCodeNameCreating, "name deletion in progress", time.Second)
	default:
		http.Error(w, "unknown registry state", http.StatusInternalServerError)
	}
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	storeID := r.PathValue("storeID")
	customerID, ok := s.requireCustomerID(w, r)
	if !ok {
		return
	}

	if _, ok := s.parseStoreIDWithShard(w, storeID, customerID); !ok {
		return
	}

	st, err := s.store.Get(storeID, customerID)
	if err != nil {
		writeHTTPError(w, err, "failed to get store")
		return
	}

	ttlRemaining := max(time.Until(st.ExpiresAt).Seconds(), 0)
	w.Header().Set("BigBunny-Not-Valid-After", strconv.FormatInt(int64(ttlRemaining), 10))

	// Handle counter stores with JSON response
	if st.DataType == store.DataTypeCounter {
		counterData, version, err := s.store.GetCounter(storeID, customerID)
		if err != nil {
			writeHTTPError(w, err, "failed to get counter")
			return
		}

		resp := CounterResponse{
			Value:   counterData.Value,
			Version: version,
			Min:     counterData.Min,
			Max:     counterData.Max,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Printf("error writing response body: %v", err)
		}
		return
	}

	// Handle blob stores with raw body
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(st.Body); err != nil {
		log.Printf("error writing response body: %v", err)
	}
}

func (s *Server) handleBeginModify(w http.ResponseWriter, r *http.Request) {
	if !s.requirePrimary(w, r) {
		return
	}

	storeID := r.PathValue("storeID")
	customerID, ok := s.requireCustomerID(w, r)
	if !ok {
		return
	}

	shardID, ok := s.parseStoreIDWithShard(w, storeID, customerID)
	if !ok {
		return
	}

	if unknown, retryAfter := s.replica.IsLockStateUnknown(storeID); unknown {
		s.writeLockStateUnknown(w, retryAfter)
		return
	}

	lockID, err := routing.GenerateLockID()
	if err != nil {
		http.Error(w, "failed to generate lock ID", http.StatusInternalServerError)
		return
	}

	st, err := s.store.AcquireLock(storeID, customerID, lockID, s.config.ModifyTimeout)
	if err != nil {
		writeHTTPError(w, err, "failed to acquire lock")
		return
	}

	s.replica.QueueReplication(&replica.ReplicationMessage{
		Type:        replica.MsgLockAcquired,
		StoreID:     storeID,
		ShardID:     shardID,
		CustomerID:  customerID,
		LockID:      lockID,
		LockTimeout: s.config.ModifyTimeout,
	})

	ttlRemaining := max(time.Until(st.ExpiresAt).Seconds(), 0)

	w.Header().Set("BigBunny-Not-Valid-After", strconv.FormatInt(int64(ttlRemaining), 10))
	w.Header().Set("BigBunny-Lock-ID", lockID)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(st.Body); err != nil {
		log.Printf("error writing response body: %v", err)
	}
}

func (s *Server) handleCompleteModify(w http.ResponseWriter, r *http.Request) {
	now := time.Now()

	if !s.requirePrimary(w, r) {
		return
	}

	storeID := r.PathValue("storeID")
	customerID, ok := s.requireCustomerID(w, r)
	if !ok {
		return
	}

	lockID := r.Header.Get("BigBunny-Lock-ID")
	if lockID == "" {
		http.Error(w, "missing lock ID", http.StatusBadRequest)
		return
	}

	shardID, ok := s.parseStoreIDWithShard(w, storeID, customerID)
	if !ok {
		return
	}

	if unknown, retryAfter := s.replica.IsLockStateUnknown(storeID); unknown {
		s.writeLockStateUnknown(w, retryAfter)
		return
	}

	body, ok := s.readLimitedBody(w, r)
	if !ok {
		return
	}

	newExpiresAt := s.parseExpiresAtHeader(r, now)

	updated, err := s.store.CompleteLock(storeID, customerID, lockID, body, newExpiresAt)
	if err != nil {
		writeHTTPError(w, err, "failed to complete modify")
		return
	}

	s.replica.QueueReplication(&replica.ReplicationMessage{
		Type:       replica.MsgUpdateStore,
		StoreID:    storeID,
		ShardID:    shardID,
		CustomerID: customerID,
		DataType:   uint8(updated.DataType),
		Body:       body,
		ExpiresAt:  updated.ExpiresAt,
		Version:    updated.Version,
	})

	s.checkDegradedWrite(w)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCancelModify(w http.ResponseWriter, r *http.Request) {
	if !s.requirePrimary(w, r) {
		return
	}

	storeID := r.PathValue("storeID")
	customerID, ok := s.requireCustomerID(w, r)
	if !ok {
		return
	}

	lockID := r.Header.Get("BigBunny-Lock-ID")
	if lockID == "" {
		http.Error(w, "missing lock ID", http.StatusBadRequest)
		return
	}

	shardID, ok := s.parseStoreIDWithShard(w, storeID, customerID)
	if !ok {
		return
	}

	if unknown, retryAfter := s.replica.IsLockStateUnknown(storeID); unknown {
		s.writeLockStateUnknown(w, retryAfter)
		return
	}

	if err := s.store.ReleaseLock(storeID, customerID, lockID); err != nil {
		writeHTTPError(w, err, "failed to cancel modify")
		return
	}

	s.replica.QueueReplication(&replica.ReplicationMessage{
		Type:       replica.MsgLockReleased,
		StoreID:    storeID,
		ShardID:    shardID,
		CustomerID: customerID,
		LockID:     lockID,
	})

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	now := time.Now()

	if !s.requirePrimary(w, r) {
		return
	}

	storeID := r.PathValue("storeID")
	customerID, ok := s.requireCustomerID(w, r)
	if !ok {
		return
	}

	shardID, ok := s.parseStoreIDWithShard(w, storeID, customerID)
	if !ok {
		return
	}

	if unknown, retryAfter := s.replica.IsLockStateUnknown(storeID); unknown {
		s.writeLockStateUnknown(w, retryAfter)
		return
	}

	body, ok := s.readLimitedBody(w, r)
	if !ok {
		return
	}

	// Check if this is a counter store and handle accordingly
	st, getErr := s.store.Get(storeID, customerID)
	if getErr != nil {
		writeHTTPError(w, getErr, "failed to get store")
		return
	}

	if st.DataType == store.DataTypeCounter {
		// For counters, expect JSON with {"value": N}
		var req struct {
			Value int64 `json:"value"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "invalid JSON for counter update", http.StatusBadRequest)
			return
		}

		// Parse optional TTL header
		newExpiresAt := s.parseExpiresAtHeader(r, now)

		version, err := s.store.SetCounter(storeID, customerID, req.Value, newExpiresAt)
		if err != nil {
			writeHTTPError(w, err, "failed to set counter")
			return
		}

		// Get updated counter data for replication
		updatedSt, _ := s.store.Get(storeID, customerID)
		s.replica.QueueReplication(&replica.ReplicationMessage{
			Type:       replica.MsgUpdateStore,
			StoreID:    storeID,
			ShardID:    shardID,
			CustomerID: customerID,
			DataType:   uint8(updatedSt.DataType),
			Body:       updatedSt.Body,
			ExpiresAt:  updatedSt.ExpiresAt,
			Version:    version,
		})

		s.checkDegradedWrite(w)
		w.WriteHeader(http.StatusOK)
		return
	}

	// For blob stores, use the lock mechanism
	lockID, err := routing.GenerateLockID()
	if err != nil {
		http.Error(w, "failed to generate lock ID", http.StatusInternalServerError)
		return
	}

	if _, err = s.store.AcquireLock(storeID, customerID, lockID, s.config.ModifyTimeout); err != nil {
		writeHTTPError(w, err, "failed to acquire lock")
		return
	}

	newExpiresAt := s.parseExpiresAtHeader(r, now)

	updated, err := s.store.CompleteLock(storeID, customerID, lockID, body, newExpiresAt)
	if err != nil {
		_ = s.store.ReleaseLock(storeID, customerID, lockID)
		writeHTTPError(w, err, "failed to update store")
		return
	}

	s.replica.QueueReplication(&replica.ReplicationMessage{
		Type:       replica.MsgUpdateStore,
		StoreID:    storeID,
		ShardID:    shardID,
		CustomerID: customerID,
		DataType:   uint8(updated.DataType),
		Body:       body,
		ExpiresAt:  updated.ExpiresAt,
		Version:    updated.Version,
	})

	s.checkDegradedWrite(w)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
	var msg replica.ReplicationMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "invalid message", http.StatusBadRequest)
		return
	}

	// Validate store ID before applying replication
	// This ensures the store ID is properly formatted and bound to the claimed customer
	if msg.StoreID != "" && msg.CustomerID != "" {
		components, err := s.cipher.Open(msg.StoreID, msg.CustomerID)
		if err != nil {
			http.Error(w, "invalid store ID in replication message", http.StatusBadRequest)
			return
		}
		// Verify ShardID matches if present in message
		if msg.ShardID != "" && components.ShardID != msg.ShardID {
			http.Error(w, "shard ID mismatch in replication message", http.StatusBadRequest)
			return
		}
	}

	if err := s.replica.ApplyReplication(&msg); err != nil {
		if err == replica.ErrJoinInProgress {
			writeRetryableError(w, ErrCodeStoreUnavailable, "recovery in progress", time.Second)
			return
		}
		if strings.Contains(err.Error(), "stale epoch") {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var hb replica.HeartbeatMessage
	if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
		http.Error(w, "invalid heartbeat", http.StatusBadRequest)
		return
	}

	ack := s.replica.HandleHeartbeat(&hb)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(ack); err != nil {
		log.Printf("error encoding JSON response: %v", err)
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.replica.GetStatus()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		log.Printf("error encoding JSON response: %v", err)
	}
}

// handlePromote forces this node to become primary.
func (s *Server) handlePromote(w http.ResponseWriter, r *http.Request) {
	s.replica.ForcePromote()
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("promoted")); err != nil {
		log.Printf("error writing response body: %v", err)
	}
}

// handleForceReleaseLock forcibly releases a lock on a store without customer verification.
func (s *Server) handleForceReleaseLock(w http.ResponseWriter, r *http.Request) {
	storeID := r.PathValue("storeID")
	if storeID == "" {
		http.Error(w, "store ID required", http.StatusBadRequest)
		return
	}

	if err := s.store.ForceReleaseLock(storeID); err != nil {
		if err == store.ErrStoreNotFound {
			writeErrorWithCode(w, ErrCodeNotFound, "store not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("lock released")); err != nil {
		log.Printf("error writing response body: %v", err)
	}
}

// handleInternalSnapshot returns a snapshot of stores and tombstones for recovery.
// Only primary can serve snapshots.
func (s *Server) handleInternalSnapshot(w http.ResponseWriter, r *http.Request) {
	if !s.requirePrimary(w, r) {
		return
	}

	stores := s.store.Snapshot()
	tombstones := s.replica.TombstonesSnapshot()

	resp := replica.SnapshotData{
		HostID:      s.config.HostID,
		Stores:      stores,
		Tombstones:  tombstones,
		LeaderEpoch: s.replica.LeaderEpoch(),
		Complete:    true,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("error encoding JSON response: %v", err)
	}
}

// RegistrySnapshotResponse is the response for registry snapshot requests.
type RegistrySnapshotResponse struct {
	Entries     []*registry.Entry `json:"entries"`
	LeaderEpoch uint64            `json:"leader_epoch"`
}

// handleRegistrySnapshot returns a snapshot of registry entries for recovery.
// Only primary can serve snapshots.
func (s *Server) handleRegistrySnapshot(w http.ResponseWriter, r *http.Request) {
	if !s.requirePrimary(w, r) {
		return
	}

	reg := s.replica.Registry()
	if reg == nil {
		http.Error(w, "registry not initialized", http.StatusInternalServerError)
		return
	}

	entries := reg.Snapshot()

	resp := RegistrySnapshotResponse{
		Entries:     entries,
		LeaderEpoch: s.replica.LeaderEpoch(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("error encoding JSON response: %v", err)
	}
}

func (s *Server) extractCustomerID(r *http.Request) string {
	return r.Header.Get("X-Customer-ID")
}

func (s *Server) requireCustomerID(w http.ResponseWriter, r *http.Request) (string, bool) {
	customerID := s.extractCustomerID(r)
	if customerID == "" {
		http.Error(w, "missing customer ID", http.StatusUnauthorized)
		return "", false
	}

	// Check rate limit if rate limiter is configured
	if s.rateLimiter != nil && !s.rateLimiter.Allow(customerID) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return "", false
	}

	return customerID, true
}

func (s *Server) parseStoreIDWithShard(w http.ResponseWriter, storeID, customerID string) (string, bool) {
	components, err := s.openStoreID(storeID, customerID)
	if err != nil {
		http.Error(w, "invalid store ID", http.StatusBadRequest)
		return "", false
	}
	if components != nil {
		return components.ShardID, true
	}
	return "", true
}

func (s *Server) getRegistry(w http.ResponseWriter) (*registry.Manager, bool) {
	reg := s.replica.Registry()
	if reg == nil {
		http.Error(w, "registry not initialized", http.StatusInternalServerError)
		return nil, false
	}
	return reg, true
}

func (s *Server) readLimitedBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, s.config.MaxBodySize+1))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return nil, false
	}
	if int64(len(body)) > s.config.MaxBodySize {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	return body, true
}

func (s *Server) parseTTLHeader(r *http.Request) time.Duration {
	if ttlHeader := r.Header.Get("BigBunny-Not-Valid-After"); ttlHeader != "" {
		if secs, err := strconv.ParseInt(ttlHeader, 10, 64); err == nil {
			return time.Duration(secs) * time.Second
		}
	}
	return s.config.DefaultTTL
}

func (s *Server) parseExpiresAtHeader(r *http.Request, now time.Time) time.Time {
	if ttlHeader := r.Header.Get("BigBunny-Not-Valid-After"); ttlHeader != "" {
		if secs, err := strconv.ParseInt(ttlHeader, 10, 64); err == nil {
			return now.Add(time.Duration(secs) * time.Second)
		}
	}
	return time.Time{}
}

func (s *Server) checkDegradedWrite(w http.ResponseWriter) {
	if !s.replica.IsSecondaryHealthy() {
		setDegradedWriteWarning(w)
	}
}

func (s *Server) abortRegistryReservation(entry *registry.Entry) {
	reg := s.replica.Registry()
	if reg == nil {
		return
	}
	if err := reg.Abort(entry.CustomerID, entry.Name, entry.ReservationID); err != nil {
		return
	}
	s.replica.QueueRegistryReplication(&replica.RegistryReplicationMessage{
		Type:          replica.MsgRegistryAbort,
		CustomerID:    entry.CustomerID,
		Name:          entry.Name,
		ReservationID: entry.ReservationID,
	})
}

func parseBoolHeader(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func (s *Server) writeLockStateUnknown(w http.ResponseWriter, retryAfter time.Duration) {
	w.Header().Set(HeaderLockState, "unknown")
	w.Header().Set(HeaderErrorCode, string(ErrCodeLockStateUnknown))
	setRetryAfter(w, retryAfter)
	http.Error(w, "lock state unknown", http.StatusConflict)
}

func (s *Server) handleReplicateRegistry(w http.ResponseWriter, r *http.Request) {
	var msg replica.RegistryReplicationMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "invalid message", http.StatusBadRequest)
		return
	}

	if err := s.replica.ApplyRegistryReplication(&msg); err != nil {
		if err == replica.ErrJoinInProgress {
			writeRetryableError(w, ErrCodeStoreUnavailable, "recovery in progress", time.Second)
			return
		}
		if strings.Contains(err.Error(), "stale epoch") {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

type RegistryReserveRequest struct {
	CustomerID string `json:"customer_id"`
	Name       string `json:"name"`
}

type RegistryReserveResponse struct {
	ReservationID string `json:"reservation_id"`
	LeaderEpoch   uint64 `json:"leader_epoch"`
}

func (s *Server) handleRegistryReserve(w http.ResponseWriter, r *http.Request) {
	var req RegistryReserveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	reg := s.replica.Registry()
	if reg == nil {
		http.Error(w, "registry not initialized", http.StatusInternalServerError)
		return
	}

	entry, err := reg.Reserve(req.CustomerID, req.Name, s.replica.LeaderEpoch())
	if err != nil {
		switch err {
		case registry.ErrNameExists:
			http.Error(w, "name already exists", http.StatusConflict)
		case registry.ErrNameReserved:
			http.Error(w, "name reservation in progress", http.StatusConflict)
		case registry.ErrNameDeleting:
			http.Error(w, "name deletion in progress", http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	s.replica.QueueRegistryReplication(&replica.RegistryReplicationMessage{
		Type:          replica.MsgRegistryReserve,
		CustomerID:    entry.CustomerID,
		Name:          entry.Name,
		State:         int(entry.State),
		ReservationID: entry.ReservationID,
		Version:       entry.Version,
	})

	resp := RegistryReserveResponse{
		ReservationID: entry.ReservationID,
		LeaderEpoch:   entry.LeaderEpoch,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("error encoding JSON response: %v", err)
	}
}

type RegistryCommitRequest struct {
	CustomerID    string    `json:"customer_id"`
	Name          string    `json:"name"`
	ReservationID string    `json:"reservation_id"`
	StoreID       string    `json:"store_id"`
	ExpiresAt     time.Time `json:"expires_at"`
}

type RegistryCommitResponse struct {
	StoreID     string `json:"store_id"`
	LeaderEpoch uint64 `json:"leader_epoch"`
}

func (s *Server) handleRegistryCommit(w http.ResponseWriter, r *http.Request) {
	var req RegistryCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	reg := s.replica.Registry()
	if reg == nil {
		http.Error(w, "registry not initialized", http.StatusInternalServerError)
		return
	}

	entry, err := reg.Commit(req.CustomerID, req.Name, req.ReservationID, req.StoreID, req.ExpiresAt, s.replica.LeaderEpoch())
	if err != nil {
		switch err {
		case registry.ErrEntryNotFound:
			http.Error(w, "reservation not found", http.StatusNotFound)
		case registry.ErrUnauthorized:
			http.Error(w, "unauthorized", http.StatusForbidden)
		case registry.ErrInvalidState:
			http.Error(w, "invalid state for commit", http.StatusConflict)
		case registry.ErrReservationMismatch:
			http.Error(w, "reservation ID mismatch", http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	s.replica.QueueRegistryReplication(&replica.RegistryReplicationMessage{
		Type:          replica.MsgRegistryCommit,
		CustomerID:    entry.CustomerID,
		Name:          entry.Name,
		StoreID:       entry.StoreID,
		State:         int(entry.State),
		ExpiresAt:     entry.ExpiresAt,
		ReservationID: entry.ReservationID,
		Version:       entry.Version,
	})

	resp := RegistryCommitResponse{
		StoreID:     entry.StoreID,
		LeaderEpoch: entry.LeaderEpoch,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("error encoding JSON response: %v", err)
	}
}

type RegistryAbortRequest struct {
	CustomerID    string `json:"customer_id"`
	Name          string `json:"name"`
	ReservationID string `json:"reservation_id"`
}

func (s *Server) handleRegistryAbort(w http.ResponseWriter, r *http.Request) {
	var req RegistryAbortRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	reg := s.replica.Registry()
	if reg == nil {
		http.Error(w, "registry not initialized", http.StatusInternalServerError)
		return
	}

	if err := reg.Abort(req.CustomerID, req.Name, req.ReservationID); err != nil {
		switch err {
		case registry.ErrUnauthorized:
			http.Error(w, "unauthorized", http.StatusForbidden)
		case registry.ErrInvalidState:
			http.Error(w, "invalid state for abort", http.StatusConflict)
		case registry.ErrReservationMismatch:
			http.Error(w, "reservation ID mismatch", http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	s.replica.QueueRegistryReplication(&replica.RegistryReplicationMessage{
		Type:          replica.MsgRegistryAbort,
		CustomerID:    req.CustomerID,
		Name:          req.Name,
		ReservationID: req.ReservationID,
	})

	w.WriteHeader(http.StatusOK)
}

type RegistryLookupResponse struct {
	StoreID       string `json:"store_id,omitempty"`
	State         string `json:"state"`
	ReservationID string `json:"reservation_id,omitempty"`
}

func (s *Server) handleRegistryLookup(w http.ResponseWriter, r *http.Request) {
	customerID := r.URL.Query().Get("customer_id")
	name := r.URL.Query().Get("name")

	if customerID == "" || name == "" {
		http.Error(w, "missing customer_id or name", http.StatusBadRequest)
		return
	}

	reg := s.replica.Registry()
	if reg == nil {
		http.Error(w, "registry not initialized", http.StatusInternalServerError)
		return
	}

	entry, err := reg.Lookup(customerID, name)
	if err != nil {
		switch err {
		case registry.ErrEntryNotFound:
			http.Error(w, "name not found", http.StatusNotFound)
		case registry.ErrUnauthorized:
			http.Error(w, "unauthorized", http.StatusForbidden)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	resp := RegistryLookupResponse{
		State: entry.State.String(),
	}

	switch entry.State {
	case registry.StateActive:
		resp.StoreID = entry.StoreID
	case registry.StateCreating:
		resp.ReservationID = entry.ReservationID
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("error encoding JSON response: %v", err)
	}
}

type RegistryDeleteRequest struct {
	CustomerID string `json:"customer_id"`
	Name       string `json:"name"`
}

func (s *Server) handleRegistryDelete(w http.ResponseWriter, r *http.Request) {
	var req RegistryDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	reg := s.replica.Registry()
	if reg == nil {
		http.Error(w, "registry not initialized", http.StatusInternalServerError)
		return
	}

	if err := reg.Delete(req.CustomerID, req.Name); err != nil {
		switch err {
		case registry.ErrUnauthorized:
			http.Error(w, "unauthorized", http.StatusForbidden)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	s.replica.QueueRegistryReplication(&replica.RegistryReplicationMessage{
		Type:       replica.MsgRegistryDelete,
		CustomerID: req.CustomerID,
		Name:       req.Name,
	})

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCounterDelta(w http.ResponseWriter, r *http.Request, negate bool, opName string) {
	now := time.Now()

	if !s.requirePrimary(w, r) {
		return
	}

	customerID, ok := s.requireCustomerID(w, r)
	if !ok {
		return
	}

	storeID := strings.TrimSpace(r.PathValue("storeID"))
	if storeID == "" {
		http.Error(w, "missing store ID", http.StatusBadRequest)
		return
	}

	_, ok = s.parseStoreIDWithShard(w, storeID, customerID)
	if !ok {
		return
	}

	body, ok := s.readLimitedBody(w, r)
	if !ok {
		return
	}

	var req IncrementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON request", http.StatusBadRequest)
		return
	}

	newExpiresAt := s.parseExpiresAtHeader(r, now)

	delta := req.Delta
	if negate {
		delta = -delta
	}

	result, err := s.store.Increment(storeID, customerID, delta, s.replica.LeaderEpoch(), newExpiresAt)
	if err != nil {
		writeHTTPError(w, err, "failed to "+opName+" counter")
		return
	}

	s.replica.QueueReplication(&replica.ReplicationMessage{
		Type:       replica.MsgUpdateStore,
		StoreID:    storeID,
		ShardID:    result.ShardID,
		CustomerID: customerID,
		DataType:   uint8(store.DataTypeCounter),
		Body:       result.Body,
		ExpiresAt:  result.ExpiresAt,
		Version:    result.Version,
	})

	resp := CounterResponse{
		Value:   result.Value,
		Version: result.Version,
		Bounded: result.Bounded,
		Min:     result.Min,
		Max:     result.Max,
	}

	w.Header().Set("Content-Type", "application/json")
	s.checkDegradedWrite(w)
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("error writing response body: %v", err)
	}
}

func (s *Server) handleIncrement(w http.ResponseWriter, r *http.Request) {
	s.handleCounterDelta(w, r, false, "increment")
}

func (s *Server) handleDecrement(w http.ResponseWriter, r *http.Request) {
	s.handleCounterDelta(w, r, true, "decrement")
}
