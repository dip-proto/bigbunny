package test

import (
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/auth"
	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
)

func TestStoreCreateAndGet(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:abc123",
		CustomerID: "cust1",
		Body:       []byte("hello world"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}

	if err := mgr.Create(s); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	got, err := mgr.Get(s.ID, s.CustomerID)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if string(got.Body) != "hello world" {
		t.Errorf("body mismatch: got %q, want %q", got.Body, "hello world")
	}

	if got.Version != 1 {
		t.Errorf("version mismatch: got %d, want 1", got.Version)
	}
}

func TestStoreUnauthorized(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:abc123",
		CustomerID: "cust1",
		Body:       []byte("secret"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	_, err := mgr.Get(s.ID, "other-customer")
	if err != store.ErrUnauthorized {
		t.Errorf("expected ErrUnauthorized, got %v", err)
	}
}

func TestStoreLocking(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:lock-test",
		CustomerID: "cust1",
		Body:       []byte{0},
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	// Acquire lock
	_, err := mgr.AcquireLock(s.ID, s.CustomerID, "lock1", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire lock failed: %v", err)
	}

	// Try to acquire again - should fail
	_, err = mgr.AcquireLock(s.ID, s.CustomerID, "lock2", 500*time.Millisecond)
	if err != store.ErrStoreLocked {
		t.Errorf("expected ErrStoreLocked, got %v", err)
	}

	// Complete the lock with new data
	_, err = mgr.CompleteLock(s.ID, s.CustomerID, "lock1", []byte{42}, time.Time{})
	if err != nil {
		t.Fatalf("complete lock failed: %v", err)
	}

	// Verify update
	got, _ := mgr.Get(s.ID, s.CustomerID)
	if got.Body[0] != 42 {
		t.Errorf("body not updated: got %d, want 42", got.Body[0])
	}
	if got.Version != 2 {
		t.Errorf("version not incremented: got %d, want 2", got.Version)
	}
}

func TestRendezvousHashing(t *testing.T) {
	hosts := []*routing.Host{
		{ID: "host1", Address: "localhost:8081", Healthy: true},
		{ID: "host2", Address: "localhost:8082", Healthy: true},
		{ID: "host3", Address: "localhost:8083", Healthy: true},
	}

	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	// Same shard should always map to same hosts
	rs1 := hasher.GetReplicaSet("shard-abc")
	rs2 := hasher.GetReplicaSet("shard-abc")

	if rs1.Primary.ID != rs2.Primary.ID {
		t.Errorf("primary inconsistent: %s vs %s", rs1.Primary.ID, rs2.Primary.ID)
	}
	if rs1.Secondary.ID != rs2.Secondary.ID {
		t.Errorf("secondary inconsistent: %s vs %s", rs1.Secondary.ID, rs2.Secondary.ID)
	}

	// Primary and secondary should be different
	if rs1.Primary.ID == rs1.Secondary.ID {
		t.Errorf("primary and secondary should differ")
	}
}

func TestStoreIDGeneration(t *testing.T) {
	shardID, err := routing.GenerateShardID()
	if err != nil {
		t.Fatalf("generate shard ID failed: %v", err)
	}

	ks := auth.DevKeySet()
	cipher := auth.NewCipher(ks)

	storeID, err := routing.GenerateEncryptedStoreID(cipher, "local", shardID, "customer1")
	if err != nil {
		t.Fatalf("generate store ID failed: %v", err)
	}

	// Verify we can decrypt and get back components
	components, err := cipher.Open(storeID, "customer1")
	if err != nil {
		t.Fatalf("open store ID failed: %v", err)
	}

	if components.Site != "local" {
		t.Errorf("site mismatch: got %s, want local", components.Site)
	}
	if components.ShardID != shardID {
		t.Errorf("shard mismatch: got %s, want %s", components.ShardID, shardID)
	}
}

func TestReplicatedUpdateMerge(t *testing.T) {
	mgr := store.NewManager()

	// Create store with extra fields
	original := &store.Store{
		ID:          "v1:local:shard1:merge-test",
		CustomerID:  "cust1",
		Body:        []byte("original"),
		ExpiresAt:   time.Now().Add(time.Hour),
		Version:     1,
		PendingName: "my-named-store",
	}
	mgr.Create(original)

	// Apply replicated update - should preserve PendingName and CreatedAt
	update := &store.ReplicatedUpdate{
		StoreID:          original.ID,
		CustomerID:       original.CustomerID,
		Body:             []byte("updated"),
		ExpiresAt:        time.Now().Add(2 * time.Hour),
		Version:          2,
		LeaderEpoch:      5,
		LastReplicatedAt: time.Now(),
	}

	err := mgr.ApplyReplicatedUpdate(update)
	if err != nil {
		t.Fatalf("apply replicated update failed: %v", err)
	}

	got, _ := mgr.Get(original.ID, original.CustomerID)

	// Updated fields should change
	if string(got.Body) != "updated" {
		t.Errorf("body not updated: got %q", got.Body)
	}
	if got.Version != 2 {
		t.Errorf("version not updated: got %d, want 2", got.Version)
	}
	if got.LeaderEpoch != 5 {
		t.Errorf("leader epoch not updated: got %d, want 5", got.LeaderEpoch)
	}

	// Preserved fields should remain
	if got.PendingName != "my-named-store" {
		t.Errorf("PendingName lost: got %q, want %q", got.PendingName, "my-named-store")
	}
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt was lost (is zero)")
	}
}

func TestReplicatedUpdateIdempotent(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:idempotent-test",
		CustomerID: "cust1",
		Body:       []byte("v2-body"),
		ExpiresAt:  time.Now().Add(time.Hour),
		Version:    2,
	}
	mgr.Create(s)

	// Try to apply older version - should be no-op
	update := &store.ReplicatedUpdate{
		StoreID:    s.ID,
		CustomerID: s.CustomerID,
		Body:       []byte("v1-body-old"),
		Version:    1, // older version
	}

	err := mgr.ApplyReplicatedUpdate(update)
	if err != nil {
		t.Fatalf("apply old version failed: %v", err)
	}

	got, _ := mgr.Get(s.ID, s.CustomerID)
	if string(got.Body) != "v2-body" {
		t.Errorf("old version was applied: got %q, want %q", got.Body, "v2-body")
	}
}

func TestReplicatedUpdateCustomerMismatch(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:mismatch-test",
		CustomerID: "cust1",
		Body:       []byte("secret"),
		ExpiresAt:  time.Now().Add(time.Hour),
		Version:    1,
	}
	mgr.Create(s)

	// Try to update with different customer - should fail
	update := &store.ReplicatedUpdate{
		StoreID:    s.ID,
		CustomerID: "attacker", // different customer
		Body:       []byte("pwned"),
		Version:    2,
	}

	err := mgr.ApplyReplicatedUpdate(update)
	if err != store.ErrUnauthorized {
		t.Errorf("expected ErrUnauthorized, got %v", err)
	}

	// Verify original data unchanged
	got, _ := mgr.Get(s.ID, s.CustomerID)
	if string(got.Body) != "secret" {
		t.Errorf("data was modified despite mismatch: got %q", got.Body)
	}
}

func TestCreateOrUpdateCustomerMismatch(t *testing.T) {
	mgr := store.NewManager()

	// Create original store
	original := &store.Store{
		ID:         "v1:local:shard1:create-or-update-test",
		CustomerID: "cust1",
		Body:       []byte("original"),
		ExpiresAt:  time.Now().Add(time.Hour),
		Version:    1,
	}
	mgr.Create(original)

	// Try CreateOrUpdate with different customer - should fail
	impostor := &store.Store{
		ID:         original.ID,
		CustomerID: "attacker",
		Body:       []byte("hijacked"),
		Version:    2,
	}

	err := mgr.CreateOrUpdate(impostor)
	if err != store.ErrUnauthorized {
		t.Errorf("expected ErrUnauthorized, got %v", err)
	}

	// Verify original unchanged
	got, _ := mgr.Get(original.ID, original.CustomerID)
	if string(got.Body) != "original" {
		t.Errorf("store was hijacked: got %q", got.Body)
	}
}

func TestCreateOrUpdatePreservesCreatedAt(t *testing.T) {
	mgr := store.NewManager()

	// Create original
	original := &store.Store{
		ID:         "v1:local:shard1:preserve-created-test",
		CustomerID: "cust1",
		Body:       []byte("v1"),
		Version:    1,
	}
	mgr.Create(original)

	// Get the CreatedAt that was set
	got1, _ := mgr.Get(original.ID, original.CustomerID)
	originalCreatedAt := got1.CreatedAt

	time.Sleep(10 * time.Millisecond) // ensure time difference

	// Update via CreateOrUpdate
	updated := &store.Store{
		ID:         original.ID,
		CustomerID: original.CustomerID,
		Body:       []byte("v2"),
		Version:    2,
	}
	mgr.CreateOrUpdate(updated)

	got2, _ := mgr.Get(original.ID, original.CustomerID)

	if !got2.CreatedAt.Equal(originalCreatedAt) {
		t.Errorf("CreatedAt changed: was %v, now %v", originalCreatedAt, got2.CreatedAt)
	}
	if string(got2.Body) != "v2" {
		t.Errorf("body not updated: got %q", got2.Body)
	}
}

func TestSetLockAndClearLock(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:lock-test",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	// SetLock should set the lock on the store (used by replication)
	lockTime := time.Now()
	err := mgr.SetLock(s.ID, "lock123", lockTime, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("SetLock failed: %v", err)
	}

	// Verify lock is set by trying to acquire - should fail
	_, err = mgr.AcquireLock(s.ID, s.CustomerID, "other-lock", 500*time.Millisecond)
	if err != store.ErrStoreLocked {
		t.Errorf("expected ErrStoreLocked after SetLock, got %v", err)
	}

	// ClearLock with correct ID should clear it
	err = mgr.ClearLock(s.ID, "lock123")
	if err != nil {
		t.Fatalf("ClearLock failed: %v", err)
	}

	// Now acquire should work
	_, err = mgr.AcquireLock(s.ID, s.CustomerID, "new-lock", 500*time.Millisecond)
	if err != nil {
		t.Errorf("expected acquire to succeed after ClearLock, got %v", err)
	}
}

func TestClearLockWrongID(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:clear-wrong-test",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	// Set a lock
	mgr.SetLock(s.ID, "lock-A", time.Now(), 500*time.Millisecond)

	// Try to clear with wrong ID - should not clear the lock
	mgr.ClearLock(s.ID, "lock-B")

	// Lock should still be held
	_, err := mgr.AcquireLock(s.ID, s.CustomerID, "lock-C", 500*time.Millisecond)
	if err != store.ErrStoreLocked {
		t.Errorf("expected ErrStoreLocked (wrong clear ID should not clear), got %v", err)
	}
}

func TestClearLockUnconditionally(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:unconditional-test",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	// Set a lock
	mgr.SetLock(s.ID, "some-lock", time.Now(), 500*time.Millisecond)

	// Clear unconditionally (no lock ID check)
	mgr.ClearLockUnconditionally(s.ID)

	// Lock should be cleared
	_, err := mgr.AcquireLock(s.ID, s.CustomerID, "new-lock", 500*time.Millisecond)
	if err != nil {
		t.Errorf("expected acquire to succeed after ClearLockUnconditionally, got %v", err)
	}
}

func TestSetLockOverwritesExpired(t *testing.T) {
	mgr := store.NewManager()

	s := &store.Store{
		ID:         "v1:local:shard1:overwrite-test",
		CustomerID: "cust1",
		Body:       []byte("data"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(s)

	// Set a lock with very short timeout
	mgr.SetLock(s.ID, "old-lock", time.Now().Add(-time.Second), 1*time.Millisecond)

	// SetLock should be able to overwrite expired lock
	err := mgr.SetLock(s.ID, "new-lock", time.Now(), 500*time.Millisecond)
	if err != nil {
		t.Fatalf("SetLock should overwrite expired lock: %v", err)
	}

	// The new lock should be active
	_, err = mgr.AcquireLock(s.ID, s.CustomerID, "another-lock", 500*time.Millisecond)
	if err != store.ErrStoreLocked {
		t.Errorf("expected ErrStoreLocked with new lock, got %v", err)
	}
}

// Phase C.5: Snapshot and Reset tests

func TestStoreSnapshot(t *testing.T) {
	mgr := store.NewManager()

	// Create some stores
	stores := []*store.Store{
		{ID: "store1", CustomerID: "cust1", Body: []byte("data1"), ExpiresAt: time.Now().Add(time.Hour)},
		{ID: "store2", CustomerID: "cust1", Body: []byte("data2"), ExpiresAt: time.Now().Add(time.Hour)},
		{ID: "store3", CustomerID: "cust2", Body: []byte("data3"), ExpiresAt: time.Now().Add(time.Hour)},
	}

	for _, s := range stores {
		if err := mgr.Create(s); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	// Take snapshot
	snapshot := mgr.Snapshot()

	if len(snapshot) != 3 {
		t.Errorf("expected 3 stores in snapshot, got %d", len(snapshot))
	}

	// Verify all stores are in snapshot
	foundStores := make(map[string]bool)
	for _, s := range snapshot {
		foundStores[s.ID] = true
	}
	for _, s := range stores {
		if !foundStores[s.ID] {
			t.Errorf("store %s not found in snapshot", s.ID)
		}
	}
}

func TestStoreReset(t *testing.T) {
	mgr := store.NewManager()

	// Create initial stores
	initial := []*store.Store{
		{ID: "old1", CustomerID: "cust1", Body: []byte("old-data"), ExpiresAt: time.Now().Add(time.Hour)},
		{ID: "old2", CustomerID: "cust1", Body: []byte("old-data"), ExpiresAt: time.Now().Add(time.Hour)},
	}
	for _, s := range initial {
		mgr.Create(s)
	}

	// Reset with new stores
	newStores := []*store.Store{
		{ID: "new1", CustomerID: "cust2", Body: []byte("new-data1"), ExpiresAt: time.Now().Add(time.Hour), Role: store.RolePrimary},
		{ID: "new2", CustomerID: "cust2", Body: []byte("new-data2"), ExpiresAt: time.Now().Add(time.Hour), Role: store.RolePrimary},
		{ID: "new3", CustomerID: "cust3", Body: []byte("new-data3"), ExpiresAt: time.Now().Add(time.Hour), Role: store.RolePrimary},
	}
	mgr.Reset(newStores)

	// Old stores should be gone
	_, err := mgr.Get("old1", "cust1")
	if err != store.ErrStoreNotFound {
		t.Errorf("expected old store to be gone, got %v", err)
	}

	// New stores should exist
	for _, s := range newStores {
		got, err := mgr.Get(s.ID, s.CustomerID)
		if err != nil {
			t.Errorf("expected new store %s to exist: %v", s.ID, err)
			continue
		}
		if string(got.Body) != string(s.Body) {
			t.Errorf("store %s body mismatch: got %q", s.ID, got.Body)
		}
		// After reset, stores should be secondary
		if got.Role != store.RoleSecondary {
			t.Errorf("store %s should be secondary after reset, got %v", s.ID, got.Role)
		}
	}

	// Count should be updated
	if mgr.Count() != 3 {
		t.Errorf("expected count 3, got %d", mgr.Count())
	}
}

func TestStoreResetUpdatesMemoryUsage(t *testing.T) {
	mgr := store.NewManager()

	// Create initial stores
	initial := &store.Store{
		ID:         "old",
		CustomerID: "cust1",
		Body:       make([]byte, 1000), // 1KB body
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(initial)

	initialMemory := mgr.MemoryUsage()
	if initialMemory == 0 {
		t.Fatal("expected non-zero memory usage after create")
	}

	// Reset with smaller stores
	small := []*store.Store{
		{ID: "small1", CustomerID: "cust1", Body: []byte("tiny"), ExpiresAt: time.Now().Add(time.Hour)},
	}
	mgr.Reset(small)

	newMemory := mgr.MemoryUsage()
	if newMemory >= initialMemory {
		t.Errorf("expected memory to decrease after reset: before=%d, after=%d", initialMemory, newMemory)
	}
}

func TestCounterCreateAndIncrement(t *testing.T) {
	mgr := store.NewManager()
	initialValue := int64(10)

	s, err := mgr.CreateCounter("counter1", "shard1", "cust1", initialValue, nil, nil, time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("create counter failed: %v", err)
	}

	if s.DataType != store.DataTypeCounter {
		t.Errorf("expected counter type, got %s", s.DataType)
	}

	// Increment by 5
	result, err := mgr.Increment("counter1", "cust1", 5, 1, time.Time{})
	if err != nil {
		t.Fatalf("increment failed: %v", err)
	}
	if result.Value != 15 {
		t.Errorf("expected value 15, got %d", result.Value)
	}
	if result.Bounded {
		t.Error("expected bounded=false for unbounded counter")
	}

	// Verify version incremented
	if result.Version != 2 {
		t.Errorf("expected version 2, got %d", result.Version)
	}

	// Decrement (negative delta)
	result, err = mgr.Increment("counter1", "cust1", -3, 1, time.Time{})
	if err != nil {
		t.Fatalf("decrement failed: %v", err)
	}
	if result.Value != 12 {
		t.Errorf("expected value 12, got %d", result.Value)
	}
}

func TestCounterWithBounds(t *testing.T) {
	mgr := store.NewManager()
	min := int64(0)
	max := int64(100)

	_, err := mgr.CreateCounter("counter1", "shard1", "cust1", 50, &min, &max, time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("create counter failed: %v", err)
	}

	// Increment within bounds
	result, err := mgr.Increment("counter1", "cust1", 30, 1, time.Time{})
	if err != nil {
		t.Fatalf("increment failed: %v", err)
	}
	if result.Value != 80 || result.Bounded {
		t.Errorf("expected value 80, bounded=false; got %d, %v", result.Value, result.Bounded)
	}

	// Increment beyond max - should clamp to 100
	result, err = mgr.Increment("counter1", "cust1", 50, 1, time.Time{})
	if err != nil {
		t.Fatalf("increment failed: %v", err)
	}
	if result.Value != 100 {
		t.Errorf("expected value clamped to 100, got %d", result.Value)
	}
	if !result.Bounded {
		t.Error("expected bounded=true when hitting max")
	}

	// Decrement below min - should clamp to 0
	result, err = mgr.Increment("counter1", "cust1", -150, 1, time.Time{})
	if err != nil {
		t.Fatalf("decrement failed: %v", err)
	}
	if result.Value != 0 {
		t.Errorf("expected value clamped to 0, got %d", result.Value)
	}
	if !result.Bounded {
		t.Error("expected bounded=true when hitting min")
	}
}

func TestCounterInvalidBounds(t *testing.T) {
	mgr := store.NewManager()
	min := int64(100)
	max := int64(0) // min > max - invalid!

	_, err := mgr.CreateCounter("counter1", "shard1", "cust1", 50, &min, &max, time.Now().Add(time.Hour), 1)
	if err != store.ErrInvalidBounds {
		t.Errorf("expected ErrInvalidBounds, got %v", err)
	}
}

func TestCounterInitialValueOutOfBounds(t *testing.T) {
	mgr := store.NewManager()
	min := int64(10)
	max := int64(100)

	// Initial value below min
	_, err := mgr.CreateCounter("counter1", "shard1", "cust1", 5, &min, &max, time.Now().Add(time.Hour), 1)
	if err != store.ErrValueOutOfBounds {
		t.Errorf("expected ErrValueOutOfBounds for value < min, got %v", err)
	}

	// Initial value above max
	_, err = mgr.CreateCounter("counter2", "shard1", "cust1", 150, &min, &max, time.Now().Add(time.Hour), 1)
	if err != store.ErrValueOutOfBounds {
		t.Errorf("expected ErrValueOutOfBounds for value > max, got %v", err)
	}
}

func TestCounterOverflow(t *testing.T) {
	mgr := store.NewManager()
	initial := int64(9223372036854775800) // Near MaxInt64

	_, err := mgr.CreateCounter("counter1", "shard1", "cust1", initial, nil, nil, time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("create counter failed: %v", err)
	}

	// Try to increment beyond MaxInt64
	_, err = mgr.Increment("counter1", "cust1", 1000, 1, time.Time{})
	if err != store.ErrOverflow {
		t.Errorf("expected ErrOverflow, got %v", err)
	}
}

func TestCounterUnderflow(t *testing.T) {
	mgr := store.NewManager()
	initial := int64(-9223372036854775800) // Near MinInt64

	_, err := mgr.CreateCounter("counter1", "shard1", "cust1", initial, nil, nil, time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("create counter failed: %v", err)
	}

	// Try to decrement beyond MinInt64
	_, err = mgr.Increment("counter1", "cust1", -1000, 1, time.Time{})
	if err != store.ErrOverflow {
		t.Errorf("expected ErrOverflow, got %v", err)
	}
}

func TestCounterTypeMismatch(t *testing.T) {
	mgr := store.NewManager()

	// Create a blob store
	blob := &store.Store{
		ID:         "blob1",
		CustomerID: "cust1",
		DataType:   store.DataTypeBlob,
		Body:       []byte("not a counter"),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	mgr.Create(blob)

	// Try to increment blob store
	_, err := mgr.Increment("blob1", "cust1", 5, 1, time.Time{})
	if err != store.ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestCounterLockedStore(t *testing.T) {
	mgr := store.NewManager()

	_, err := mgr.CreateCounter("counter1", "shard1", "cust1", 10, nil, nil, time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("create counter failed: %v", err)
	}

	// Acquire lock on the counter
	_, err = mgr.AcquireLock("counter1", "cust1", "lock1", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire lock failed: %v", err)
	}

	// Try to increment locked counter - should fail
	_, err = mgr.Increment("counter1", "cust1", 5, 1, time.Time{})
	if err != store.ErrStoreLocked {
		t.Errorf("expected ErrStoreLocked, got %v", err)
	}

	// Release lock
	mgr.ReleaseLock("counter1", "cust1", "lock1")

	// Now increment should work
	result, err := mgr.Increment("counter1", "cust1", 5, 1, time.Time{})
	if err != nil {
		t.Fatalf("increment after unlock failed: %v", err)
	}
	if result.Value != 15 {
		t.Errorf("expected value 15, got %d", result.Value)
	}
}

func TestCounterGetCounter(t *testing.T) {
	mgr := store.NewManager()
	min := int64(0)
	max := int64(100)

	_, err := mgr.CreateCounter("counter1", "shard1", "cust1", 42, &min, &max, time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("create counter failed: %v", err)
	}

	data, version, err := mgr.GetCounter("counter1", "cust1")
	if err != nil {
		t.Fatalf("get counter failed: %v", err)
	}

	if data.Value != 42 {
		t.Errorf("expected value 42, got %d", data.Value)
	}
	if data.Min == nil || *data.Min != 0 {
		t.Errorf("expected min=0, got %v", data.Min)
	}
	if data.Max == nil || *data.Max != 100 {
		t.Errorf("expected max=100, got %v", data.Max)
	}
	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}
}

func TestCounterSetCounter(t *testing.T) {
	mgr := store.NewManager()
	min := int64(0)
	max := int64(100)

	_, err := mgr.CreateCounter("counter1", "shard1", "cust1", 50, &min, &max, time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("create counter failed: %v", err)
	}

	// Set to valid value
	version, err := mgr.SetCounter("counter1", "cust1", 75, time.Time{})
	if err != nil {
		t.Fatalf("set counter failed: %v", err)
	}
	if version != 2 {
		t.Errorf("expected version 2, got %d", version)
	}

	// Verify value changed
	data, _, err := mgr.GetCounter("counter1", "cust1")
	if err != nil {
		t.Fatalf("get counter failed: %v", err)
	}
	if data.Value != 75 {
		t.Errorf("expected value 75, got %d", data.Value)
	}

	// Try to set out of bounds
	_, err = mgr.SetCounter("counter1", "cust1", 150, time.Time{})
	if err != store.ErrValueOutOfBounds {
		t.Errorf("expected ErrValueOutOfBounds, got %v", err)
	}

	_, err = mgr.SetCounter("counter1", "cust1", -10, time.Time{})
	if err != store.ErrValueOutOfBounds {
		t.Errorf("expected ErrValueOutOfBounds, got %v", err)
	}
}

func TestCounterConcurrentIncrements(t *testing.T) {
	mgr := store.NewManager()

	_, err := mgr.CreateCounter("counter1", "shard1", "cust1", 0, nil, nil, time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("create counter failed: %v", err)
	}

	// Launch 10 goroutines, each incrementing by 10
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				mgr.Increment("counter1", "cust1", 1, 1, time.Time{})
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify final value is 100 (10 goroutines * 10 increments)
	data, _, err := mgr.GetCounter("counter1", "cust1")
	if err != nil {
		t.Fatalf("get counter failed: %v", err)
	}
	if data.Value != 100 {
		t.Errorf("expected value 100 from concurrent increments, got %d", data.Value)
	}
}
