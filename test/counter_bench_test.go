package test

import (
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
)

// BenchmarkCounterIncrement measures atomic counter increment performance
func BenchmarkCounterIncrement(b *testing.B) {
	mgr := store.NewManager()
	s, _ := mgr.CreateCounter("counter1", "shard1", "cust1", 0, nil, nil, time.Now().Add(time.Hour), 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Increment(s.ID, "cust1", 1, 1, time.Time{})
	}
}

// BenchmarkBlobModify measures traditional modify flow performance for comparison
func BenchmarkBlobModify(b *testing.B) {
	mgr := store.NewManager()
	s := &store.Store{
		ID:         "blob1",
		ShardID:    "shard1",
		CustomerID: "cust1",
		DataType:   store.DataTypeBlob,
		Body:       []byte{0, 0, 0, 0, 0, 0, 0, 0}, // 8 bytes like int64
		ExpiresAt:  time.Now().Add(time.Hour),
		Version:    1,
		Role:       store.RolePrimary,
	}
	mgr.Create(s)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lockID, _ := routing.GenerateLockID()
		_, err := mgr.AcquireLock("blob1", "cust1", lockID, 500*time.Millisecond)
		if err != nil {
			b.Fatalf("failed to acquire lock: %v", err)
		}
		mgr.CompleteLock("blob1", "cust1", lockID, []byte{1, 2, 3, 4, 5, 6, 7, 8}, time.Time{})
	}
}

// BenchmarkCounterIncrementParallel measures concurrent counter performance
func BenchmarkCounterIncrementParallel(b *testing.B) {
	mgr := store.NewManager()
	s, _ := mgr.CreateCounter("counter1", "shard1", "cust1", 0, nil, nil, time.Now().Add(time.Hour), 1)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mgr.Increment(s.ID, "cust1", 1, 1, time.Time{})
		}
	})
}

// BenchmarkCounterWithBounds measures performance with bounds checking
func BenchmarkCounterWithBounds(b *testing.B) {
	mgr := store.NewManager()
	min := int64(0)
	max := int64(1000000)
	s, _ := mgr.CreateCounter("counter1", "shard1", "cust1", 500000, &min, &max, time.Now().Add(time.Hour), 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Increment(s.ID, "cust1", 1, 1, time.Time{})
	}
}
