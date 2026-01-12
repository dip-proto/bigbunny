package ratelimit

import (
	"sync"
	"testing"
	"time"
)

type testClock struct {
	mu   sync.Mutex
	time time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.time
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.time = c.time.Add(d)
}

func TestLimiter_BasicRateLimit(t *testing.T) {
	clock := &testClock{time: time.Now()}
	limiter := NewLimiterWithClock(10, 10, clock)
	defer limiter.Stop()

	customerID := "customer1"

	// First 10 requests should succeed (burst capacity)
	for i := 0; i < 10; i++ {
		if !limiter.Allow(customerID) {
			t.Errorf("request %d should be allowed (burst capacity)", i)
		}
	}

	// 11th request should be blocked (bucket empty)
	if limiter.Allow(customerID) {
		t.Error("request 11 should be blocked (bucket empty)")
	}

	// Advance 1 second to refill tokens (10 req/s)
	clock.Advance(1 * time.Second)

	// Next 10 requests should succeed again
	for i := 0; i < 10; i++ {
		if !limiter.Allow(customerID) {
			t.Errorf("request %d after refill should be allowed", i)
		}
	}
}

func TestLimiter_MultipleCustomers(t *testing.T) {
	clock := &testClock{time: time.Now()}
	limiter := NewLimiterWithClock(5, 5, clock)
	defer limiter.Stop()

	// Customer1 uses all their tokens
	for i := 0; i < 5; i++ {
		if !limiter.Allow("customer1") {
			t.Errorf("customer1 request %d should be allowed", i)
		}
	}
	if limiter.Allow("customer1") {
		t.Error("customer1 should be rate limited")
	}

	// Customer2 should still have tokens
	for i := 0; i < 5; i++ {
		if !limiter.Allow("customer2") {
			t.Errorf("customer2 request %d should be allowed", i)
		}
	}
	if limiter.Allow("customer2") {
		t.Error("customer2 should be rate limited")
	}

	// Advance time to refill
	clock.Advance(1 * time.Second)

	// Both customers should have tokens again
	if !limiter.Allow("customer1") {
		t.Error("customer1 should be allowed after refill")
	}
	if !limiter.Allow("customer2") {
		t.Error("customer2 should be allowed after refill")
	}
}

func TestLimiter_GradualRefill(t *testing.T) {
	clock := &testClock{time: time.Now()}
	limiter := NewLimiterWithClock(10, 10, clock)
	defer limiter.Stop()

	customerID := "customer1"

	// Use all tokens
	for i := 0; i < 10; i++ {
		limiter.Allow(customerID)
	}

	// Should be blocked
	if limiter.Allow(customerID) {
		t.Error("should be blocked after using all tokens")
	}

	// Advance 100ms (1 token should be added: 10 req/s = 1 req per 100ms)
	clock.Advance(100 * time.Millisecond)

	// Should allow 1 request
	if !limiter.Allow(customerID) {
		t.Error("should allow 1 request after 100ms")
	}

	// Should be blocked again
	if limiter.Allow(customerID) {
		t.Error("should be blocked after using refilled token")
	}

	// Advance 500ms (5 more tokens)
	clock.Advance(500 * time.Millisecond)

	// Should allow 5 requests
	for i := 0; i < 5; i++ {
		if !limiter.Allow(customerID) {
			t.Errorf("request %d should be allowed after 500ms refill", i)
		}
	}

	// Should be blocked
	if limiter.Allow(customerID) {
		t.Error("should be blocked after using all refilled tokens")
	}
}

func TestLimiter_BurstCapacity(t *testing.T) {
	clock := &testClock{time: time.Now()}
	limiter := NewLimiterWithClock(10, 20, clock)
	defer limiter.Stop()

	customerID := "customer1"

	// Should allow burst of 20 requests
	for i := 0; i < 20; i++ {
		if !limiter.Allow(customerID) {
			t.Errorf("request %d should be allowed (within burst)", i)
		}
	}

	// 21st should be blocked
	if limiter.Allow(customerID) {
		t.Error("request 21 should be blocked (burst exceeded)")
	}
}

func TestLimiter_NoOverfillBeyondCapacity(t *testing.T) {
	clock := &testClock{time: time.Now()}
	limiter := NewLimiterWithClock(10, 10, clock)
	defer limiter.Stop()

	customerID := "customer1"

	// Use 5 tokens
	for i := 0; i < 5; i++ {
		limiter.Allow(customerID)
	}

	// Advance 10 seconds (should add 100 tokens, but capped at capacity)
	clock.Advance(10 * time.Second)

	// Should only have capacity (10 tokens), not 105
	for i := 0; i < 10; i++ {
		if !limiter.Allow(customerID) {
			t.Errorf("request %d should be allowed", i)
		}
	}

	// 11th should be blocked
	if limiter.Allow(customerID) {
		t.Error("should be blocked (capacity is 10)")
	}
}

func TestLimiter_Cleanup(t *testing.T) {
	clock := &testClock{time: time.Now()}
	limiter := NewLimiterWithClock(10, 10, clock)
	defer limiter.Stop()

	// Create buckets for 3 customers, use only 1 token each (so they'll refill to capacity)
	limiter.Allow("customer1")
	limiter.Allow("customer2")
	limiter.Allow("customer3")

	// Check initial state
	if limiter.Stats() != 3 {
		t.Errorf("expected 3 buckets, got %d", limiter.Stats())
	}

	// Advance time beyond refill time to get to full capacity (1 second is enough for 10 tokens/s)
	clock.Advance(1 * time.Second)

	// Advance time beyond inactive threshold
	clock.Advance(15 * time.Minute)

	// Run cleanup - buckets are inactive (15 min) and will be at full capacity when checked
	limiter.cleanup()

	// All buckets should be cleaned up (inactive and at full capacity)
	if limiter.Stats() != 0 {
		t.Errorf("expected 0 buckets after cleanup, got %d", limiter.Stats())
	}

	// Use customer1 again, creating a new bucket
	limiter.Allow("customer1")

	if limiter.Stats() != 1 {
		t.Errorf("expected 1 bucket after new request, got %d", limiter.Stats())
	}
}

func TestLimiter_CleanupKeepsRecentlyActiveBuckets(t *testing.T) {
	clock := &testClock{time: time.Now()}
	limiter := NewLimiterWithClock(10, 10, clock)
	defer limiter.Stop()

	// Create bucket for customer1
	limiter.Allow("customer1")

	// Advance time but stay within inactive threshold (5 minutes < 10 minutes)
	clock.Advance(5 * time.Minute)

	// Run cleanup
	limiter.cleanup()

	// Bucket should NOT be cleaned up (accessed within threshold)
	if limiter.Stats() != 1 {
		t.Errorf("expected 1 bucket (recently active), got %d", limiter.Stats())
	}
}

func TestLimiter_ConcurrentAccess(t *testing.T) {
	limiter := NewLimiter(100, 100)
	defer limiter.Stop()

	customerID := "customer1"
	concurrency := 10
	requestsPerGoroutine := 20

	var wg sync.WaitGroup
	wg.Add(concurrency)

	allowed := make([]int, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			count := 0
			for j := 0; j < requestsPerGoroutine; j++ {
				if limiter.Allow(customerID) {
					count++
				}
			}
			allowed[idx] = count
		}(i)
	}

	wg.Wait()

	// Sum all allowed requests
	totalAllowed := 0
	for _, count := range allowed {
		totalAllowed += count
	}

	// Should allow exactly burst capacity (100)
	if totalAllowed != 100 {
		t.Errorf("expected 100 allowed requests, got %d", totalAllowed)
	}
}

func TestLimiter_ZeroInitialTokens(t *testing.T) {
	clock := &testClock{time: time.Now()}
	limiter := NewLimiterWithClock(10, 10, clock)
	defer limiter.Stop()

	customerID := "customer1"

	// Use all tokens
	for i := 0; i < 10; i++ {
		limiter.Allow(customerID)
	}

	// Should be blocked
	if limiter.Allow(customerID) {
		t.Error("should be blocked with zero tokens")
	}

	// Advance minimal time (1ms = 0.01 tokens, not enough)
	clock.Advance(1 * time.Millisecond)

	// Still blocked (need at least 1 token)
	if limiter.Allow(customerID) {
		t.Error("should still be blocked with 0.01 tokens")
	}

	// Advance to get 1 full token
	clock.Advance(99 * time.Millisecond)

	// Should allow 1 request
	if !limiter.Allow(customerID) {
		t.Error("should allow 1 request after 100ms total")
	}
}
