package ratelimit

import (
	"sync"
	"time"
)

// Limiter implements a per-customer token bucket rate limiter. It maintains
// separate buckets for each customer ID and refills tokens at a constant rate
// up to a configurable burst capacity. Inactive buckets are periodically
// cleaned up to prevent memory leaks.
type Limiter struct {
	mu          sync.RWMutex
	buckets     map[string]*tokenBucket
	rate        float64 // tokens per second
	capacity    float64 // max tokens in bucket
	clock       Clock
	stopCleanup chan struct{}
	cleanupWg   sync.WaitGroup
}

type tokenBucket struct {
	tokens    float64
	lastCheck time.Time
}

// Clock provides the current time. This abstraction exists so tests can
// control time progression without waiting for real time to pass.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// NewLimiter creates a rate limiter that allows the given number of requests
// per second with a burst capacity for handling traffic spikes. It starts a
// background goroutine for cleanup, so you should call Stop when done.
func NewLimiter(rate, burst int) *Limiter {
	return NewLimiterWithClock(rate, burst, realClock{})
}

// NewLimiterWithClock is like NewLimiter but accepts a custom clock, which is
// handy for testing time-dependent behavior without sleeps.
func NewLimiterWithClock(rate, burst int, clock Clock) *Limiter {
	l := &Limiter{
		buckets:     make(map[string]*tokenBucket),
		rate:        float64(rate),
		capacity:    float64(burst),
		clock:       clock,
		stopCleanup: make(chan struct{}),
	}
	l.startCleanup()
	return l
}

// Allow checks whether the customer has tokens available and consumes one if
// so. Returns true if the request should proceed, false if rate limited.
func (l *Limiter) Allow(customerID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	bucket, exists := l.buckets[customerID]
	if !exists {
		bucket = &tokenBucket{
			tokens:    l.capacity,
			lastCheck: l.clock.Now(),
		}
		l.buckets[customerID] = bucket
	}

	now := l.clock.Now()
	elapsed := now.Sub(bucket.lastCheck).Seconds()

	// Add tokens based on time elapsed
	bucket.tokens = min(l.capacity, bucket.tokens+(elapsed*l.rate))
	bucket.lastCheck = now

	// Check if we have enough tokens
	if bucket.tokens >= 1.0 {
		bucket.tokens -= 1.0
		return true
	}

	return false
}

func (l *Limiter) startCleanup() {
	l.cleanupWg.Add(1)
	go func() {
		defer l.cleanupWg.Done()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				l.cleanup()
			case <-l.stopCleanup:
				return
			}
		}
	}()
}

func (l *Limiter) cleanup() {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock.Now()
	inactiveThreshold := 10 * time.Minute

	for customerID, bucket := range l.buckets {
		// Check if bucket is inactive
		if now.Sub(bucket.lastCheck) > inactiveThreshold {
			// Calculate refilled tokens to check if at full capacity
			elapsed := now.Sub(bucket.lastCheck).Seconds()
			refilledTokens := min(l.capacity, bucket.tokens+(elapsed*l.rate))

			// Remove if at full capacity (no recent activity)
			if refilledTokens >= l.capacity {
				delete(l.buckets, customerID)
			}
		}
	}
}

// Stop shuts down the background cleanup goroutine and waits for it to finish.
// Call this when you are done with the limiter to avoid leaking goroutines.
func (l *Limiter) Stop() {
	close(l.stopCleanup)
	l.cleanupWg.Wait()
}

// Stats returns the number of customer buckets currently tracked. Useful for
// monitoring memory usage and verifying that cleanup is working properly.
func (l *Limiter) Stats() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.buckets)
}
