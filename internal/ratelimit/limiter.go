package ratelimit

import (
	"sync"
	"time"
)

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

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func NewLimiter(rate, burst int) *Limiter {
	return NewLimiterWithClock(rate, burst, realClock{})
}

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

func (l *Limiter) Stop() {
	close(l.stopCleanup)
	l.cleanupWg.Wait()
}

func (l *Limiter) Stats() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.buckets)
}
