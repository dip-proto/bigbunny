package test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/test/harness"
)

// Soak test configuration
type soakConfig struct {
	StoreCount    int
	TargetRPS     int
	WarmupTime    time.Duration
	LoadTime      time.Duration
	BodySize      int
	MemoryLimit   int64
	WithFailover  bool
	FailoverDelay time.Duration // when to trigger failover
	FailoverDur   time.Duration // how long partition lasts
}

func ciSoakConfig() soakConfig {
	return soakConfig{
		StoreCount:  1000,
		TargetRPS:   50,
		WarmupTime:  5 * time.Second,
		LoadTime:    20 * time.Second,
		BodySize:    512,
		MemoryLimit: 128 * 1024 * 1024, // 128MB
	}
}

func fullSoakConfig() soakConfig {
	return soakConfig{
		StoreCount:    10000,
		TargetRPS:     100,
		WarmupTime:    15 * time.Second,
		LoadTime:      3 * time.Minute,
		BodySize:      1024,
		MemoryLimit:   512 * 1024 * 1024, // 512MB
		WithFailover:  true,
		FailoverDelay: 30 * time.Second,
		FailoverDur:   5 * time.Second,
	}
}

// Metrics collector for soak tests
type soakMetrics struct {
	mu sync.Mutex

	// Counters
	totalRequests atomic.Int64
	totalErrors   atomic.Int64
	createCount   atomic.Int64
	readCount     atomic.Int64
	modifyCount   atomic.Int64
	deleteCount   atomic.Int64

	// Latencies (sampled to avoid memory blowup)
	latencies   []time.Duration
	maxSamples  int
	sampleCount int
}

func newSoakMetrics(maxSamples int) *soakMetrics {
	return &soakMetrics{
		latencies:  make([]time.Duration, 0, maxSamples),
		maxSamples: maxSamples,
	}
}

func (m *soakMetrics) recordRequest(op string, dur time.Duration, err error) {
	m.totalRequests.Add(1)
	if err != nil {
		m.totalErrors.Add(1)
	}

	switch op {
	case "create":
		m.createCount.Add(1)
	case "read":
		m.readCount.Add(1)
	case "modify":
		m.modifyCount.Add(1)
	case "delete":
		m.deleteCount.Add(1)
	}

	// Reservoir sampling for latencies
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sampleCount++
	if len(m.latencies) < m.maxSamples {
		m.latencies = append(m.latencies, dur)
	} else {
		// Replace random element with decreasing probability
		j := rand.Intn(m.sampleCount)
		if j < m.maxSamples {
			m.latencies[j] = dur
		}
	}
}

func (m *soakMetrics) errorRate() float64 {
	total := m.totalRequests.Load()
	if total == 0 {
		return 0
	}
	return float64(m.totalErrors.Load()) / float64(total)
}

func (m *soakMetrics) percentile(p float64) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.latencies) == 0 {
		return 0
	}

	sorted := make([]time.Duration, len(m.latencies))
	copy(sorted, m.latencies)
	slices.Sort(sorted)

	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func (m *soakMetrics) summary() string {
	return fmt.Sprintf(
		"requests=%d errors=%d (%.2f%%) create=%d read=%d modify=%d delete=%d p50=%v p99=%v",
		m.totalRequests.Load(),
		m.totalErrors.Load(),
		m.errorRate()*100,
		m.createCount.Load(),
		m.readCount.Load(),
		m.modifyCount.Load(),
		m.deleteCount.Load(),
		m.percentile(0.5),
		m.percentile(0.99),
	)
}

// Rate limiter using token bucket
type rateLimiter struct {
	interval time.Duration
	ticker   *time.Ticker
	tokens   chan struct{}
	done     chan struct{}
}

func newRateLimiter(rps int) *rateLimiter {
	rl := &rateLimiter{
		interval: time.Second / time.Duration(rps),
		tokens:   make(chan struct{}, rps*2), // buffer for burst
		done:     make(chan struct{}),
	}
	rl.ticker = time.NewTicker(rl.interval)
	go rl.run()
	return rl
}

func (rl *rateLimiter) run() {
	for {
		select {
		case <-rl.ticker.C:
			select {
			case rl.tokens <- struct{}{}:
			default: // drop if buffer full
			}
		case <-rl.done:
			return
		}
	}
}

func (rl *rateLimiter) wait() bool {
	select {
	case <-rl.tokens:
		return true
	case <-rl.done:
		return false
	}
}

func (rl *rateLimiter) stop() {
	close(rl.done)
	rl.ticker.Stop()
}

// Soak test worker
type soakWorker struct {
	t          *testing.T
	cluster    *harness.Cluster
	metrics    *soakMetrics
	storeIDs   []string
	storeIDsMu sync.RWMutex
	customerID string
	bodySize   int
	rng        *rand.Rand
	rngMu      sync.Mutex
	httpClient *http.Client
}

func newSoakWorker(t *testing.T, cluster *harness.Cluster, metrics *soakMetrics, bodySize int) *soakWorker {
	return &soakWorker{
		t:          t,
		cluster:    cluster,
		metrics:    metrics,
		storeIDs:   make([]string, 0, 10000),
		customerID: "soak-test-customer",
		bodySize:   bodySize,
		rng:        rand.New(rand.NewSource(42)), // fixed seed for reproducibility
		httpClient: &http.Client{Timeout: 2 * time.Second},
	}
}

func (w *soakWorker) randomBody() []byte {
	w.rngMu.Lock()
	defer w.rngMu.Unlock()
	body := make([]byte, w.bodySize)
	for i := range body {
		body[i] = byte('a' + w.rng.Intn(26))
	}
	return body
}

func (w *soakWorker) randomStoreID() string {
	w.storeIDsMu.RLock()
	defer w.storeIDsMu.RUnlock()
	if len(w.storeIDs) == 0 {
		return ""
	}
	w.rngMu.Lock()
	idx := w.rng.Intn(len(w.storeIDs))
	w.rngMu.Unlock()
	return w.storeIDs[idx]
}

func (w *soakWorker) addStoreID(id string) {
	w.storeIDsMu.Lock()
	w.storeIDs = append(w.storeIDs, id)
	w.storeIDsMu.Unlock()
}

func (w *soakWorker) removeStoreID(id string) {
	w.storeIDsMu.Lock()
	defer w.storeIDsMu.Unlock()
	for i, sid := range w.storeIDs {
		if sid == id {
			w.storeIDs = append(w.storeIDs[:i], w.storeIDs[i+1:]...)
			return
		}
	}
}

func (w *soakWorker) getPrimary() *harness.Node {
	if p := w.cluster.Primary(); p != nil {
		return p
	}
	// During failover, try node1 then node2
	if n := w.cluster.Node("node1"); n != nil && n.Replica.Role() == replica.RolePrimary {
		return n
	}
	if n := w.cluster.Node("node2"); n != nil && n.Replica.Role() == replica.RolePrimary {
		return n
	}
	return w.cluster.Node("node1") // fallback
}

func (w *soakWorker) doCreate() error {
	primary := w.getPrimary()
	if primary == nil {
		return fmt.Errorf("no primary available")
	}

	start := time.Now()
	body := w.randomBody()

	req, err := http.NewRequest("POST", "http://"+primary.Addr()+"/api/v1/create", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-Customer-ID", w.customerID)

	resp, err := w.httpClient.Do(req)
	dur := time.Since(start)

	if err != nil {
		w.metrics.recordRequest("create", dur, err)
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("create failed: %d", resp.StatusCode)
		w.metrics.recordRequest("create", dur, err)
		return err
	}

	storeID := string(respBody)
	w.addStoreID(storeID)
	w.metrics.recordRequest("create", dur, nil)
	return nil
}

func (w *soakWorker) doRead() error {
	storeID := w.randomStoreID()
	if storeID == "" {
		return nil // no stores yet
	}

	primary := w.getPrimary()
	if primary == nil {
		return fmt.Errorf("no primary available")
	}

	start := time.Now()
	req, err := http.NewRequest("POST", "http://"+primary.Addr()+"/api/v1/snapshot/"+storeID, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Customer-ID", w.customerID)

	resp, err := w.httpClient.Do(req)
	dur := time.Since(start)

	if err != nil {
		w.metrics.recordRequest("read", dur, err)
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		err = fmt.Errorf("read failed: %d", resp.StatusCode)
	}
	w.metrics.recordRequest("read", dur, err)
	return err
}

func (w *soakWorker) doModify() error {
	storeID := w.randomStoreID()
	if storeID == "" {
		return nil
	}

	primary := w.getPrimary()
	if primary == nil {
		return fmt.Errorf("no primary available")
	}

	// Begin modify
	start := time.Now()
	req, err := http.NewRequest("POST", "http://"+primary.Addr()+"/api/v1/begin-modify/"+storeID, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Customer-ID", w.customerID)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		w.metrics.recordRequest("modify", time.Since(start), err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("begin-modify failed: %d", resp.StatusCode)
		w.metrics.recordRequest("modify", time.Since(start), err)
		return err
	}

	lockID := resp.Header.Get("BigBunny-Lock-ID")
	io.Copy(io.Discard, resp.Body)

	// Complete modify with new body
	newBody := w.randomBody()
	req, err = http.NewRequest("POST", "http://"+primary.Addr()+"/api/v1/complete-modify/"+storeID, bytes.NewReader(newBody))
	if err != nil {
		return err
	}
	req.Header.Set("X-Customer-ID", w.customerID)
	req.Header.Set("BigBunny-Lock-ID", lockID)

	resp, err = w.httpClient.Do(req)
	dur := time.Since(start)

	if err != nil {
		w.metrics.recordRequest("modify", dur, err)
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("complete-modify failed: %d", resp.StatusCode)
	}
	w.metrics.recordRequest("modify", dur, err)
	return err
}

func (w *soakWorker) doDelete() error {
	storeID := w.randomStoreID()
	if storeID == "" {
		return nil
	}

	primary := w.getPrimary()
	if primary == nil {
		return fmt.Errorf("no primary available")
	}

	start := time.Now()
	req, err := http.NewRequest("POST", "http://"+primary.Addr()+"/api/v1/delete/"+storeID, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Customer-ID", w.customerID)

	resp, err := w.httpClient.Do(req)
	dur := time.Since(start)

	if err != nil {
		w.metrics.recordRequest("delete", dur, err)
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusOK {
		w.removeStoreID(storeID)
	} else if resp.StatusCode != http.StatusNotFound {
		err = fmt.Errorf("delete failed: %d", resp.StatusCode)
	}
	w.metrics.recordRequest("delete", dur, err)
	return err
}

// selectOp returns an operation based on workload mix:
// 70% read, 20% create, 5% modify, 5% delete
func (w *soakWorker) selectOp() string {
	w.rngMu.Lock()
	r := w.rng.Intn(100)
	w.rngMu.Unlock()

	switch {
	case r < 70:
		return "read"
	case r < 90:
		return "create"
	case r < 95:
		return "modify"
	default:
		return "delete"
	}
}

func (w *soakWorker) runOp() {
	op := w.selectOp()
	switch op {
	case "create":
		w.doCreate()
	case "read":
		w.doRead()
	case "modify":
		w.doModify()
	case "delete":
		w.doDelete()
	}
}

// TestSoak_SteadyStateCI is the CI-friendly soak test
func TestSoak_SteadyStateCI(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}

	cfg := ciSoakConfig()
	runSoakTest(t, cfg)
}

// TestSoak_FailoverFull is the full soak test with failover (requires SOAK=1)
func TestSoak_FailoverFull(t *testing.T) {
	if os.Getenv("SOAK") != "1" {
		t.Skip("skipping full soak test (set SOAK=1 to run)")
	}

	cfg := fullSoakConfig()
	runSoakTest(t, cfg)
}

func runSoakTest(t *testing.T, cfg soakConfig) {
	// Create cluster
	clusterCfg := &harness.ClusterConfig{
		NodeCount:          2,
		HeartbeatInterval:  100 * time.Millisecond,
		LeaseDuration:      500 * time.Millisecond,
		LeaseGrace:         500 * time.Millisecond,
		ReplicationTimeout: time.Second,
		ModifyTimeout:      200 * time.Millisecond,
		MemoryLimit:        cfg.MemoryLimit,
	}

	cluster, err := harness.NewCluster(clusterCfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Stop()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for roles to stabilize
	if err := cluster.WaitForRole("node1", replica.RolePrimary, 5*time.Second); err != nil {
		t.Fatalf("node1 didn't become primary: %v", err)
	}
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 5*time.Second); err != nil {
		t.Fatalf("node2 didn't become secondary: %v", err)
	}

	metrics := newSoakMetrics(10000)
	worker := newSoakWorker(t, cluster, metrics, cfg.BodySize)

	// Seed with some initial stores
	t.Log("seeding initial stores...")
	for i := 0; i < min(100, cfg.StoreCount/10); i++ {
		if err := worker.doCreate(); err != nil {
			t.Logf("seed create failed: %v", err)
		}
	}

	// Warmup phase
	t.Logf("warmup: %v at %d RPS", cfg.WarmupTime, cfg.TargetRPS/2)
	runLoadPhase(worker, cfg.TargetRPS/2, cfg.WarmupTime)

	// Reset metrics after warmup
	metrics = newSoakMetrics(10000)
	worker.metrics = metrics

	// Main load phase
	var failoverWg sync.WaitGroup
	if cfg.WithFailover {
		failoverWg.Add(1)
		go func() {
			defer failoverWg.Done()
			time.Sleep(cfg.FailoverDelay)
			t.Log("triggering failover: partitioning primary")
			cluster.Partition("node1", "node2")

			time.Sleep(cfg.FailoverDur)
			t.Log("healing partition")
			cluster.Heal("node1", "node2")
		}()
	}

	t.Logf("load: %v at %d RPS", cfg.LoadTime, cfg.TargetRPS)
	runLoadPhase(worker, cfg.TargetRPS, cfg.LoadTime)

	if cfg.WithFailover {
		failoverWg.Wait()
	}

	// Wait for replication to catch up
	t.Log("waiting for replication catch-up...")
	time.Sleep(2 * time.Second)

	// Verify cluster health
	t.Log("verifying cluster health...")

	// Check roles
	primary := cluster.Primary()
	if primary == nil {
		t.Error("no primary after test")
	}

	// Check store counts match (within tolerance)
	node1Count := cluster.Node("node1").Store.Count()
	node2Count := cluster.Node("node2").Store.Count()
	t.Logf("store counts: node1=%d node2=%d", node1Count, node2Count)

	if node1Count > 0 && node2Count > 0 {
		diff := node1Count - node2Count
		if diff < 0 {
			diff = -diff
		}
		lagPct := float64(diff) / float64(max(node1Count, node2Count))
		if lagPct > 0.01 { // 1% tolerance
			t.Errorf("replication lag too high: %d vs %d (%.1f%%)", node1Count, node2Count, lagPct*100)
		}
	}

	// Check no node stuck in JOINING
	for _, nodeID := range []string{"node1", "node2"} {
		role := cluster.Node(nodeID).Replica.Role()
		if role == replica.RoleJoining {
			t.Errorf("%s stuck in JOINING", nodeID)
		}
	}

	// Report metrics
	t.Logf("metrics: %s", metrics.summary())

	// Assert error rate
	maxErrorRate := 0.01 // 1% for CI
	if cfg.WithFailover {
		maxErrorRate = 0.05 // 5% with failover
	}
	if metrics.errorRate() > maxErrorRate {
		t.Errorf("error rate too high: %.2f%% > %.2f%%", metrics.errorRate()*100, maxErrorRate*100)
	}

	// Assert latency
	maxP99 := 500 * time.Millisecond
	if cfg.WithFailover {
		maxP99 = time.Second
	}
	p99 := metrics.percentile(0.99)
	if p99 > maxP99 {
		t.Errorf("p99 latency too high: %v > %v", p99, maxP99)
	}
}

func runLoadPhase(worker *soakWorker, rps int, duration time.Duration) {
	limiter := newRateLimiter(rps)
	defer limiter.stop()

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Worker pool
	numWorkers := min(rps, 50) // cap workers
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					if limiter.wait() {
						worker.runOp()
					} else {
						return
					}
				}
			}
		}()
	}

	time.Sleep(duration)
	close(done)
	wg.Wait()
}
