package harness

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dip-proto/bigbunny/internal/api"
	"github.com/dip-proto/bigbunny/internal/auth"
	"github.com/dip-proto/bigbunny/internal/registry"
	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
)

// TestClock provides a controllable clock for deterministic tests. It wraps a
// single time value that can be advanced or set manually, letting you simulate
// time passage without waiting for real time to elapse.
type TestClock struct {
	mu   sync.RWMutex
	time time.Time
}

// NewTestClock creates a TestClock initialized to the given start time.
func NewTestClock(start time.Time) *TestClock {
	return &TestClock{time: start}
}

// Now returns the current time according to this clock.
func (c *TestClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.time
}

// Advance moves the clock forward by the given duration.
func (c *TestClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.time = c.time.Add(d)
}

// Set overwrites the clock's current time with the given value.
func (c *TestClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.time = t
}

// NetworkSim simulates network partitions and latency between nodes. You can
// block traffic between specific source and destination pairs, or inject
// artificial latency to test timeout and retry behavior.
type NetworkSim struct {
	mu        sync.RWMutex
	blocked   map[string]bool // "src->dst" -> blocked
	latency   map[string]time.Duration
	transport http.RoundTripper
}

// NewNetworkSim creates a NetworkSim with no partitions or latency configured.
func NewNetworkSim() *NetworkSim {
	return &NetworkSim{
		blocked:   make(map[string]bool),
		latency:   make(map[string]time.Duration),
		transport: http.DefaultTransport,
	}
}

func (n *NetworkSim) key(src, dst string) string {
	return src + "->" + dst
}

// Block prevents traffic from src to dst.
func (n *NetworkSim) Block(src, dst string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[n.key(src, dst)] = true
}

// BlockBidirectional blocks traffic in both directions.
func (n *NetworkSim) BlockBidirectional(a, b string) {
	n.Block(a, b)
	n.Block(b, a)
}

// Heal restores traffic from src to dst.
func (n *NetworkSim) Heal(src, dst string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.blocked, n.key(src, dst))
}

// HealBidirectional restores traffic in both directions.
func (n *NetworkSim) HealBidirectional(a, b string) {
	n.Heal(a, b)
	n.Heal(b, a)
}

// HealAll clears all partitions.
func (n *NetworkSim) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = make(map[string]bool)
	n.latency = make(map[string]time.Duration)
}

// SetLatency adds latency for traffic from src to dst.
func (n *NetworkSim) SetLatency(src, dst string, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.latency[n.key(src, dst)] = d
}

// IsBlocked checks if traffic from src to dst is blocked.
func (n *NetworkSim) IsBlocked(src, dst string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.blocked[n.key(src, dst)]
}

// GetLatency returns the configured latency for traffic from src to dst.
func (n *NetworkSim) GetLatency(src, dst string) time.Duration {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.latency[n.key(src, dst)]
}

// NodeTransport is an http.RoundTripper that routes requests through the
// NetworkSim. It checks for partitions and applies latency before forwarding
// the request, making it possible to test how nodes behave under network failures.
type NodeTransport struct {
	nodeID  string
	network *NetworkSim
	nodes   map[string]*Node // address -> node for destination lookup
}

// RoundTrip implements http.RoundTripper by checking the NetworkSim for
// partitions and latency before delegating to the default transport.
func (t *NodeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Find destination node by address
	destAddr := req.URL.Host
	var destID string
	for _, node := range t.nodes {
		if node.Addr() == destAddr {
			destID = node.ID
			break
		}
	}

	if destID != "" {
		// Check if blocked
		if t.network.IsBlocked(t.nodeID, destID) {
			return nil, fmt.Errorf("network partition: %s -> %s blocked", t.nodeID, destID)
		}

		// Apply latency
		if latency := t.network.GetLatency(t.nodeID, destID); latency > 0 {
			time.Sleep(latency)
		}
	}

	return http.DefaultTransport.RoundTrip(req)
}

// Node represents a single bbd node in the test cluster. It holds all the
// components that make up a running node: the store manager, registry, replica
// manager, API server, and HTTP server. The Clock field lets you simulate clock
// skew for that specific node.
type Node struct {
	ID         string
	Store      *store.Manager
	Registry   *registry.Manager
	Replica    *replica.Manager
	API        *api.Server
	HTTPServer *http.Server
	Clock      *TestClock // per-node clock for skew simulation
	listener   net.Listener
	addr       string
	network    *NetworkSim
	allNodes   map[string]*Node
}

// Addr returns the TCP address the node is listening on.
func (n *Node) Addr() string {
	return n.addr
}

// Start begins serving HTTP requests and starts the replica manager's
// background goroutines.
func (n *Node) Start() error {
	n.Replica.Start()
	go n.HTTPServer.Serve(n.listener)
	return nil
}

// Stop gracefully shuts down the node, stopping replication and the HTTP server.
func (n *Node) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	n.Replica.Stop()
	return n.HTTPServer.Shutdown(ctx)
}

// Cluster manages a test cluster of bbd nodes. It provides helpers for
// starting and stopping the cluster, simulating network partitions, and
// waiting for replication or role changes to complete.
type Cluster struct {
	nodes        map[string]*Node
	clocks       map[string]*TestClock // per-node clocks
	network      *NetworkSim
	useTestClock bool
	mu           sync.RWMutex
}

// ClusterConfig holds configuration for creating a test cluster. The timing
// values control heartbeat intervals, lease durations, and timeouts. Setting
// UseTestClock to true enables per-node clock simulation for testing clock skew.
type ClusterConfig struct {
	NodeCount          int
	HeartbeatInterval  time.Duration
	LeaseDuration      time.Duration
	LeaseGrace         time.Duration
	ReplicationTimeout time.Duration
	ModifyTimeout      time.Duration
	UseTestClock       bool   // if true, use injectable TestClock per node
	MemoryLimit        int64  // per-node memory limit in bytes (0 = no limit)
	InternalToken      string // shared secret for internal endpoint auth (empty = no auth)
}

// DefaultClusterConfig returns a ClusterConfig with sensible defaults for fast
// tests: two nodes, short heartbeat intervals, and quick lease timeouts.
func DefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		NodeCount:          2,
		HeartbeatInterval:  50 * time.Millisecond,
		LeaseDuration:      200 * time.Millisecond,
		LeaseGrace:         200 * time.Millisecond,
		ReplicationTimeout: 500 * time.Millisecond,
		ModifyTimeout:      100 * time.Millisecond,
	}
}

// NewCluster creates a new test cluster according to the given configuration.
// It allocates TCP listeners on ephemeral ports, wires up the nodes with a
// shared NetworkSim, and returns the cluster ready to be started.
func NewCluster(cfg *ClusterConfig) (*Cluster, error) {
	if cfg == nil {
		cfg = DefaultClusterConfig()
	}

	network := NewNetworkSim()
	nodes := make(map[string]*Node)
	clocks := make(map[string]*TestClock)

	// Create listeners first to get addresses
	listeners := make([]net.Listener, cfg.NodeCount)
	addrs := make([]string, cfg.NodeCount)
	for i := 0; i < cfg.NodeCount; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			// Clean up already created listeners
			for j := 0; j < i; j++ {
				listeners[j].Close()
			}
			return nil, fmt.Errorf("failed to create listener for node %d: %w", i, err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}

	// Build host list for routing
	hosts := make([]*routing.Host, cfg.NodeCount)
	for i := 0; i < cfg.NodeCount; i++ {
		hosts[i] = &routing.Host{
			ID:      fmt.Sprintf("node%d", i+1),
			Address: addrs[i],
			Healthy: true,
		}
	}
	hasher := routing.NewRendezvousHasher(hosts, "test-secret")

	// Create nodes
	for i := 0; i < cfg.NodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)

		// Create per-node clock if test clock enabled
		var nodeClock *TestClock
		var nowFunc func() time.Time
		if cfg.UseTestClock {
			nodeClock = NewTestClock(time.Now())
			nowFunc = nodeClock.Now
			clocks[nodeID] = nodeClock
		}

		var storeMgr *store.Manager
		if cfg.MemoryLimit > 0 {
			storeMgr = store.NewManagerWithLimit(cfg.MemoryLimit)
		} else {
			storeMgr = store.NewManager()
		}
		registryMgr := registry.NewManager()

		// Create node transport (will be updated with node references after all nodes created)
		nodeTransport := &NodeTransport{
			nodeID:  nodeID,
			network: network,
			nodes:   nodes,
		}

		replicaCfg := &replica.Config{
			HostID:               nodeID,
			Site:                 "test",
			HeartbeatInterval:    cfg.HeartbeatInterval,
			LeaseDuration:        cfg.LeaseDuration,
			LeaseGrace:           cfg.LeaseGrace,
			ReplicationTimeout:   cfg.ReplicationTimeout,
			ModifyTimeout:        cfg.ModifyTimeout,
			HTTPClient:           &http.Client{Transport: nodeTransport, Timeout: cfg.ReplicationTimeout},
			BroadcastReplication: true, // Test mode: replicate to all hosts
			Now:                  nowFunc,
			InternalToken:        cfg.InternalToken,
		}
		replicaMgr := replica.NewManager(replicaCfg, storeMgr, hasher)
		replicaMgr.SetRegistry(registryMgr)

		// Use dev keys for test cipher
		cipher := auth.NewCipher(auth.DevKeySet())

		apiCfg := &api.Config{
			Site:                "test",
			HostID:              nodeID,
			DefaultTTL:          14 * 24 * time.Hour,
			MaxBodySize:         2 * 1024,
			MaxInternalBodySize: 4 * 1024,
			ModifyTimeout:       cfg.ModifyTimeout,
			Cipher:              cipher,
		}
		apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher, nil)

		mux := http.NewServeMux()
		apiServer.RegisterRoutes(mux)

		// Wrap with internal auth middleware if token configured
		var handler http.Handler = mux
		if cfg.InternalToken != "" {
			handler = internalAuthMiddleware(mux, cfg.InternalToken)
		}

		node := &Node{
			ID:         nodeID,
			Store:      storeMgr,
			Registry:   registryMgr,
			Replica:    replicaMgr,
			API:        apiServer,
			HTTPServer: &http.Server{Handler: handler},
			Clock:      nodeClock,
			listener:   listeners[i],
			addr:       addrs[i],
			network:    network,
			allNodes:   nodes,
		}
		nodes[nodeID] = node
	}

	// Update node transports with complete node map
	for _, node := range nodes {
		if transport, ok := node.Replica.GetClient().Transport.(*NodeTransport); ok {
			transport.nodes = nodes
		}
	}

	return &Cluster{
		nodes:        nodes,
		clocks:       clocks,
		network:      network,
		useTestClock: cfg.UseTestClock,
	}, nil
}

// Start brings up all nodes in the cluster. The node with the smallest ID
// becomes the primary, while other nodes begin recovery to sync state.
func (c *Cluster) Start() error {
	// Determine primary (first node in sorted order)
	primaryID := c.getPrimaryID()

	for id, node := range c.nodes {
		if err := node.Start(); err != nil {
			return fmt.Errorf("failed to start node %s: %w", id, err)
		}

		if id == primaryID {
			node.Replica.SetRole(replica.RolePrimary)
		} else {
			// Non-primary nodes start recovery
			go node.Replica.StartRecovery()
		}
	}
	return nil
}

// Stop gracefully shuts down all nodes in the cluster.
func (c *Cluster) Stop() error {
	var lastErr error
	for id, node := range c.nodes {
		if err := node.Stop(); err != nil {
			lastErr = fmt.Errorf("failed to stop node %s: %w", id, err)
		}
	}
	return lastErr
}

func (c *Cluster) getPrimaryID() string {
	// First node alphabetically is primary
	var first string
	for id := range c.nodes {
		if first == "" || id < first {
			first = id
		}
	}
	return first
}

// Node returns the node with the given ID, or nil if not found.
func (c *Cluster) Node(id string) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodes[id]
}

// Nodes returns a copy of the node map, keyed by node ID.
func (c *Cluster) Nodes() map[string]*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]*Node)
	for k, v := range c.nodes {
		result[k] = v
	}
	return result
}

// Primary returns the node currently acting as primary, or nil if none.
func (c *Cluster) Primary() *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, node := range c.nodes {
		if node.Replica.Role() == replica.RolePrimary {
			return node
		}
	}
	return nil
}

// Secondary returns a node currently acting as secondary, or nil if none.
func (c *Cluster) Secondary() *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, node := range c.nodes {
		if node.Replica.Role() == replica.RoleSecondary {
			return node
		}
	}
	return nil
}

// Network returns the NetworkSim used to simulate partitions and latency.
func (c *Cluster) Network() *NetworkSim {
	return c.network
}

// NodeClock returns the TestClock for a specific node.
// Returns nil if UseTestClock was false or node not found.
func (c *Cluster) NodeClock(nodeID string) *TestClock {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clocks[nodeID]
}

// SkewNode adjusts a node's clock by the given delta (can be positive or negative).
func (c *Cluster) SkewNode(nodeID string, delta time.Duration) {
	if clock := c.NodeClock(nodeID); clock != nil {
		clock.Advance(delta)
	}
}

// SetNodeTime sets a node's clock to a specific time.
func (c *Cluster) SetNodeTime(nodeID string, t time.Time) {
	if clock := c.NodeClock(nodeID); clock != nil {
		clock.Set(t)
	}
}

// AdvanceAll advances all node clocks by the same duration.
func (c *Cluster) AdvanceAll(d time.Duration) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, clock := range c.clocks {
		clock.Advance(d)
	}
}

// SyncClocks sets all node clocks to the same time.
func (c *Cluster) SyncClocks(t time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, clock := range c.clocks {
		clock.Set(t)
	}
}

// HasTestClocks returns true if per-node test clocks are enabled for this cluster.
func (c *Cluster) HasTestClocks() bool {
	return c.useTestClock
}

// WaitForRole polls until the given node reaches the specified role, or until
// the timeout expires. It returns an error if the node is not found or the
// timeout is exceeded.
func (c *Cluster) WaitForRole(nodeID string, role replica.Role, timeout time.Duration) error {
	node := c.Node(nodeID)
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if node.Replica.Role() == role {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for node %s to reach role %s (current: %s)",
		nodeID, role, node.Replica.Role())
}

// WaitForCondition polls the given check function until it returns true, or
// until the timeout expires. Use this instead of time.Sleep for more stable tests.
func (c *Cluster) WaitForCondition(check func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for condition")
}

// Partition blocks traffic in both directions between the two named nodes.
func (c *Cluster) Partition(a, b string) {
	c.network.BlockBidirectional(a, b)
}

// Heal restores traffic in both directions between the two named nodes.
func (c *Cluster) Heal(a, b string) {
	c.network.HealBidirectional(a, b)
}

// HealAll clears all partitions and latency settings from the network simulation.
func (c *Cluster) HealAll() {
	c.network.HealAll()
}

// WaitForStore polls until the specified store exists on the given node, which
// is useful for waiting for replication to complete after a write.
func (c *Cluster) WaitForStore(nodeID, storeID, customerID string, timeout time.Duration) error {
	node := c.Node(nodeID)
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := node.Store.Get(storeID, customerID)
		if err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for store %s on node %s", storeID, nodeID)
}

func internalAuthMiddleware(next http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/internal/") {
			providedToken := r.Header.Get("X-Internal-Token")
			if providedToken != token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
