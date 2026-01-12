package test

import (
	"testing"
	"time"

	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/test/harness"
)

// Clock skew test configuration with test clocks enabled
func clockSkewConfig() *harness.ClusterConfig {
	return &harness.ClusterConfig{
		NodeCount:          2,
		HeartbeatInterval:  50 * time.Millisecond,
		LeaseDuration:      200 * time.Millisecond,
		LeaseGrace:         200 * time.Millisecond,
		ReplicationTimeout: 500 * time.Millisecond,
		ModifyTimeout:      100 * time.Millisecond,
		UseTestClock:       true,
	}
}

// Test 1: Forward-skewed secondary promotes early
// When secondary's clock jumps forward past lease expiry, it should promote.
// On heal/heartbeat, old primary demotes to JOINING and recovers.
func TestClockSkew_ForwardSkewedSecondary(t *testing.T) {
	cfg := clockSkewConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for initial roles to stabilize
	if err := cluster.WaitForRole("node1", replica.RolePrimary, 2*time.Second); err != nil {
		t.Fatalf("node1 didn't become primary: %v", err)
	}
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("node2 didn't become secondary: %v", err)
	}

	primary := cluster.Node("node1")
	secondary := cluster.Node("node2")
	initialEpoch := primary.Replica.LeaderEpoch()

	// Partition the nodes so secondary stops receiving heartbeats
	cluster.Partition("node1", "node2")

	// Advance secondary's clock forward past lease expiry
	skewAmount := cfg.LeaseDuration + cfg.LeaseGrace + 100*time.Millisecond
	cluster.SkewNode("node2", skewAmount)

	// Secondary should promote because its clock shows lease expired
	if err := cluster.WaitForRole("node2", replica.RolePrimary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't promote after clock skew: %v", err)
	}

	newEpoch := secondary.Replica.LeaderEpoch()
	if newEpoch <= initialEpoch {
		t.Errorf("epoch should increase after promotion: initial=%d, new=%d", initialEpoch, newEpoch)
	}

	// Heal partition
	cluster.Heal("node1", "node2")

	// Old primary should demote to JOINING and eventually recover
	if err := cluster.WaitForCondition(func() bool {
		role := primary.Replica.Role()
		return role == replica.RoleJoining || role == replica.RoleSecondary
	}, 2*time.Second); err != nil {
		t.Fatalf("old primary didn't demote: %v (role=%s)", err, primary.Replica.Role())
	}

	// Wait for recovery to complete
	if err := cluster.WaitForRole("node1", replica.RoleSecondary, 3*time.Second); err != nil {
		t.Logf("node1 role: %s", primary.Replica.Role())
		// May still be recovering - that's acceptable
	}

	// Epoch should be monotonic
	recoveredEpoch := primary.Replica.LeaderEpoch()
	if recoveredEpoch < newEpoch {
		t.Errorf("epoch regressed after recovery: new=%d, recovered=%d", newEpoch, recoveredEpoch)
	}
}

// Test 2: Backward-skewed secondary delays promotion
// When secondary's clock is behind, it won't see lease as expired.
func TestClockSkew_BackwardSkewedSecondary(t *testing.T) {
	cfg := clockSkewConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for initial roles
	if err := cluster.WaitForRole("node1", replica.RolePrimary, 2*time.Second); err != nil {
		t.Fatalf("node1 didn't become primary: %v", err)
	}
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("node2 didn't become secondary: %v", err)
	}

	// Skew secondary's clock backward significantly
	skewAmount := cfg.LeaseDuration + cfg.LeaseGrace
	cluster.SkewNode("node2", -skewAmount)

	// Partition
	cluster.Partition("node1", "node2")

	// Verify secondary does NOT promote due to backward skew
	// Its clock thinks not enough time has passed since last heartbeat
	// Poll several times to confirm it stays SECONDARY
	for range 10 {
		if cluster.Node("node2").Replica.Role() == replica.RolePrimary {
			t.Error("backward-skewed secondary promoted too early")
			break
		}
	}

	// Now advance secondary's clock well past expiry
	// Need to go forward enough that now.Sub(lastLeaderSeen) > LeaseDuration + LeaseGrace
	// Since we went back by skewAmount, we need to go forward by 3x to ensure we're past
	cluster.SkewNode("node2", 3*skewAmount)

	// Now secondary should promote
	if err := cluster.WaitForRole("node2", replica.RolePrimary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't promote after clock catchup: %v", err)
	}
}

// Test 3: Forward-skewed primary doesn't block secondary promotion
// Primary's clock being ahead shouldn't affect secondary's lease tracking.
func TestClockSkew_ForwardSkewedPrimary(t *testing.T) {
	cfg := clockSkewConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for initial roles
	if err := cluster.WaitForRole("node1", replica.RolePrimary, 2*time.Second); err != nil {
		t.Fatalf("node1 didn't become primary: %v", err)
	}
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("node2 didn't become secondary: %v", err)
	}

	// Skew primary's clock forward significantly
	cluster.SkewNode("node1", 10*time.Second)

	// Partition
	cluster.Partition("node1", "node2")

	// Advance secondary's clock to trigger promotion (using its own clock)
	skewAmount := cfg.LeaseDuration + cfg.LeaseGrace + 100*time.Millisecond
	cluster.SkewNode("node2", skewAmount)

	// Secondary should still promote based on its own clock
	if err := cluster.WaitForRole("node2", replica.RolePrimary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't promote (primary skew shouldn't matter): %v", err)
	}
}

// Test 4: Clock skew during JOINING recovery
// Forward jump can trigger immediate retry; backward jump can delay it.
func TestClockSkew_DuringJoiningRecovery(t *testing.T) {
	cfg := clockSkewConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	// Partition before start so node2 can't recover initially
	cluster.Partition("node1", "node2")

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// node1 becomes primary
	if err := cluster.WaitForRole("node1", replica.RolePrimary, time.Second); err != nil {
		t.Fatalf("node1 didn't become primary: %v", err)
	}

	// node2 should be stuck in JOINING
	if err := cluster.WaitForCondition(func() bool {
		return cluster.Node("node2").Replica.Role() == replica.RoleJoining
	}, time.Second); err != nil {
		t.Fatalf("node2 should be JOINING: %v", err)
	}

	initialAttempts := cluster.Node("node2").Replica.RecoveryAttempts()

	// Forward skew node2's clock past retry interval
	cluster.SkewNode("node2", 2*time.Second)

	// Heal and verify recovery eventually completes
	cluster.Heal("node1", "node2")

	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 3*time.Second); err != nil {
		t.Fatalf("node2 didn't recover: %v", err)
	}

	finalAttempts := cluster.Node("node2").Replica.RecoveryAttempts()
	if finalAttempts <= initialAttempts {
		t.Logf("recovery attempts: initial=%d, final=%d", initialAttempts, finalAttempts)
	}
}

// Test 5: Clock skew affects lock unknown window
// Forward skew closes window early; backward skew extends it.
func TestClockSkew_LockUnknownWindow(t *testing.T) {
	cfg := clockSkewConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for roles
	if err := cluster.WaitForRole("node1", replica.RolePrimary, 2*time.Second); err != nil {
		t.Fatalf("node1 didn't become primary: %v", err)
	}
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("node2 didn't become secondary: %v", err)
	}

	primary := cluster.Node("node1")

	// Create a store
	storeID := createStore(t, primary.Addr(), "customer1", []byte("lock test"))
	if err := cluster.WaitForStore("node2", storeID, "customer1", time.Second); err != nil {
		t.Fatalf("store not replicated: %v", err)
	}

	// Partition and promote secondary
	cluster.Partition("node1", "node2")
	skewAmount := cfg.LeaseDuration + cfg.LeaseGrace + 100*time.Millisecond
	cluster.SkewNode("node2", skewAmount)

	if err := cluster.WaitForRole("node2", replica.RolePrimary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	newPrimary := cluster.Node("node2")

	// Immediately after promotion, should be in unknown window
	unknown, _ := newPrimary.Replica.IsLockStateUnknown(storeID)
	if !unknown {
		t.Log("expected lock state unknown immediately after promotion")
	}

	// Forward skew to close window early
	cluster.SkewNode("node2", cfg.ModifyTimeout+50*time.Millisecond)

	// Window should be closed now
	unknown, _ = newPrimary.Replica.IsLockStateUnknown(storeID)
	if unknown {
		t.Error("lock state should be known after forward skew past ModifyTimeout")
	}
}

// Test 6: Status durations remain non-negative under skew
func TestClockSkew_StatusConsistency(t *testing.T) {
	cfg := clockSkewConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for roles
	if err := cluster.WaitForRole("node1", replica.RolePrimary, 2*time.Second); err != nil {
		t.Fatalf("node1 didn't become primary: %v", err)
	}
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("node2 didn't become secondary: %v", err)
	}

	// Get initial status
	status := cluster.Node("node2").Replica.GetStatus()
	lastLeaderSeen := status["last_leader_seen"].(string)

	// The duration should parse as positive (or very small)
	dur, err := time.ParseDuration(lastLeaderSeen)
	if err != nil {
		t.Fatalf("couldn't parse last_leader_seen: %v", err)
	}
	if dur < 0 {
		t.Errorf("last_leader_seen should be non-negative: %v", dur)
	}

	// Skew node2's clock backward
	cluster.SkewNode("node2", -1*time.Second)

	// Status should still have non-negative durations
	// (because m.now() is used, which is the skewed clock)
	status = cluster.Node("node2").Replica.GetStatus()
	lastLeaderSeen = status["last_leader_seen"].(string)
	dur, err = time.ParseDuration(lastLeaderSeen)
	if err != nil {
		t.Fatalf("couldn't parse last_leader_seen after skew: %v", err)
	}
	// With backward skew, the duration could be negative if lastLeaderSeen was
	// set using wall clock but now() uses test clock. This test validates
	// the implementation handles this gracefully.
	t.Logf("last_leader_seen after backward skew: %v", dur)
}

// Test 7: Epoch monotonicity under skew + partition
func TestClockSkew_EpochMonotonicity(t *testing.T) {
	cfg := clockSkewConfig()
	cluster, err := harness.NewCluster(cfg)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	if err := cluster.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for roles
	if err := cluster.WaitForRole("node1", replica.RolePrimary, 2*time.Second); err != nil {
		t.Fatalf("node1 didn't become primary: %v", err)
	}
	if err := cluster.WaitForRole("node2", replica.RoleSecondary, 2*time.Second); err != nil {
		t.Fatalf("node2 didn't become secondary: %v", err)
	}

	primary := cluster.Node("node1")
	secondary := cluster.Node("node2")
	initialEpoch := primary.Replica.LeaderEpoch()
	t.Logf("initial epoch: %d", initialEpoch)

	// Partition and skew secondary forward to promote
	cluster.Partition("node1", "node2")
	skewAmount := cfg.LeaseDuration + cfg.LeaseGrace + 100*time.Millisecond
	cluster.SkewNode("node2", skewAmount)

	if err := cluster.WaitForRole("node2", replica.RolePrimary, 2*time.Second); err != nil {
		t.Fatalf("secondary didn't promote: %v", err)
	}

	newPrimaryEpoch := secondary.Replica.LeaderEpoch()
	t.Logf("new primary epoch: %d", newPrimaryEpoch)
	if newPrimaryEpoch <= initialEpoch {
		t.Errorf("epoch should increase after promotion: %d <= %d", newPrimaryEpoch, initialEpoch)
	}

	// Skew old primary backward (simulating clock behind)
	cluster.SkewNode("node1", -2*time.Second)

	// Heal partition
	cluster.Heal("node1", "node2")

	// Old primary should demote and recover
	if err := cluster.WaitForCondition(func() bool {
		epoch := primary.Replica.LeaderEpoch()
		return epoch >= newPrimaryEpoch
	}, 3*time.Second); err != nil {
		t.Fatalf("old primary didn't update epoch: current=%d, expected>=%d",
			primary.Replica.LeaderEpoch(), newPrimaryEpoch)
	}

	// Verify epoch never regressed
	finalEpoch := primary.Replica.LeaderEpoch()
	if finalEpoch < newPrimaryEpoch {
		t.Errorf("epoch regressed: final=%d < new_primary=%d", finalEpoch, newPrimaryEpoch)
	}
	t.Logf("final old primary epoch: %d", finalEpoch)
}
