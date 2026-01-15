# Operations Guide

Running Big Bunny in production requires thinking about deployment architecture, monitoring, capacity planning, and handling the inevitable issues that come up. This guide covers the operational side of Big Bunny—how to deploy it, keep it healthy, and fix it when things go wrong.

## Deployment Architecture

The standard Big Bunny deployment uses two nodes per Point of Presence. One node acts as the primary, handling all writes and keeping the authoritative copy of your data. The other acts as secondary, receiving asynchronous updates and standing by to take over if the primary fails.

This two-node setup is deliberately simple. You don't need to coordinate between three, five, or seven nodes like you would with Raft or Paxos. You just need two nodes that talk to each other. The primary sends updates to the secondary asynchronously, which keeps write latency low. If the primary dies, the secondary detects this within about four seconds and promotes itself. Your application retries its requests, they get routed to the new primary, and life goes on.

Here's what a typical production deployment looks like. You'd start the first node like this:

```bash
./bbd \
  --host-id=node1 \
  --site=nyc01 \
  --tcp=:8081 \
  --uds=/var/run/bbd/bbd.sock \
  --peers=node2@10.0.1.2:8082 \
  --memory-limit=4294967296 \
  --customer-memory-quota=104857600 \
  --store-keys="$STORE_KEYS" \
  --store-key-current=0 \
  --internal-token="$INTERNAL_TOKEN" \
  --rate-limit=100 \
  --burst-size=200 \
  --tombstone-customer-limit=1000 \
  --tombstone-global-limit=10000
```

The second node gets almost identical configuration, just with a different host ID and adjusted network settings. Because `node1` sorts before `node2` alphabetically, node1 automatically becomes the primary. This deterministic election avoids the coordination overhead of leader election protocols.

If you're running in a non-critical environment where downtime is acceptable, you can run a single node. Just omit the `--peers` flag. Single-node deployments are simpler to manage but lose the failover benefit, so think carefully about whether that trade-off makes sense for your use case.

## Managing Configuration

In production, you'll want to manage configuration carefully. The encryption keys and internal token are sensitive—if someone gets them, they can decrypt your store IDs or inject fake replication traffic. Don't put them in version control, don't log them, and don't leave them lying around in shell history.

The cleanest approach is to use environment variables loaded from a secure file. Create a file that only the Big Bunny user can read:

```bash
# /etc/bigbunny/bbd.env
SERIALD_STORE_KEYS="0:a1b2c3d4e5f6..."
SERIALD_STORE_KEY_CURRENT=0
SERIALD_INTERNAL_TOKEN="f1e2d3c4b5a6..."
```

Set restrictive permissions:

```bash
chmod 600 /etc/bigbunny/bbd.env
chown bbd:bbd /etc/bigbunny/bbd.env
```

Then load these when starting the daemon. If you're using systemd, add `EnvironmentFile=/etc/bigbunny/bbd.env` to your unit file.

For larger deployments, integrate with a proper secrets management system like HashiCorp Vault or AWS Secrets Manager. Have Big Bunny retrieve its keys at startup rather than loading them from files. This gives you audit trails, automatic rotation, and other security features that become important as you scale.

## Process Management with systemd

Most Linux distributions use systemd for process management. Here's a basic unit file for Big Bunny:

```ini
[Unit]
Description=Big Bunny Session Store
After=network.target

[Service]
Type=simple
User=bbd
Group=bbd
EnvironmentFile=/etc/bigbunny/bbd.env
ExecStart=/usr/local/bin/bbd \
  --host-id=%H \
  --site=prod \
  --tcp=:8081 \
  --uds=/var/run/bbd/bbd.sock \
  --peers=peer@10.0.1.2:8082 \
  --memory-limit=4294967296

Restart=always
RestartSec=5s

LimitNOFILE=65536
MemoryMax=5G

NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

The `%H` in `--host-id` expands to the hostname, which is convenient if you're using the same unit file across multiple hosts. The `Restart=always` means systemd will restart Big Bunny if it crashes, though you should investigate why it crashed rather than relying on automatic restarts to paper over problems.

Enable and start the service:

```bash
sudo systemctl enable bbd
sudo systemctl start bbd
```

When you need to stop it, systemd sends SIGTERM and waits for the process to shut down gracefully. Big Bunny handles this by stopping new requests, draining in-flight operations, and flushing replication queues. This usually takes a few seconds. If it takes longer than 90 seconds, systemd sends SIGKILL, which is less graceful but ensures the process doesn't hang forever.

## Monitoring

You need visibility into what Big Bunny is doing. The basic tool is the status endpoint, which returns a JSON object describing the node's current state:

```bash
curl --unix-socket /var/run/bbd/bbd.sock http://localhost/status | jq .
```

This shows you the node's role (primary, secondary, or joining), its epoch number (which increments on each failover), how many stores it's managing, how much memory it's using, and information about its replication queues.

The key metrics to watch are the store count, memory usage, and replication queue depths. If the store count is growing faster than expected, maybe you have a memory leak or clients aren't cleaning up expired stores. If memory usage approaches your configured limit, you're about to start rejecting new stores. If the replication queue depth is growing, replication is falling behind writes, which increases the data loss window on failover.

For production monitoring, you'll want to export these metrics to Prometheus or your monitoring system of choice. Big Bunny doesn't include a Prometheus exporter in the current version, but the status endpoint returns JSON that's easy to parse. Write a small sidecar that polls the status endpoint and exports metrics in Prometheus format.

The metrics you definitely want to track:

The node's role tells you if it's currently primary, secondary, or recovering (joining). If you have two nodes and neither is primary, something has gone very wrong. If both are primary, you have split-brain and need to investigate immediately.

The epoch number increments on every failover. If you see this changing frequently, you have instability. Maybe the nodes can't reliably communicate with each other, maybe there's clock skew causing premature promotions, or maybe the primary is crashing repeatedly.

Store count and memory usage tell you about capacity. You want to trend these over time to predict when you'll need more capacity. If you hit your memory limit, new stores get rejected, which is bad for users but at least doesn't crash the system.

Replication queue depths show how far behind replication is. Under normal conditions, these should be small (zero or low single digits). If they grow, writes are coming in faster than replication can keep up. This might be transient during a burst of writes, or it might indicate a sustained problem like network congestion or a slow secondary.

## Alerting

Set up alerts for conditions that require human attention. Here are the critical ones.

If both nodes in a cluster are down, you've lost your data. You need to know about this immediately. Alert if you can't reach any Big Bunny node in a cluster.

If no node is acting as primary, writes are failing. This happens during the brief window of a failover, but if it persists beyond five seconds, something is wrong. Alert if no primary is present for more than five seconds.

If memory usage exceeds 95% of the configured limit, you're about to start rejecting creates. Alert so you can either increase the limit, clean up expired stores, or add capacity.

For less urgent issues, set up warnings. If the replication queue depth exceeds 100 messages, something is slowing down replication. This might resolve itself, but you should investigate. If the epoch number changes more than once in five minutes, you have frequent failovers, which suggests instability.

If you see repeated replication failures (the `replication_fail_count` metric), the secondary isn't receiving updates. This is tolerable briefly during network hiccups, but if it persists, you're operating without a backup and need to investigate.

## Capacity Planning

Big Bunny keeps everything in RAM, so capacity planning is mostly about memory. Each store needs roughly 2.5KB total—2KB for the body (the maximum size) plus 256 bytes for metadata overhead. If you configure a 4GB memory limit, you can store about 1.6 million stores per node.

Remember that stores are replicated. With replication factor 2, you're storing two copies of everything. Your million stores are spread across both nodes, so you have about 500,000 unique stores with each one replicated once.

For network bandwidth, estimate your replication traffic. If you're doing 100 writes per second and the average store size is 2KB, that's 200KB/sec or about 1.6 Mbps of replication traffic. This is minimal and shouldn't strain any reasonable network.

Your CPU usage depends on request rate. Big Bunny is fast because it's simple. Reads and writes are in-memory operations with no disk I/O. Unless you're doing millions of operations per second, you're unlikely to be CPU-bound.

The real constraint is usually memory. Watch your memory usage trends. If you're growing at 100MB per day and you have 2GB free, you have roughly 20 days before you hit your limit. Plan to expand capacity before you hit the limit, not after.

## Backup and Recovery

Here's the uncomfortable truth: Big Bunny doesn't really do backups. It's in-memory storage designed for ephemeral session data. If both nodes fail simultaneously, your data is gone. This is a deliberate design choice trading durability for speed.

For applications that can't tolerate this data loss, Big Bunny might not be the right tool. But for session storage, where data naturally expires after hours or days, this is usually acceptable. If a user's shopping cart disappears because both replicas failed, they'll be annoyed, but they can just start over. It's not ideal, but it's not catastrophic.

The way you "back up" Big Bunny is by keeping your stores short-lived with appropriate TTLs. If stores expire after an hour, the maximum data loss is an hour's worth of data. If stores expire after a day, you can lose up to a day of data. Set TTLs based on how much loss you can tolerate.

For disaster recovery, the procedure is simple: restart the nodes. They'll come up with empty state, elect a primary, and start serving requests. Clients will get "not found" errors for their old store IDs and create new stores. It's not seamless, but it works.

## Planned Maintenance

Sometimes you need to restart Big Bunny for upgrades or configuration changes. With a two-node cluster, you can do this with minimal disruption using a rolling restart.

First, figure out which node is the secondary. Check the status endpoint on both nodes to see their roles. Once you know which is secondary, restart it first.

```bash
sudo systemctl restart bbd
```

The secondary will go through its recovery process, fetching a snapshot from the primary and catching up on any missed updates. This usually takes a few seconds. Wait until it's back in secondary role before proceeding.

Now you can restart the primary. Clients will see a brief period of unavailability (about four seconds while the secondary detects the failure and promotes itself), but then requests start working again against the new primary.

If you're more careful, you can force a promotion before restarting the primary. On the secondary node, run:

```bash
./bbd promote -uds=/var/run/bbd/bbd.sock
```

This immediately promotes the secondary to primary. Now you can restart the old primary without any client-visible downtime, because requests are already going to the new primary.

## Troubleshooting

When things go wrong, you need to diagnose the problem quickly. Here are the common issues and how to recognize and fix them.

### Split-Brain

Split-brain means both nodes think they're primary. This can happen during a network partition—each node thinks the other is dead and promotes itself. When the network heals, you have two primaries.

You can detect split-brain by checking the role on both nodes. If both show `primary`, you have split-brain.

To resolve it, check the epoch numbers. The node with the higher epoch is the "correct" primary. Restart the node with the lower epoch. It will come back as secondary and recover from the higher-epoch node.

If the epochs are equal, you have a weirder situation. Pick one node to keep as primary and restart the other. The restarted node will become secondary.

To prevent split-brain, make sure nodes have synchronized clocks (use NTP) and reliable network connectivity. Most split-brain scenarios come from clock skew or flaky networks.

### Replication Lag

If the replication queue is growing, replication is falling behind. Check the queue length in the status output. If it's more than a few hundred, something is wrong.

Common causes include network congestion (check your network), a slow secondary (check CPU and memory on the secondary), or an overwhelmingly high write rate (check your request logs).

For network issues, check if you can reach the secondary from the primary. Try telnet or curl to the secondary's TCP port. If the network is flaky, fix that first.

For a slow secondary, check system resources. Is it swapping? Is the CPU maxed out? Is something else on the same machine consuming resources?

For high write rate, you might just be pushing more traffic than the system can handle. Consider whether you actually need all those writes, or if you can batch or rate-limit them.

### Memory Exhaustion

When Big Bunny hits its memory limit, it stops accepting operations that would increase memory usage. Existing stores keep working fine, but creates and updates that grow store size return `CapacityExceeded` errors.

Check memory usage in the status output. If you're at or near your limit, you have a few options. The immediate fix is to raise the memory limit and restart. If you set `--memory-limit=4294967296` (4GB), try `--memory-limit=8589934592` (8GB).

The longer-term fix is to reduce how much memory you're using. Are stores expiring appropriately? Check your TTLs. If stores are living too long, lower the default TTL or encourage clients to set shorter TTLs.

Are you creating more stores than you expected? Look at your usage patterns. Maybe clients are creating stores unnecessarily, or maybe you need more capacity than you thought.

### Customer Quota Exceeded

If individual customers are hitting `CustomerQuotaExceeded` errors (HTTP 507), they've exceeded their per-customer memory allocation. This is different from global memory exhaustion—other customers can still operate normally. The quota applies to all operations that increase memory usage: creates, updates, complete-modify, and counter mutations.

Check which customers are hitting limits and why. Are they creating more stores than expected? Are their stores larger than typical? Are updates growing store sizes over time? Sometimes this indicates a bug in the client application (forgetting to delete old stores, or stores growing unbounded).

If the quota is legitimately too low for your workload, increase it:

```bash
# Increase to 200 MB per customer
./bbd --customer-memory-quota=209715200
```

If per-customer quotas are causing problems and you trust all your customers, you can disable them:

```bash
# Disable per-customer quotas
./bbd --customer-memory-quota=0
```

### Tombstone Limit Exceeded

If delete operations are returning `TombstoneLimitExceeded` errors (HTTP 429), a customer has deleted too many stores in a short period. Tombstones are retained for 24 hours to prevent resurrection during replication, and limits prevent memory exhaustion from rapid create/delete cycles.

This is usually a sign of either abusive behavior (intentional rapid delete cycles) or a bug in the client application. Investigate the source of the deletes before adjusting limits.

If the limit is too low for legitimate workloads:

```bash
# Increase limits
./bbd --tombstone-customer-limit=5000 --tombstone-global-limit=50000
```

Tombstones are cleaned up automatically after 24 hours, so affected customers just need to wait. If this is a persistent problem, consider whether the application architecture is creating and deleting stores too frequently.

### Rate Limit Errors

If clients are receiving `429 Too Many Requests` responses with `Retry-After: 1` headers, they're hitting the per-customer rate limit. By default, Big Bunny allows 100 requests per second per customer with a burst capacity of 200.

Check if the rate limit is appropriate for your workload. If you have legitimate high-traffic customers, you may need to raise the limits:

```bash
# Increase to 500 req/s per customer with 1000 burst
./bbd --rate-limit=500 --burst-size=1000
```

If rate limiting is causing problems in a trusted environment where abuse isn't a concern, you can disable it entirely:

```bash
# Disable rate limiting
./bbd --rate-limit=0
```

For production, investigate whether the traffic is legitimate or abusive. Check your application logs to see which customers are hitting the limit. Legitimate high-volume customers may need higher limits, while abusive clients should be handled at the application layer (blocking, warning, etc.) before their requests reach Big Bunny.

The rate limiter uses a token bucket algorithm, so burst traffic is allowed. A customer can send 200 requests immediately, then sustain 100 requests per second. If clients are hitting limits during normal operation, they may need to implement backoff and retry logic that respects the `Retry-After` header.

### Stuck Locks

If all operations on a store fail with `StoreLocked` and it's been more than 500 milliseconds, the lock is stuck. This shouldn't happen normally, but bugs exist.

The lock should automatically expire after 500 milliseconds. If it doesn't, that's a bug. As a workaround, you can force-release the lock:

```bash
./bbd release-lock -uds=/var/run/bbd/bbd.sock <store-id>
```

This bypasses the normal lock checking and clears the lock unconditionally. Only use this when you're sure the lock is genuinely stuck and not legitimately held.

### Node Won't Promote

If the primary dies but the secondary stays as secondary, something is preventing promotion. Check the logs on the secondary for messages about why it won't promote.

The most common reason is that the lease hasn't expired yet. Big Bunny waits four seconds after losing contact with the primary before promoting. This is intentional—it prevents false promotions during transient network issues. Wait the full four seconds before worrying.

If it's been longer than four seconds and there's still no promotion, check the logs. You might see messages about clock skew, inability to increment the epoch, or other issues.

As a last resort, you can force promotion:

```bash
./bbd promote -uds=/var/run/bbd/bbd.sock
```

This immediately promotes the node, bypassing the lease checks. Use this only in emergencies when you're sure the old primary is really dead.

## Security Hardening

The basic security model assumes you're running on a trusted network. The internal replication traffic uses a shared secret token but isn't encrypted. If you're on an untrusted network, this isn't enough.

For production deployments on untrusted networks, you'd want mTLS for the internal replication endpoints. Big Bunny doesn't currently support this, but the architecture is ready for it. You'd configure certificate paths and have nodes verify each other's certificates before accepting replication traffic.

For the Unix socket, the default permissions (0600) restrict access to the owner. If you need multiple users to access it, use group permissions rather than world-readable:

```bash
chmod 660 /var/run/bbd/bbd.sock
chown bbd:app-users /var/run/bbd/bbd.sock
```

Anyone in the `app-users` group can now access the API, but it's still not world-accessible.

The network security matters too. Put your Big Bunny nodes on a private network and use firewall rules to restrict who can reach the TCP replication ports. Only peer nodes should be able to connect.

## Performance Tuning

Big Bunny is designed to be fast out of the box, so there isn't much tuning to do. The main knobs are memory limits and lock timeouts.

Memory limits control capacity. Set them based on how many stores you need to support. Each store needs roughly 2.5KB (2KB max body plus 256 bytes overhead), so divide your available memory by 2.5KB to get your store capacity.

Lock timeouts are currently fixed at 500 milliseconds. This is a good default for most workloads. If your modify operations are slower than this (maybe you're doing complex processing), you'll see lock timeout errors. Unfortunately, there's no way to adjust this without modifying the code. If this becomes a problem, file an issue.

The async replication is already tuned for good performance. Messages are batched automatically when under heavy load, and the queue provides natural backpressure. You shouldn't need to tune anything here.

## What to Do When Things Break

When Big Bunny breaks in production, stay calm and work through the problem systematically. Start by checking the status endpoint on both nodes. This tells you who's primary, what the epoch is, and what the queue depths look like.

Check the logs. Big Bunny logs errors and warnings that usually point you in the right direction. Look for messages about replication failures, promotion events, or error handling.

If one node is down, the other should have promoted itself. If that didn't happen, check the logs on the remaining node to see why. If both nodes are down, check what caused them to crash. Out of memory? Kernel panic? Network isolation?

If you have split-brain (both nodes primary), resolve it by restarting the lower-epoch node. If replication is backed up, investigate why. If locks are stuck, force-release them as a last resort.

Most problems come down to network issues, clock skew, or resource exhaustion. Check your network connectivity, verify clocks are synchronized, and make sure you're not out of memory or CPU.

## Getting Help

The [Architecture](architecture.md) document explains how Big Bunny works internally, which is helpful for understanding failure modes. The [Security](security.md) guide covers the security model and threat analysis. And the [API Reference](api-reference.md) documents every endpoint and error code in detail.

If you're stuck, gather the following information before asking for help: logs from both nodes, status output from both nodes, description of what you were doing when things broke, and any relevant configuration. This gives whoever is helping you enough context to diagnose the problem quickly.
