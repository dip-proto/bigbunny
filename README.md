# ![Big Bunny](.media/logo.jpg)

A fast, in-memory session store built for edge computing. If you're running application servers at the edge and need somewhere to stash session data, user state, or temporary information with automatic replication and failover, Big Bunny has you covered.

## What It Does

Big Bunny gives you in-memory storage with microsecond-level latency for reads and writes. The lock-based modify protocol prevents race conditions when you need to read, change, and write back data. If a host fails, the secondary automatically takes over within about four seconds, so your application stays available even when hardware dies.

Store IDs are encrypted with AES-128-SIV, which provides both security and customer isolation. TTL management happens automatically—stores expire when their time is up, and the garbage collector cleans them up without you having to think about it.

**[Read the full documentation](doc/index.md)** — Complete guides covering installation, usage, operations, architecture, and security.

## Quick Start

Build the binary:

```bash
make
```

Start a single node in development mode:

```bash
./bbd --dev --host-id=node1 --tcp=:8081 --uds=/tmp/bbd.sock
```

Create a store:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-customer" \
  -H "BigBunny-Not-Valid-After: 3600" \
  -d "hello world" \
  http://localhost/api/v1/create
```

You'll get back an encrypted store ID like `v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9...`. Save that—you'll need it to read the store back:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-customer" \
  http://localhost/api/v1/snapshot/v1:0:8ahePLwi...
```

For a complete walkthrough, see the [Quick Start Guide](doc/quickstart.md).

## Key Features

- **In-Memory Storage** — Microsecond-level read/write latency
- **Lock-Based Serialization** — Prevents race conditions for read-modify-write operations
- **Automatic Failover** — Two-node replication with ~4 second failover time
- **Encrypted Store IDs** — AES-128-SIV encryption provides security and customer isolation
- **Named Stores** — Optional human-readable names instead of opaque IDs
- **TTL Management** — Automatic expiration and garbage collection
- **Unix Socket API** — Local access with filesystem-based permission control

## Architecture Overview

The architecture is deliberately simple. Two nodes per Point of Presence, with the primary handling writes and the secondary receiving asynchronous updates. When you write to the primary, it responds immediately and then replicates to the secondary in the background.

```
┌─────────────────────────────────────────────────┐
│                    Client                       │
│               (Application Server)              │
└───────────────────┬─────────────────────────────┘
                    │ Unix Socket (Local API)
                    ↓
┌─────────────────────────────────────────────────┐
│              Big Bunny Node 1 (Primary)         │
│  - In-memory store management                   │
│  - Lock-based serialization                     │
│  - Async replication queue                      │
└───────────────────┬─────────────────────────────┘
                    │ TCP (Replication + Heartbeat)
                    ↓
┌─────────────────────────────────────────────────┐
│             Big Bunny Node 2 (Secondary)        │
│  - Receives async updates                       │
│  - Monitors primary health                      │
│  - Promotes on lease expiry                     │
└─────────────────────────────────────────────────┘
```

Failover uses a lease-based mechanism. The primary sends heartbeats every 200 milliseconds. If the secondary doesn't hear from the primary for about four seconds, it promotes itself. The epoch number increments with each failover, preventing split-brain corruption when the network heals.

For a deep dive into how it all works, see the [Architecture Guide](doc/architecture.md).

## Production Deployment

For production, you'll want proper encryption keys and a two-node cluster:

```bash
# Generate keys and secrets
KEY=$(openssl rand -hex 32)
ROUTING_SECRET=$(openssl rand -hex 32)
TOKEN=$(openssl rand -hex 16)

# Node 1 (becomes primary: node1 < node2 lexicographically)
./bbd --host-id=node1 --tcp=:8081 --uds=/var/run/bbd/bbd.sock \
  --peers=node2@node2-host:8082 \
  --store-keys="0:$KEY" --store-key-current=0 \
  --routing-secret="$ROUTING_SECRET" \
  --internal-token="$TOKEN" \
  --memory-limit=4294967296

# Node 2 (becomes secondary)
./bbd --host-id=node2 --tcp=:8082 --uds=/var/run/bbd/bbd.sock \
  --peers=node1@node1-host:8081 \
  --store-keys="0:$KEY" --store-key-current=0 \
  --routing-secret="$ROUTING_SECRET" \
  --internal-token="$TOKEN" \
  --memory-limit=4294967296
```

The [Installation Guide](doc/installation.md) covers building from source, managing secrets, and configuring systemd. The [Operations Guide](doc/operations.md) explains monitoring, capacity planning, and troubleshooting.

## Documentation

The complete documentation is in the [doc/](doc/) directory:

**Getting Started:**
- [Introduction](doc/introduction.md) — What Big Bunny is and what problems it solves
- [Installation](doc/installation.md) — Building, configuring, and deploying
- [Quick Start](doc/quickstart.md) — 5-minute hands-on walkthrough

**Using Big Bunny:**
- [Usage Guide](doc/usage.md) — Detailed examples for CLI and HTTP API
- [API Reference](doc/api-reference.md) — Complete endpoint documentation with error codes

**Production:**
- [Operations Guide](doc/operations.md) — Deployment, monitoring, and troubleshooting
- [Architecture](doc/architecture.md) — How replication, failover, and recovery work
- [Security](doc/security.md) — Threat analysis and security best practices

## API at a Glance

All operations go through a Unix domain socket using HTTP:

```bash
# Create a store
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -d "initial data" \
  http://localhost/api/v1/create

# Read a store
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  http://localhost/api/v1/snapshot/{store-id}

# One-shot update (lock, write, unlock in one operation)
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -d "updated data" \
  http://localhost/api/v1/update/{store-id}

# Modify protocol (lock-based)
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  http://localhost/api/v1/begin-modify/{store-id}
# ... do work with lock ...
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "BigBunny-Lock-ID: {lock-id}" \
  -d "modified data" \
  http://localhost/api/v1/complete-modify/{store-id}

# Named stores
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -d "cart data" \
  http://localhost/api/v1/create-by-name/shopping-cart

# Check node status
curl --unix-socket /tmp/bbd.sock http://localhost/status
```

See the [API Reference](doc/api-reference.md) for complete documentation.

## Trade-Offs and Limitations

Big Bunny makes deliberate trade-offs to optimize for edge computing workloads:

- **Not durable** — Data lives in RAM. If both nodes fail simultaneously, you lose everything. This is acceptable for session data that naturally expires anyway.
- **Async replication** — Recent writes (within ~100ms) might be lost on failover.
- **Small stores** — Maximum 2KB per store. This is session storage, not a general-purpose database.
- **Single-site only** — Designed for within a Point of Presence, not cross-region.
- **Two nodes maximum** — No support for larger replica sets.

These trade-offs make Big Bunny fast and simple to operate. If you need durability, consistency, or large payloads, you want a different tool.

## Performance

| Metric          | Value   | Notes                       |
| --------------- | ------- | --------------------------- |
| Write latency   | ~100μs  | Local operation only        |
| Read latency    | ~50μs   | No lock acquisition         |
| Failover time   | ~4s     | Lease + grace period        |
| Max store size  | 2KB     | Configurable limit          |
| Default TTL     | 14 days | Configurable per store      |
| Lock timeout    | 500ms   | Fixed for modify operations |
| Replication lag | <100ms  | Typical under normal load   |

## Security

Store IDs use AES-128-SIV encryption with per-customer key derivation via HKDF. This provides cryptographic isolation—even with a bug in the routing logic, customers can't access each other's data because they literally can't decrypt the store IDs.

Best practices:
- Generate keys with `openssl rand -hex 32`
- Rotate keys every 90 days
- Store keys in a secrets manager (Vault, AWS Secrets Manager, etc.)
- Run on private networks
- Use restrictive Unix socket permissions (0600)
- Run as a non-root user

See the [Security Guide](doc/security.md) for the complete threat analysis.

## Testing

```bash
go test ./...                      # All tests
go test ./test -run TestStore -v   # Specific test
go test ./test -run Cluster -v     # Cluster tests
go test ./test -run ClockSkew -v   # Clock skew tests
SOAK=1 go test ./test -run Soak    # Full soak test (~3 min)
```

The test suite includes 86 tests covering unit, integration, cluster operations, clock skew scenarios, and soak testing.
