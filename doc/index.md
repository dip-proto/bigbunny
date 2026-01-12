# Big Bunny Documentation

Welcome to Big Bunny—a fast, in-memory session store built for edge computing. If you're running application servers at the edge and need somewhere to stash session data, user state, or temporary information with automatic replication and failover, you're in the right place.

This documentation will walk you through everything from getting started to deploying Big Bunny in production. The guides are written to be read straight through, but feel free to jump around if you're looking for something specific.

## Start Here

If you're new to Big Bunny, these three guides will get you oriented and up to speed quickly.

**[Introduction](introduction.md)** explains what Big Bunny is and what problems it solves. If you're wondering whether Big Bunny is right for your use case, start here. We'll talk about the edge computing problem it addresses, how it compares to alternatives like Redis or Memcached, and what trade-offs it makes.

**[Installation](installation.md)** covers getting Big Bunny built and running. You'll learn how to compile from source, generate encryption keys, and configure both single-node and two-node cluster deployments. If you just want to kick the tires locally, this guide will get you there in a few minutes.

**[Quick Start](quickstart.md)** is a hands-on walkthrough that takes about five minutes. You'll start a Big Bunny node, create some stores, read them back, use the modify protocol, and try out named stores. This is the fastest way to get a feel for how Big Bunny works.

## Using Big Bunny

Once you're past the basics, these guides dive into the details of working with Big Bunny day-to-day.

**[Usage Guide](usage.md)** is the comprehensive manual for both the command-line interface and the HTTP API. You'll find detailed examples for every operation, explanations of error handling and retry logic, and practical patterns for common use cases like shopping carts and session storage. This is where you come when you need to know exactly how something works.

**[API Reference](api-reference.md)** is the complete specification for every HTTP endpoint. Each endpoint is documented with request/response formats, all possible error codes, and curl examples. The reference tables for headers and error codes make this the place to go when you're debugging an integration or need to know the exact behavior of an edge case.

## Running in Production

When you're ready to deploy Big Bunny for real, these guides cover operations, monitoring, and keeping things running smoothly.

**[Operations Guide](operations.md)** walks through production deployment—from systemd configuration and secrets management to monitoring metrics, setting up alerts, and handling the inevitable issues that come up. You'll learn about capacity planning, rolling restarts, and troubleshooting common problems like split-brain or replication lag.

## Understanding How It Works

These documents explain the internals and design decisions behind Big Bunny.

**[Architecture](architecture.md)** is a deep dive into how Big Bunny works under the hood. We cover the replication model, lease-based failover, epoch fencing, lock-based serialization, and the recovery protocol. If you want to understand why Big Bunny behaves the way it does, or you're curious about the trade-offs between consistency and availability, this is the document for you.

**[Security](security.md)** explains the security model from top to bottom. You'll learn how store ID encryption works with AES-128-SIV, how per-customer key derivation provides cryptographic isolation, and how to manage encryption keys safely. The document also includes a threat analysis covering various attack scenarios and explains what Big Bunny protects against (and what it doesn't).

## Quick Navigation

### Common Tasks

New users often want to know how to do these specific things:

- [Create a store](quickstart.md#step-2-create-a-store) — The most basic operation
- [Read a store](quickstart.md#step-3-read-the-store) — Retrieving what you stored
- [Modify a store](quickstart.md#step-4-modify-the-store) — Using the lock-based protocol
- [Use named stores](quickstart.md#step-5-named-stores) — Human-readable store names
- [Set up a two-node cluster](installation.md#two-node-cluster) — Production deployment
- [Check node status](usage.md#check-status) — Monitoring and health checks

### Key Concepts

If you're trying to understand how Big Bunny really works, these sections explain the core ideas:

- [Replication model](architecture.md#replication-model) — How data gets copied between nodes
- [Primary election](architecture.md#primary-election) — How nodes decide who's in charge
- [Lock-based serialization](architecture.md#lock-based-serialization) — Preventing race conditions
- [Store ID encryption](architecture.md#store-id-encryption) — Security and isolation
- [Failover and recovery](architecture.md#recovery-protocol-rejoining-nodes) — What happens when nodes die

### Production Topics

When you're deploying Big Bunny for real, these sections have what you need:

- [Configuration management](operations.md#configuration-management) — Handling secrets safely
- [Process management with systemd](operations.md#process-management) — Running as a service
- [Monitoring and alerting](operations.md#monitoring) — What metrics to watch
- [Capacity planning](operations.md#capacity-planning) — How much memory you need
- [Troubleshooting guide](operations.md#troubleshooting) — Fixing common problems

## What Big Bunny Does

Big Bunny gives you in-memory storage with microsecond-level latency for reads and writes. The lock-based modify protocol prevents race conditions when you need to read, change, and write back data. If a host fails, the secondary automatically takes over within about four seconds, so your application stays available even when hardware dies.

Store IDs are encrypted with AES-128-SIV, which provides both security and customer isolation. Even if someone gets another customer's store ID, they can't decrypt it because each customer's stores are encrypted with a different derived key. Named stores give you the option of using human-readable names instead of opaque encrypted IDs when that makes sense.

TTL management happens automatically—stores expire when their time is up, and the garbage collector cleans them up without you having to think about it. You can configure memory limits to control how much RAM each node uses, and Big Bunny will reject new stores once the limit is reached rather than crashing.

## How Big Bunny Works

The architecture is deliberately simple. Two nodes per Point of Presence, with the primary handling writes and the secondary receiving asynchronous updates. When you write to the primary, it responds immediately and then replicates to the secondary in the background. This async approach keeps write latency low at the cost of possibly losing the most recent updates if the primary fails before replication completes.

Failover uses a lease-based mechanism. The primary sends heartbeats every 200 milliseconds, and the secondary expects to receive them. If the secondary doesn't hear from the primary for about four seconds, it assumes the primary is dead and promotes itself. The epoch number increments with each failover, which prevents split-brain scenarios from corrupting data when the network heals after a partition.

When a node rejoins after a failure, it enters a "joining" state and fetches a snapshot from the current primary. Once caught up, it becomes the secondary and starts receiving normal replication traffic. The state machine is simple: joining → secondary → primary (on failover) → joining again (if it restarts).

Here's a visual representation of how the system fits together:

```
┌─────────────────────────────────────────────────┐
│                    Client                       │
│               (Application Server)              │
└───────────────────┬─────────────────────────────┘
                    │ Unix Socket
                    │ (Local API)
                    ↓
┌────────────────────────────────────────────────┐
│              Big Bunny Node 1                  │
│                  (Primary)                     │
│  ┌──────────────────────────────────────────┐  │
│  │  Store Manager (In-Memory)               │  │
│  │  - Store CRUD operations                 │  │
│  │  - Lock management                       │  │
│  │  - Version tracking                      │  │
│  └──────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────┐  │
│  │  Replica Manager                         │  │
│  │  - Async replication queue               │  │
│  │  - Heartbeat monitoring                  │  │
│  │  - Failover detection                    │  │
│  └──────────────────────────────────────────┘  │
└───────────────────┬────────────────────────────┘
                    │ TCP (Internal)
                    │ (Replication + Heartbeat)
                    ↓
┌────────────────────────────────────────────────┐
│              Big Bunny Node 2                  │
│                 (Secondary)                    │
│  ┌──────────────────────────────────────────┐  │
│  │  Store Manager (Replicated)              │  │
│  │  - Receives async updates                │  │
│  │  - Serves stale reads                    │  │
│  └──────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────┐  │
│  │  Replica Manager                         │  │
│  │  - Monitors primary health               │  │
│  │  - Promotes on lease expiry              │  │
│  └──────────────────────────────────────────┘  │
└────────────────────────────────────────────────┘
```

Write operations flow from the client to the primary via the Unix socket. The primary applies the change locally, increments the version counter, responds immediately to the client, and then queues a replication message to the secondary. The secondary applies the update after checking that the version number is monotonic.

When the primary fails, the secondary detects the lease expiry after about four seconds, promotes itself to primary, and increments the epoch. Clients retry their requests, which now route to the new primary. If the old primary rejoins later, it comes back as a secondary and recovers its state from the current primary.

## API at a Glance

Big Bunny's API lives on a Unix domain socket and uses HTTP. Here's what you can do:

### Basic Store Operations

| Endpoint                      | Method | Description            |
| ----------------------------- | ------ | ---------------------- |
| `/api/v1/create`              | POST   | Create anonymous store |
| `/api/v1/snapshot/{store-id}` | POST   | Read store contents    |
| `/api/v1/update/{store-id}`   | POST   | One-shot update        |
| `/api/v1/delete/{store-id}`   | POST   | Delete store           |

### Modify Protocol (Lock-Based)

| Endpoint                             | Method | Description                  |
| ------------------------------------ | ------ | ---------------------------- |
| `/api/v1/begin-modify/{store-id}`    | POST   | Acquire lock, get contents   |
| `/api/v1/complete-modify/{store-id}` | POST   | Commit changes, release lock |
| `/api/v1/cancel-modify/{store-id}`   | POST   | Release lock without changes |

### Named Stores

| Endpoint                           | Method | Description           |
| ---------------------------------- | ------ | --------------------- |
| `/api/v1/create-by-name/{name}`    | POST   | Create named store    |
| `/api/v1/lookup-id-by-name/{name}` | POST   | Get store ID for name |
| `/api/v1/delete-by-name/{name}`    | POST   | Delete named store    |

Every request requires an `X-Customer-ID` header for tenant isolation. See the [API Reference](api-reference.md) for complete documentation with examples and error codes.

## Configuration Basics

Getting Big Bunny running requires just a few flags. Here's the minimum for a single node:

```bash
--host-id=node1           # Unique node identifier
--tcp=:8081              # TCP address for replication
--uds=/tmp/bbd.sock      # Unix socket for API
```

For production, you'll also want these:

```bash
--store-keys="0:hexkey"          # Encryption keys
--store-key-current=0            # Current key ID
--internal-token="secret"        # Internal endpoint auth
--memory-limit=4294967296        # 4GB memory cap
--customer-memory-quota=104857600 # 100MB per customer
--tombstone-customer-limit=1000  # Prevent rapid delete attacks
--tombstone-global-limit=10000   # Global tombstone cap
```

And for a two-node cluster, add the peer:

```bash
--peers=node2@host:8082  # Comma-separated peer list
```

Generate encryption keys with `openssl rand -hex 32` and store them securely. Never commit keys to version control or leave them in shell history. The [Installation](installation.md) guide has the full details.

## What to Expect

Big Bunny is fast—write latency is around 100 microseconds, and reads are even faster at about 50 microseconds. Failover takes roughly four seconds from when the primary dies to when the secondary takes over. Stores can be up to 2KB each, and the default TTL is 14 days (though you can configure this per store).

The modify protocol's lock timeout is 500 milliseconds, which means you can do at most two modifications per second per store. Replication lag is typically under 100 milliseconds, so if you write to the primary and immediately read from the secondary, you might see slightly stale data.

See the [Architecture](architecture.md) document for a detailed performance analysis and discussion of the trade-offs.

## Security Overview

Big Bunny uses AES-128-SIV to encrypt store IDs, with per-customer key derivation. This provides both tamper protection and cryptographic isolation between customers. Even with a bug in the routing logic, customers can't access each other's data because they literally can't decrypt the store IDs.

Internal replication endpoints require authentication via a shared token, though the current implementation doesn't encrypt the traffic (you should run on a trusted private network). The Unix socket's file permissions control who can access the local API.

Resource exhaustion protection includes per-customer rate limiting, per-customer memory quotas, tombstone limits to prevent rapid create/delete attacks, and configurable HTTP timeouts to defend against Slowloris-style attacks.

Best practices include generating strong random keys, rotating them every 90 days, using a secrets manager for storage, deploying on private networks, running as a non-root user, restricting socket permissions to mode 0600, and enabling resource exhaustion protections in production. The [Security](security.md) document has the full threat analysis and detailed guidance.

## Trade-Offs and Limitations

Big Bunny makes deliberate trade-offs to optimize for edge computing workloads. Data lives in RAM, so it's not durable—if both nodes fail simultaneously, you lose everything. This is acceptable for session data that naturally expires anyway.

Replication is asynchronous, which means the most recent writes (within about a 100-millisecond window) might be lost on failover. Stores are limited to 2KB each because this is session storage, not a general-purpose database. The system only supports two nodes per Point of Presence, not larger replica sets.

Split-brain is possible during network partitions because there's no external consensus service. When the partition heals, the epoch fencing mechanism ensures data doesn't get corrupted, but you might have had two primaries temporarily accepting writes. The [Architecture](architecture.md) document discusses these trade-offs in depth and explains when they're acceptable.

## What's Next?

If you're brand new to Big Bunny, start with the [Introduction](introduction.md) to understand what problems it solves and whether it's right for your use case. If you're ready to try it out, jump straight to the [Quick Start](quickstart.md) for a hands-on walkthrough. Planning a production deployment? The [Operations Guide](operations.md) has everything you need to know. And if you want to understand the algorithms and protocols, the [Architecture](architecture.md) document dives deep into the internals.

Thanks for using Big Bunny!
