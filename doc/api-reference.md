# API Reference

This is the complete reference for Big Bunny's HTTP API. If you're looking for examples and practical usage patterns, check out the [Usage Guide](usage.md) first. This document is the detailed specification you'll want when implementing against the API or debugging tricky issues.

## How to Connect

Big Bunny communicates over a Unix domain socket rather than TCP. This is deliberate—it keeps the API local to the host, which is exactly what you want for session storage at the edge. Your application server and Big Bunny live on the same machine, so there's no network hop, and the Unix socket gives you both speed and automatic access control through filesystem permissions.

The socket path is configurable with the `--uds` flag and defaults to `/tmp/bbd.sock`. When you make requests with curl, you'll use the `--unix-socket` flag to connect:

```bash
curl --unix-socket /tmp/bbd.sock http://localhost/api/v1/create
```

The hostname in the URL doesn't matter—you're connecting through the socket, not TCP. The convention is to use `localhost`, but Big Bunny ignores it entirely.

## Authentication and Isolation

Every request must include an `X-Customer-ID` header. This is how Big Bunny enforces tenant isolation. Each customer's data is cryptographically separated—their store IDs are encrypted with a customer-specific key, so even if you somehow got another customer's store ID, you couldn't decrypt it.

```
X-Customer-ID: my-customer-id
```

The customer ID needs to be 1-64 characters, using alphanumeric characters plus underscores and hyphens. In production, you'd derive this from an authentication token after verifying it (JWT, signed tokens, session tokens, or any other secure authentication mechanism). The proof of concept uses a simple header for clarity, but the security model assumes you're doing proper authentication before the request reaches Big Bunny.

## Public API Endpoints

All public endpoints live under `/api/v1/`. Here's the complete list at a glance:

| Method | Endpoint                             | Description                 |
| ------ | ------------------------------------ | --------------------------- |
| POST   | `/api/v1/create`                     | Create anonymous store      |
| POST   | `/api/v1/create-by-name/{name}`      | Create named store          |
| POST   | `/api/v1/snapshot/{store-id}`        | Read store contents         |
| POST   | `/api/v1/begin-modify/{store-id}`    | Acquire lock, get contents  |
| POST   | `/api/v1/complete-modify/{store-id}` | Update store, release lock  |
| POST   | `/api/v1/cancel-modify/{store-id}`   | Release lock without update |
| POST   | `/api/v1/update/{store-id}`          | One-shot update (auto-lock) |
| POST   | `/api/v1/delete/{store-id}`          | Delete store                |
| POST   | `/api/v1/delete-by-name/{name}`      | Delete named store          |
| POST   | `/api/v1/lookup-id-by-name/{name}`   | Get store ID for name       |
| GET    | `/status`                            | Node status (JSON)          |

Let's walk through each operation in detail.

## Creating Stores

The most common operation is creating a new store. You give Big Bunny some initial data, and it gives back an encrypted store ID that you'll use for all future operations.

**Endpoint**: `POST /api/v1/create`

You'll send the initial store contents in the request body, which can be any arbitrary bytes up to 2KB. Big Bunny doesn't care what format you use—JSON, MessagePack, Protobuf, plain text, whatever makes sense for your application. The `Content-Type` header should be `application/octet-stream` to signal that you're sending binary data, though Big Bunny doesn't actually enforce this.

The `BigBunny-Not-Valid-After` header lets you set a TTL in seconds. If you don't specify one, stores default to 14 days (1,209,600 seconds). For short-lived session data like shopping carts, you might set this to 3,600 (one hour). For longer-lived data like user preferences, you might use several days or weeks.

Here's a complete example:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "BigBunny-Not-Valid-After: 3600" \
  -H "Content-Type: application/octet-stream" \
  -d "initial data" \
  http://localhost/api/v1/create
```

On success, you get back a 200 response with the store ID in the body. It looks like this: `v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9...`. Save that ID somewhere—you'll need it for every subsequent operation on this store.

The possible errors you might see are:

- **507 Insufficient Storage** with error code `CapacityExceeded` means the node has hit its memory limit and can't accept new stores. You'll need to either clean up old stores, increase the memory limit, or add more capacity.

- **503 Service Unavailable** with error code `LeaderChanged` is rare—it means the node couldn't forward your request to the primary (typically due to network issues or the primary being unavailable). Requests are automatically forwarded to the primary internally, but if forwarding fails, you'll see this error with a `Retry-After: 1` header suggesting you retry.

## Reading Stores

To read a store without modifying it, use the snapshot endpoint. This is a simple read that doesn't acquire any locks, so multiple clients can read simultaneously without blocking each other.

**Endpoint**: `POST /api/v1/snapshot/{store-id}`

Replace `{store-id}` with the encrypted store ID you got from create. You just need the customer ID header—no body, no other headers required:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  http://localhost/api/v1/snapshot/v1:0:8ahePLwi...
```

The response body contains the store contents, and the `BigBunny-Not-Valid-After` header tells you how many seconds remain until the store expires. This is useful if you want to display a "session expires in..." message to users.

Keep in mind that snapshot reads may return slightly stale data. Because replication is asynchronous, the secondary might be a few milliseconds behind the primary. For most session storage use cases, this is fine. If you need serialized access, use the modify protocol instead.

The errors you might encounter:

- **404 Not Found** with error code `NotFound` means the store doesn't exist. Maybe it expired, maybe it was deleted, or maybe you have a typo in the store ID.

- **403 Forbidden** with error code `Unauthorized` means the customer ID you provided doesn't match the customer who created the store. This is the cryptographic isolation at work—you literally cannot read another customer's data.

- **410 Gone** with error code `StoreExpired` means the store existed but its TTL has passed. The garbage collector will clean it up eventually, but for now, it's considered gone.

- **400 Bad Request** (without a specific error code) means the store ID format is invalid or decryption failed. This usually indicates a bug in how you're constructing or storing the ID.

## Updating Stores

There are two ways to update a store: the simple one-shot update, and the explicit modify protocol. Let's start with the simple version.

**Endpoint**: `POST /api/v1/update/{store-id}`

This endpoint does everything in one operation—acquires the lock, replaces the contents, releases the lock. It's the easiest way to update a store when you don't need to read the current contents first:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "BigBunny-Not-Valid-After: 7200" \
  -d "updated data" \
  http://localhost/api/v1/update/v1:0:8ahePLwi...
```

The `BigBunny-Not-Valid-After` header is optional here too. If you provide it, the store's TTL gets reset to the new value. If you don't, the TTL stays unchanged.

On success, you get a 200 response. Sometimes you'll also see a `BigBunny-Warning: DegradedWrite` header. This means the write succeeded on the primary, but replication to the secondary failed. Your data is safe, but there's no backup copy right now. If the primary fails before replication catches up, you could lose this update.

The errors are similar to create:

- **409 Conflict** with error code `StoreLocked` means another request is currently modifying this store. The lock timeout is 500 milliseconds, so wait a bit and retry.

- **409 Conflict** with error code `LockStateUnknown` happens right after a failover. The new primary isn't sure whether the lock was held on the old primary, so it rejects lock operations for 500 milliseconds. Wait the suggested `Retry-After` duration and try again.

- **404**, **403**, and **507** errors work the same as create.

- **503 Service Unavailable** with `LeaderChanged` means automatic forwarding to the primary failed. Retry the request.

## The Modify Protocol

When you need to read the current contents, make some changes, and write them back, use the three-phase modify protocol. This gives you serialized access—no one else can modify the store while you hold the lock.

### Begin Modify

First, acquire the lock and retrieve the current contents:

**Endpoint**: `POST /api/v1/begin-modify/{store-id}`

```bash
curl -D /tmp/headers.txt -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  http://localhost/api/v1/begin-modify/v1:0:8ahePLwi...
```

The `-D /tmp/headers.txt` flag saves the response headers to a file. You need this because the response includes a `BigBunny-Lock-ID` header with a lock token that you'll use for the complete or cancel call. The body contains the current store contents, and there's also a `BigBunny-Not-Valid-After` header with the remaining TTL.

Extract the lock ID from the headers:

```bash
LOCK_ID=$(grep "BigBunny-Lock-ID:" /tmp/headers.txt | cut -d' ' -f2 | tr -d '\r')
```

Now you have 500 milliseconds to do your work and either complete or cancel the modification. If you take longer than that, the lock automatically expires and someone else can acquire it.

The errors are the same as update: `StoreLocked`, `LockStateUnknown`, `NotFound`, `Unauthorized`, or `LeaderChanged`.

### Complete Modify

Once you've made your changes, commit them and release the lock:

**Endpoint**: `POST /api/v1/complete-modify/{store-id}`

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "BigBunny-Lock-ID: a7f8e9d0-1234-5678-9abc-def012345678" \
  -H "BigBunny-Not-Valid-After: 7200" \
  -d "modified data" \
  http://localhost/api/v1/complete-modify/v1:0:8ahePLwi...
```

The `BigBunny-Lock-ID` header must match the token you got from begin-modify. If it doesn't match, you'll get a `409 Conflict` with error code `LockMismatch`. This prevents one client from accidentally releasing another client's lock.

All the usual errors apply here, plus:

- **409 Conflict** with `LockMismatch` means the lock ID is wrong or the lock has expired.

- **507 Insufficient Storage** with `CapacityExceeded` means the new contents are too large or would exceed the memory limit.

### Cancel Modify

If you decide you don't want to make changes after all, release the lock without updating:

**Endpoint**: `POST /api/v1/cancel-modify/{store-id}`

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "BigBunny-Lock-ID: a7f8e9d0-1234-5678-9abc-def012345678" \
  http://localhost/api/v1/cancel-modify/v1:0:8ahePLwi...
```

This is always safe to call. If the lock has already expired, cancel is a no-op and returns 200 anyway. This makes it idempotent, which is handy for retry logic.

## Deleting Stores

Deletion is straightforward:

**Endpoint**: `POST /api/v1/delete/{store-id}`

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  http://localhost/api/v1/delete/v1:0:8ahePLwi...
```

On success, you get 200. If the store doesn't exist, you also get 200—delete is idempotent. This means you can safely retry deletes without worrying about 404 errors on retry.

Behind the scenes, Big Bunny creates a tombstone for each deleted store. The tombstone lives for 24 hours and prevents the store from being resurrected during replication. If the secondary hasn't received the delete yet and the primary fails, the tombstone ensures the secondary won't bring the store back.

The possible errors:

- **403 Forbidden** with `Unauthorized` means the customer ID is wrong.

- **503 Service Unavailable** with `LeaderChanged` means automatic forwarding to the primary failed. Retry the request.

Note that delete doesn't return 404 if the store is missing. This is by design—deleting a non-existent resource is considered successful.

## Working with Named Stores

Sometimes you want to give a store a human-readable name instead of dealing with encrypted IDs. Big Bunny provides a name registry for this.

### Create Named Store

**Endpoint**: `POST /api/v1/create-by-name/{name}`

The name can be 1-64 characters using alphanumeric characters plus underscores and hyphens. Names are scoped per customer, so different customers can use the same name without conflict:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "BigBunny-Not-Valid-After: 3600" \
  -d '{"cart": []}' \
  http://localhost/api/v1/create-by-name/shopping-cart
```

You get back a store ID just like with regular create. The name is now associated with that ID, and you can look it up later.

By default, trying to create a name that already exists returns `409 Conflict`. If you want to be more lenient and just return the existing store ID when the name is taken, add the `BigBunny-Reuse-If-Exists: true` header:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "BigBunny-Reuse-If-Exists: true" \
  -d '{"cart": []}' \
  http://localhost/api/v1/create-by-name/shopping-cart
```

This makes the operation idempotent, which is useful for ensuring a named store exists without caring whether it's new or old.

The name registry uses a two-phase reservation protocol to prevent races. When you create a name, it first enters a "Creating" state while the store is being created. If another request tries to use the same name during this window, it gets a `503 Service Unavailable` with error code `NameCreating` and a `Retry-After` header. Just wait and retry—the reservation will either complete or time out (after 5 seconds) and then you can proceed.

### Look Up Store ID by Name

To retrieve the store ID associated with a name:

**Endpoint**: `POST /api/v1/lookup-id-by-name/{name}`

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  http://localhost/api/v1/lookup-id-by-name/shopping-cart
```

The response body contains the store ID. If the name doesn't exist or the store it references has expired, you get 404 with error code `NotFound`. The `NameCreating` error can happen here too if you race with someone creating that name.

### Delete Named Store

Deleting by name removes both the store and the name mapping:

**Endpoint**: `POST /api/v1/delete-by-name/{name}`

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  http://localhost/api/v1/delete-by-name/shopping-cart
```

This is idempotent—deleting a non-existent name returns 200. If the name exists but the store it references is gone, the name mapping gets cleaned up and you still get 200.

## Counter Stores

Big Bunny supports atomic counter stores for use cases like rate limiting, resource quotas, session metrics, and distributed semaphores. Counter stores provide server-side increment/decrement operations without exposing locks to clients, making them faster and simpler than the modify protocol.

### Creating a Counter

**Endpoint**: `POST /api/v1/create`

To create a counter instead of a blob store, send JSON with `"type":"counter"`:

```bash
# Unbounded counter starting at 0
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "Content-Type: application/json" \
  -d '{"type":"counter","value":0}' \
  http://localhost/api/v1/create

# Bounded counter with min/max limits
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "Content-Type: application/json" \
  -d '{"type":"counter","value":50,"min":0,"max":100}' \
  http://localhost/api/v1/create
```

Counters can have optional `min` and `max` bounds. When specified, increment/decrement operations will clamp the value to these bounds and set a `bounded` flag in the response. Bounds are immutable after creation.

### Incrementing a Counter

**Endpoint**: `POST /api/v1/increment/{storeID}`

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "Content-Type: application/json" \
  -d '{"delta":5}' \
  http://localhost/api/v1/increment/{store-id}

# Optionally reset TTL with BigBunny-Not-Valid-After header
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "Content-Type: application/json" \
  -H "BigBunny-Not-Valid-After: 3600" \
  -d '{"delta":5}' \
  http://localhost/api/v1/increment/{store-id}
```

Response:

```json
{
  "value": 55,
  "version": 2,
  "bounded": false,
  "min": 0,
  "max": 100
}
```

The `bounded` field is `true` when the operation hit a min/max limit. The `delta` parameter is required and can be positive or negative (negative delta decrements).

**TTL Behavior**: The optional `BigBunny-Not-Valid-After` header resets the counter's expiry time. Without it, the original expiry time is preserved.

### Decrementing a Counter

**Endpoint**: `POST /api/v1/decrement/{storeID}`

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "Content-Type: application/json" \
  -d '{"delta":3}' \
  http://localhost/api/v1/decrement/{store-id}
```

This is syntactic sugar for incrementing with a negative delta. Response format is identical to increment. Like increment, it also supports the optional `BigBunny-Not-Valid-After` header to reset the counter's TTL.

### Reading a Counter

**Endpoint**: `POST /api/v1/snapshot/{storeID}`

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  http://localhost/api/v1/snapshot/{store-id}
```

For counter stores, the response is JSON instead of raw bytes:

```json
{
  "value": 52,
  "version": 3,
  "min": 0,
  "max": 100
}
```

### Setting a Counter Value

**Endpoint**: `POST /api/v1/update/{storeID}`

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "Content-Type: application/json" \
  -d '{"value":75}' \
  http://localhost/api/v1/update/{store-id}
```

This sets the counter to a specific value. If the counter has bounds, the value must be within `[min, max]` or you'll get a `400 Bad Request` with error code `ValueOutOfBounds`.

### Named Counters

Counters work seamlessly with the named store registry. Create a named counter by sending JSON to the `create-by-name` endpoint:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: acme-corp" \
  -H "Content-Type: application/json" \
  -H "BigBunny-Not-Valid-After: 60" \
  -d '{"type":"counter","value":0,"max":100}' \
  http://localhost/api/v1/create-by-name/rate-limit:customer123
```

This is particularly useful for rate limiting where you want to create a counter per time window.

### Counter Error Codes

Counter operations can return these specific error codes (via `BigBunny-Error-Code` header):

- **TypeMismatch**: Attempted counter operation on a blob store, or blob operation on a counter
- **Overflow**: Integer overflow or underflow detected during increment/decrement
- **ValueOutOfBounds**: Tried to set counter to value outside min/max bounds
- **InvalidBounds**: Specified min > max when creating counter

### Use Cases

**Rate Limiting**: Track requests per customer per time window using named counters with TTL and max bound.

**Resource Quotas**: Track remaining credits by decrementing a counter with min=0. When `bounded: true`, quota is exhausted.

**Session Metrics**: Count page views, API calls, or events per session using unbounded counters.

**Distributed Semaphores**: Track available slots using bounded counter (min=0, max=slots). Decrement to acquire, increment to release.

### Performance

Counter operations are approximately 12% faster than the equivalent modify-protocol-based approach (benchmarked at 342ns/op vs 389ns/op) and use 45% less memory. Counters replicate asynchronously like blob stores, with the same bounded data loss characteristics on failover.

## Status Endpoint

The status endpoint gives you visibility into what the node is doing. It's available over the Unix socket without requiring a customer ID:

**Endpoint**: `GET /status`

```bash
curl --unix-socket /tmp/bbd.sock http://localhost/status
```

You get back a JSON object with lots of useful information:

```json
{
  "node_id": "node1",
  "role": "primary",
  "epoch": 3,
  "store_count": 142,
  "used_bytes": 581632,
  "memory_limit": 4294967296,
  "peers": ["node2@localhost:8082"],
  "queue_length": 0,
  "registry_queue_length": 0,
  "replication_fail_count": 0,
  "last_replication_fail": null
}
```

Here's what each field means:

- **node_id**: The identifier for this node (from `--host-id` flag)
- **role**: One of `primary`, `secondary`, or `joining`. Primary handles writes, secondary replicates and can handle reads, joining means the node is recovering after a restart.
- **epoch**: A monotonically increasing counter that increments on every failover. Useful for detecting split-brain—if two nodes both claim to be primary, the one with the higher epoch wins.
- **store_count**: How many stores are currently in memory on this node.
- **used_bytes**: Total memory used by store bodies and metadata.
- **memory_limit**: The configured memory limit (from `--memory-limit` flag). Zero means unlimited.
- **peers**: The list of peer nodes in the cluster.
- **queue_length**: How many store replication messages are queued waiting to be sent to the secondary. This should usually be zero or low single digits. If it grows, replication is falling behind.
- **registry_queue_length**: Same as queue_length but for name registry replication messages.
- **replication_fail_count**: How many consecutive replication attempts have failed. If this is non-zero and growing, something is wrong with the secondary.
- **last_replication_fail**: How many seconds ago the last replication failure happened. Useful for debugging replication issues.

In production, you'd poll this endpoint regularly and export the metrics to your monitoring system. Watch the role (to detect split-brain or missing primary), the epoch (to detect frequent failovers), the memory usage (to predict when you'll hit capacity), and the queue depths (to detect replication lag).

## Request Headers Summary

Here's a quick reference for all the request headers you might use:

| Header                     | Type           | Description                                              |
| -------------------------- | -------------- | -------------------------------------------------------- |
| `X-Customer-ID`            | Required       | Customer identifier (1-64 chars, alphanumeric + `_` `-`) |
| `BigBunny-Not-Valid-After` | Optional       | TTL in seconds (default: 1209600)                        |
| `BigBunny-Lock-ID`         | Modify ops     | Lock token from begin-modify                             |
| `BigBunny-Reuse-If-Exists` | create-by-name | Return existing store if name exists (`true`/`false`)    |
| `Content-Type`             | Create/Update  | Should be `application/octet-stream` for binary data     |

## Response Headers Summary

And here are the response headers Big Bunny might include:

| Header                     | Description                                       |
| -------------------------- | ------------------------------------------------- |
| `BigBunny-Not-Valid-After` | Remaining TTL in seconds                          |
| `BigBunny-Lock-ID`         | Lock token (from begin-modify)                    |
| `BigBunny-Error-Code`      | Machine-readable error code                       |
| `BigBunny-Warning`         | Warning message (e.g., `DegradedWrite`)           |
| `BigBunny-Lock-State`      | Lock state indicator (`unknown` after failover)   |
| `Retry-After`              | Seconds to wait before retry (for 503/409 errors) |

## Error Codes Reference

Every error response includes a `BigBunny-Error-Code` header with a machine-readable error code. Here's the complete list:

| Code               | HTTP | Retryable | Description                       |
| ------------------ | ---- | --------- | --------------------------------- |
| `NotFound`         | 404  | No        | Store or name not found           |
| `Unauthorized`     | 403  | No        | Customer ID mismatch              |
| `StoreLocked`      | 409  | Yes       | Lock held by another request      |
| `LockMismatch`     | 409  | No        | Wrong lock ID                     |
| `StoreExpired`     | 410  | No        | Store TTL expired                 |
| `LeaderChanged`    | 503  | Yes       | Forwarding to primary failed      |
| `StoreUnavailable` | 503  | Yes       | Node recovering                   |
| `LockStateUnknown` | 409  | Yes       | Lock state unclear after failover |
| `NameCreating`     | 503  | Yes       | Name reservation in progress      |
| `CapacityExceeded` | 507  | No        | Memory limit reached              |
| `TypeMismatch`     | 400  | No        | Counter op on blob or vice versa  |
| `Overflow`         | 409  | No        | Integer overflow/underflow        |
| `ValueOutOfBounds` | 400  | No        | Counter value outside min/max     |
| `InvalidBounds`    | 400  | No        | Counter min > max                 |
| (none)             | 429  | Yes       | Rate limit exceeded for customer  |

The "Retryable" column indicates whether you should retry the request. For errors like `NotFound` or `LockMismatch`, retrying won't help—you need to fix the problem first. For errors like `StoreLocked` or `LeaderChanged`, retrying after a delay is the right response.

One special case: if the store ID format is invalid or decryption fails, you get a plain `400 Bad Request` without a `BigBunny-Error-Code` header. This usually means there's a bug in how you're handling store IDs.

## Consistency Guarantees

It's worth understanding what guarantees Big Bunny provides:

**Writes are linearizable on the primary.** If you send two write requests in sequence to the primary, the second one will see the effects of the first. The lock-based serialization ensures that modifications to a single store happen in order.

**Reads are eventually consistent.** Because replication is asynchronous, the secondary might be slightly behind the primary. If you write to the primary and immediately read from the secondary, you might see old data. In practice, the lag is usually just a few milliseconds.

**Snapshot reads don't provide isolation.** The snapshot endpoint doesn't acquire any locks, so you might read partially-updated data if someone modifies the store while you're reading. For most session storage, this is fine. If you need stronger isolation, use the modify protocol.

**The modify protocol provides serialized access.** When you hold the lock, no one else can read or write the store. This gives you full isolation for read-modify-write operations.

See the [Architecture](architecture.md) document for more details on the consistency model and how replication works.

## Rate Limits

Big Bunny implements per-customer rate limiting to prevent resource exhaustion and protect against abusive clients. The rate limiter uses a token bucket algorithm that allows bursts while maintaining average throughput limits.

By default, each customer is limited to **100 requests per second** with a **burst capacity of 200 requests**. When a customer exceeds their limit, requests return `429 Too Many Requests` with a `Retry-After: 1` header suggesting they wait one second before retrying.

The rate limit is configurable via the `--rate-limit` flag (requests per second) and `--burst-size` flag (burst capacity). Setting `--rate-limit=0` disables rate limiting entirely, which is useful for trusted environments or during development.

```bash
# Enable rate limiting (100 req/s per customer, 200 burst)
./bbd --rate-limit=100 --burst-size=200

# Higher limits for high-traffic environments
./bbd --rate-limit=500 --burst-size=1000

# Disable rate limiting
./bbd --rate-limit=0
```

The rate limiter enforces limits at the customer level, so one customer can't consume all resources and starve others. Limits apply to all public API endpoints after customer ID extraction, including both direct requests and forwarded requests.

Additional natural limits exist beyond rate limiting:

- **Serialization**: Each store can only be modified by one client at a time. The 500-millisecond lock timeout means you can do at most 2 modifications per second per store.

- **Memory capacity**: Once you hit your memory limit, creates start failing with `CapacityExceeded`.

- **System resources**: CPU, memory, and network bandwidth impose practical limits. A single node can handle tens of thousands of operations per second before these become bottlenecks.

## Idempotency

Some operations are safe to retry, others are not. Here's the breakdown:

| Operation         | Idempotent? | Notes                                   |
| ----------------- | ----------- | --------------------------------------- |
| Create            | No          | Creates new store each time             |
| Snapshot          | Yes         | Read-only                               |
| Update            | No          | Modifies store each time                |
| Delete            | Yes         | Deleting non-existent store returns 200 |
| begin-modify      | No          | Each call acquires a new lock           |
| complete-modify   | No          | Modifies store                          |
| cancel-modify     | Yes         | Releasing released lock is a no-op      |
| create-by-name    | No*         | Unless `Reuse-If-Exists: true`          |
| lookup-id-by-name | Yes         | Read-only                               |
| delete-by-name    | Yes         | Deleting non-existent name returns 200  |

The asterisk on create-by-name means it's not idempotent by default, but you can make it idempotent with the `BigBunny-Reuse-If-Exists: true` header.

When implementing retry logic, always check the idempotency of the operation. For non-idempotent operations like create or update, you might want to add your own idempotency key at the application layer.

## Next Steps

This reference covers every endpoint and error code in detail, but if you're looking for practical examples and patterns, check out the [Usage Guide](usage.md). For understanding how replication and failover work under the hood, see the [Architecture](architecture.md) document. And for security considerations around encryption and authentication, read the [Security](security.md) guide.
