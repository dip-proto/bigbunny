# Usage Guide

This guide walks through everything you can do with Big Bunny, from basic store operations to advanced error handling. We'll cover both the command-line interface and the HTTP API, with practical examples throughout.

## The Command-Line Interface

The `bbd` binary includes both the daemon and a full-featured CLI client. All CLI commands connect to Big Bunny through the Unix socket, so you'll typically specify which socket to use with the `--uds` flag. If you don't specify one, it defaults to `/tmp/bbd.sock`.

Most commands also need to know which customer you are. In production, this would come from your authentication token. In development, you set it explicitly with `--customer`. The default is `"default"`, which works fine for testing.

## Working with Stores

The basic operations are creating, reading, updating, and deleting stores. Let's walk through each one.

### Creating Stores

The simplest operation is creating a store. You give Big Bunny some data, and it gives you back an encrypted store ID that you'll use to reference that store later.

```bash
./bbd create -data "session data"
```

You get back something like `v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9...`. That's your store ID. Save it somewhere—you'll need it to read, update, or delete this store.

If you don't want to pass data on the command line, you can pipe it in:

```bash
echo "data from stdin" | ./bbd create
cat myfile.json | ./bbd create
```

By default, stores expire after 14 days. If you need a different lifetime, specify it with `--ttl`:

```bash
./bbd create -data "one hour session" -ttl 3600
```

The TTL is in seconds. An hour is 3600 seconds, a day is 86400, a week is 604800. When the TTL expires, the store gets garbage collected automatically.

If you're working with a specific customer (not just the default), set that too:

```bash
./bbd create -customer=acme-corp -data "isolated data"
```

### Reading Stores

Once you have a store ID, reading is straightforward:

```bash
./bbd get v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9...
```

This prints the store contents to stdout. It's fast because everything is in memory, and it doesn't acquire any locks, so multiple readers can access the same store simultaneously.

If you want to see how much time the store has left before expiring, add `--ttl`:

```bash
./bbd get v1:0:8ahePLwi... -ttl
```

This shows both the contents and the remaining TTL in seconds.

### Updating Stores

There are two ways to update a store, depending on whether you need to read the current contents first.

If you just want to replace the contents entirely, use the one-shot update command:

```bash
curl -X POST --unix-socket /tmp/bbd.sock -H "X-Customer-ID: acme-corp" -d "new contents" http://localhost/api/v1/update/v1:0:8ahePLwi...
```

This acquires the lock, updates the store, and releases the lock, all in one operation. It's perfect when you don't care what was there before.

If you want to change the TTL too:

```bash
curl -X POST --unix-socket /tmp/bbd.sock -H "X-Customer-ID: acme-corp" -H "BigBunny-Not-Valid-After: 7200" -d "new data" http://localhost/api/v1/update/v1:0:8ahePLwi...
```

### Deleting Stores

Deletion is permanent. The store gets removed, a tombstone is created (to prevent resurrection during replication), and after 24 hours even the tombstone goes away.

```bash
./bbd delete v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9...
```

This is idempotent—deleting a store that doesn't exist returns success, not an error. So you can safely delete stores without checking if they exist first.

## The Modify Protocol

When you need to modify a store based on its current contents—like incrementing a counter or adding an item to a list—you use the three-phase modify protocol. This ensures that no other request can modify the store while you're working on it.

### Phase One: Begin Modify

First, acquire the lock and get the current contents:

```bash
LOCK=$(./bbd begin-modify v1:0:8ahePLwi...)
```

This returns a lock ID (which gets saved in the `$LOCK` variable) and prints the current store contents to stdout. The lock is good for 500 milliseconds, so you need to complete your work within that window.

While you hold the lock, no other request can modify this store. They'll get `StoreLocked` errors if they try. So don't hold locks longer than necessary.

### Phase Two: Do Your Work

Now you have the current contents and a lock token. You can process the data however you need to. For example, if the store contains a counter:

```bash
CURRENT=$(./bbd get v1:0:8ahePLwi...)
NEW_VALUE=$((CURRENT + 1))
```

### Phase Three: Complete or Cancel

Once you've computed the new value, complete the modification:

```bash
./bbd complete-modify -lock "$LOCK" -data "$NEW_VALUE" v1:0:8ahePLwi...
```

The lock gets released, the store gets updated, and the change replicates to the secondary.

If something goes wrong and you want to abort without making changes, use cancel instead:

```bash
./bbd cancel-modify v1:0:8ahePLwi... -lock "$LOCK"
```

This releases the lock without modifying anything. The store stays exactly as it was.

## Working with Named Stores

Sometimes you want to refer to a store by a name instead of tracking an opaque encrypted ID. Named stores make this possible.

### Creating Named Stores

Create a store with a name attached:

```bash
./bbd create-named -data '{"items": []}' shopping-cart
```

You still get back a store ID, but now there's also a name mapping. The name is scoped to your customer ID, so different customers can use the same name without conflict.

If you try to create a named store that already exists, you'll get an error. But sometimes you want "get or create" semantics—use the existing store if it exists, create it if it doesn't. Add `--reuse` for that:

```bash
./bbd create-named -reuse -data '{"default": true}' shopping-cart
```

If `shopping-cart` already exists, you get back its store ID. If not, a new store gets created with your data.

### Looking Up Names

Once you have a named store, you can look up its ID:

```bash
STORE_ID=$(./bbd lookup shopping-cart)
```

This is useful if you only have the name and need the ID to perform other operations.

### Deleting Named Stores

Delete both the store and the name mapping:

```bash
./bbd delete-named shopping-cart
```

This is a two-step operation internally—it deletes the store and removes the name registry entry. If the store doesn't exist, that's fine (idempotent). If the name doesn't exist, that's also fine.

## Using the HTTP API

Everything the CLI does, it does by calling the HTTP API over the Unix socket. You can call this API directly if you prefer, or if you're writing code that needs to integrate with Big Bunny.

The base URL doesn't matter since you're using a Unix socket—just use `http://localhost` and the socket will handle everything.

### Creating Stores

Send a POST request to `/api/v1/create` with your data in the body:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  -H "BigBunny-Not-Valid-After: 3600" \
  -d "session data" \
  http://localhost/api/v1/create
```

The `X-Customer-ID` header identifies your customer. In production, this would come from parsing your authentication token. In development, you can set it to anything.

The `BigBunny-Not-Valid-After` header specifies the TTL in seconds. After one hour (3600 seconds), this store will expire.

The response body contains your store ID.

### Reading Stores

Send a POST request to `/api/v1/snapshot/{store-id}`:

```bash
STORE_ID="v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9..."

curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  http://localhost/api/v1/snapshot/$STORE_ID
```

The response body contains the store contents. The `BigBunny-Not-Valid-After` response header tells you how many seconds remain before expiration.

### Updating Stores

The one-shot update is a POST to `/api/v1/update/{store-id}`:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  -H "BigBunny-Not-Valid-After: 7200" \
  -d "updated data" \
  http://localhost/api/v1/update/$STORE_ID
```

You can change both the contents and the TTL in one operation.

### Deleting Stores

Send a POST to `/api/v1/delete/{store-id}`:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  http://localhost/api/v1/delete/$STORE_ID
```

Deletion is idempotent, so you'll get 200 OK even if the store doesn't exist.

### The Modify Protocol Over HTTP

The modify protocol works the same way over HTTP, you're just dealing with headers instead of command-line flags.

Begin modify with a POST to `/api/v1/begin-modify/{store-id}`:

```bash
curl -D response-headers.txt -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  http://localhost/api/v1/begin-modify/$STORE_ID
```

Use `-D response-headers.txt` to save the response headers. You'll need to extract the `BigBunny-Lock-ID` header for the next step.

```bash
LOCK_ID=$(grep "BigBunny-Lock-ID:" response-headers.txt | cut -d' ' -f2 | tr -d '\r')
```

Complete the modification with a POST to `/api/v1/complete-modify/{store-id}`:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  -H "BigBunny-Lock-ID: $LOCK_ID" \
  -d "modified data" \
  http://localhost/api/v1/complete-modify/$STORE_ID
```

If you want to cancel instead, POST to `/api/v1/cancel-modify/{store-id}` with the same lock ID.

### Named Store Operations

Create a named store with POST to `/api/v1/create-by-name/{name}`:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  -H "BigBunny-Not-Valid-After: 3600" \
  -d '{"cart": []}' \
  http://localhost/api/v1/create-by-name/shopping-cart
```

For get-or-create semantics, add the `BigBunny-Reuse-If-Exists` header:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  -H "BigBunny-Reuse-If-Exists: true" \
  -d '{"cart": []}' \
  http://localhost/api/v1/create-by-name/shopping-cart
```

Look up a name with POST to `/api/v1/lookup-id-by-name/{name}`:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  http://localhost/api/v1/lookup-id-by-name/shopping-cart
```

Delete a named store with POST to `/api/v1/delete-by-name/{name}`:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  http://localhost/api/v1/delete-by-name/shopping-cart
```

## Checking Status

You can ask Big Bunny about its current state at any time:

```bash
./bbd status
```

This shows you the node's role (primary, secondary, or joining), how many stores it's managing, memory usage, replication queue depths, and information about peer nodes.

For machine-readable output, add `--json`:

```bash
./bbd status -json | jq .
```

The JSON includes all the same information in structured form, which is useful for monitoring systems.

## Understanding Errors

When something goes wrong, Big Bunny returns an HTTP status code and includes a machine-readable error code in the `BigBunny-Error-Code` header. This lets you handle errors programmatically instead of parsing error messages.

### Common Error Codes

**NotFound (404)**: The store or name doesn't exist. Either it was deleted, it expired, or you have the wrong ID. This isn't retryable—you need to create a new store.

**StoreLocked (409)**: Someone else holds the lock on this store. You can retry after a brief delay. The lock will either be released or will timeout after 500 milliseconds.

**LockMismatch (409)**: The lock ID you provided doesn't match the lock currently held. Either you have the wrong lock ID, the lock timed out, or someone else grabbed it. This usually isn't retryable—you need to begin-modify again to get a fresh lock.

**LockStateUnknown (409)**: The primary just failed over and isn't sure about lock state yet. Retry after the time specified in the `Retry-After` header (usually 1 second). This is temporary—after 500 milliseconds post-failover, lock state becomes certain again.

**LeaderChanged (503)**: Automatic request forwarding to the primary failed (typically due to network issues or primary unavailability). This is rare since requests are forwarded internally. Retry after the time specified in `Retry-After`.

**StoreUnavailable (503)**: The node is recovering and isn't ready to serve requests yet. Retry after the time in `Retry-After`. This happens briefly after a node joins or rejoins the cluster.

**CapacityExceeded (507)**: The memory limit has been reached. You can't create new stores until existing ones expire or get deleted. This usually isn't retryable unless you know stores are about to expire.

### Retry Logic

For errors that are retryable, check the `Retry-After` header:

```bash
HTTP/1.1 503 Service Unavailable
BigBunny-Error-Code: LeaderChanged
Retry-After: 1
```

Wait the specified number of seconds, then retry. Don't retry immediately—you'll just get the same error again.

Here's a shell script that demonstrates retry logic:

```bash
#!/bin/bash

STORE_ID="v1:0:8ahePLwi..."
MAX_RETRIES=3
RETRY=0

while [ $RETRY -lt $MAX_RETRIES ]; do
  HTTP_CODE=$(curl -w "%{http_code}" -s -o /tmp/response \
    --unix-socket /tmp/bbd.sock \
    -H "X-Customer-ID: my-app" \
    -d "data" \
    http://localhost/api/v1/update/$STORE_ID)

  if [ "$HTTP_CODE" = "200" ]; then
    cat /tmp/response
    exit 0
  elif [ "$HTTP_CODE" = "503" ]; then
    echo "Retrying after error..."
    RETRY=$((RETRY + 1))
    sleep 1
  else
    echo "Error: HTTP $HTTP_CODE"
    cat /tmp/response
    exit 1
  fi
done

echo "Max retries exceeded"
exit 1
```

### Warnings

Some operations succeed but include warnings in the `BigBunny-Warning` header.

**DegradedWrite**: The write succeeded on the primary, but the secondary is unreachable. Your data is safe on the primary, but it's not replicated yet. If the primary fails before replication recovers, you might lose this write.

```bash
HTTP/1.1 200 OK
BigBunny-Warning: DegradedWrite
```

You got a successful response, but you should monitor your cluster health. If you see lots of `DegradedWrite` warnings, something is wrong with replication.

## Practical Patterns

Let's look at some common patterns you'll use when building applications on Big Bunny.

### Rate Limiting Counters

Implement a per-user rate limiter that allows 100 requests per minute:

```bash
USER_ID="user123"
COUNTER=$(./bbd create-named -data "0" -ttl 60 -reuse "${USER_ID}-rate-limit")

# On each request, increment the counter
LOCK=$(./bbd begin-modify $COUNTER)
COUNT=$(./bbd get $COUNTER)

if [ "$COUNT" -ge 100 ]; then
  ./bbd cancel-modify $COUNTER -lock "$LOCK"
  echo "Rate limit exceeded"
  exit 1
fi

NEW_COUNT=$((COUNT + 1))
./bbd complete-modify -lock "$LOCK" -data "$NEW_COUNT" $COUNTER
```

The counter expires after 60 seconds (the TTL), which resets the limit automatically.

### Shopping Cart

Maintain a shopping cart that persists across requests:

```bash
USER_ID="user456"
CART_ID=$(./bbd create-named -data '{"items":[]}' -ttl 86400 -reuse "${USER_ID}-cart")

# Add an item (this would be JSON manipulation in a real app)
LOCK=$(./bbd begin-modify $CART_ID)
CURRENT=$(./bbd get $CART_ID)
# ... parse JSON, add item, serialize ...
NEW_CART='{"items":["item1","item2"]}'
./bbd complete-modify -lock "$LOCK" -data "$NEW_CART" $CART_ID
```

The cart expires after 24 hours (86400 seconds), which is reasonable for shopping sessions.

### Session Storage

Store user session state:

```bash
SESSION_ID=$(uuidgen)
SESSION_DATA='{"user_id":"123","logged_in":true,"preferences":{}}'

STORE_ID=$(./bbd create -data "$SESSION_DATA" -ttl 3600)

# Put the store ID in a cookie so subsequent requests can access it
echo "Set-Cookie: session=$STORE_ID; Max-Age=3600; HttpOnly; Secure"
```

When subsequent requests come in, extract the store ID from the cookie and read the session:

```bash
SESSION=$(./bbd get "$STORE_ID")
```

### TTL Management

Big Bunny doesn't extend TTLs automatically when you access a store. If you want sliding expiration (reset the TTL on each access), you need to update the TTL explicitly:

```bash
# Read the store
DATA=$(./bbd get $STORE_ID)

# Update with a fresh TTL
curl -X POST --unix-socket /tmp/bbd.sock -H "X-Customer-ID: $CUSTOMER_ID" -H "BigBunny-Not-Valid-After: 3600" -d "$DATA" http://localhost/api/v1/update/$STORE_ID
```

This gives you another hour from now. For high-traffic applications, you might only reset the TTL occasionally (say, every 10 minutes) rather than on every access, to reduce write load.

## Best Practices

Here are some patterns that work well with Big Bunny.

Use named stores sparingly. Named stores require registry lookups, which are slightly slower than direct store ID access. For high-traffic paths, use anonymous stores and cache the store ID somewhere (like a cookie or session).

Set appropriate TTLs. The default 14-day TTL is generous. If your data doesn't need to live that long, set a shorter TTL. This reduces memory pressure and allows for faster key rotation.

Handle locks carefully. You have 500 milliseconds from begin-modify to complete-modify. That's plenty of time for typical operations, but don't do slow work while holding a lock. If you need to call external services or do complex processing, do it before acquiring the lock.

Always handle errors. Network errors, timeouts, and transient failures happen. Make sure your code handles `StoreLocked`, `LeaderChanged`, and `LockStateUnknown` errors gracefully with retry logic.

Connect to any node. Big Bunny automatically forwards write requests to the primary internally, so you can connect to any node in the cluster. The forwarding is transparent with connection pooling for efficiency. If forwarding fails (rare), you'll get a `LeaderChanged` error and should retry.

Check capacity before scaling. Monitor memory usage and watch for `CapacityExceeded` errors. When you're approaching your memory limit, it's time to increase `--memory-limit` or add capacity.

## Administrative Commands

These commands are for operators, not for normal application use.

### Force Promotion

Manually promote a secondary to primary:

```bash
./bbd promote -uds=/var/run/bbd/bbd.sock
```

This immediately increments the epoch and makes the node primary. Use this only during emergency failover when you know the old primary is dead. If you use it carelessly, you can cause split-brain.

### Release Stuck Lock

Force-release a lock without normal checks:

```bash
./bbd release-lock <store-id> -uds=/var/run/bbd/bbd.sock
```

This clears the lock unconditionally, bypassing customer verification and lock ID checks. Only use this when a lock is genuinely stuck due to a bug. If you use it while a legitimate client holds the lock, you'll break that client's modify operation.

## What's Next

The [API Reference](api-reference.md) documents every endpoint and header in detail. The [Architecture](architecture.md) explains how the lock protocol and replication work under the hood. And the [Operations Guide](operations.md) covers monitoring, capacity planning, and troubleshooting in production.
