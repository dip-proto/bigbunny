# Quick Start Guide

Let's get you up and running with Big Bunny in about five minutes. We'll start a single node, create some stores, and see how the basic operations work. By the end, you'll have a good feel for how Big Bunny works and be ready to explore more advanced features.

## Starting Big Bunny

First, start a single Big Bunny instance in development mode. Development mode uses weak encryption keys that are predictable and deterministic, which makes it perfect for local testing but completely unsuitable for production.

```bash
./bbd --dev --host-id=node1 --tcp=:8081 --uds=/tmp/bbd.sock
```

You should see Big Bunny start up and print some information about its configuration. The output will tell you it's in development mode, listening on port 8081 for replication traffic, and accepting local API requests through a Unix socket at `/tmp/bbd.sock`. Since you haven't configured any peers, it'll note that it's running as a solo primary node.

Keep this terminal open. Big Bunny is now running and ready to accept requests. Open a new terminal for the commands that follow.

## Creating Your First Store

Let's create a simple session store using the command-line interface. The `create` command takes your data and gives you back an encrypted store identifier.

```bash
./bbd create -data "Hello, Big Bunny!"
```

You'll get back something that looks like `v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9...`. This is your store ID. It's encrypted, which means you can't see what's inside it, and that's intentional. The encryption prevents you from tampering with routing information or accessing stores that belong to other customers.

Save this store ID because you'll need it for the next few steps. In a real application, you'd typically stash it in a cookie or return it to your client.

## Reading the Store Back

Now let's retrieve what we just stored. Use the `get` command with your store ID.

```bash
./bbd get v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9...
```

You should see your original data printed back: `Hello, Big Bunny!`. That's the basic read operation. It's fast because everything is in memory, and it doesn't acquire any locks, so multiple readers can access the same store simultaneously without interfering with each other.

## Modifying a Store Safely

Here's where Big Bunny gets interesting. When you need to modify a store, you use a lock-based protocol that prevents race conditions. Let's walk through the three-step process.

First, you acquire a lock on the store and retrieve its current contents. This is called beginning a modify operation.

```bash
LOCK=$(./bbd begin-modify v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9...)
```

The system gives you back a lock token and shows you the current contents. The lock ensures that nobody else can modify this store while you're working on it. You have 500 milliseconds to complete your changes before the lock automatically expires.

Now you can compute your new value based on what you read. In a real application, you might increment a counter, add an item to a shopping cart, or update session state. For this example, let's just replace the contents with something new.

```bash
./bbd complete-modify -lock "$LOCK" -data "Updated content!" v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9...
```

The store is now updated, and the lock is released. If something went wrong and you wanted to abort instead, you'd use `cancel-modify` with the same lock token.

## The One-Shot Update

If you don't need to read the current contents before writing, there's a simpler alternative. The one-shot update endpoint acquires the lock, writes your new data, and releases the lock all in one operation.

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  -d "One-shot update!" \
  http://localhost/api/v1/update/v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9...
```

This is perfect for cases where you're just replacing the entire store contents and don't care what was there before.

## Named Stores

Sometimes you want to refer to a store by a memorable name instead of tracking an opaque encrypted identifier. That's what named stores are for. Let's create one for a shopping cart.

```bash
./bbd create --name shopping-cart --data '{"items": []}'
```

You still get back a store ID, but now you can look it up later by name.

```bash
./bbd lookup shopping-cart
```

This returns the store ID associated with that name. The name is scoped to your customer ID, so different customers can use the same name without conflict. Behind the scenes, Big Bunny maintains a registry that maps names to store IDs, but you don't need to think about that. You just use names when they're convenient and IDs when you already have them.

## Using the HTTP API Directly

Everything the CLI does, it does by calling the HTTP API over the Unix socket. You can call that API directly with `curl` if you prefer. Here's how you'd create a store.

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  -H "BigBunny-Not-Valid-After: 3600" \
  -d "session data" \
  http://localhost/api/v1/create
```

The `X-Customer-ID` header identifies which customer this request belongs to. In a real deployment, this would come from your authentication token, but in development mode you can set it to anything. The `BigBunny-Not-Valid-After` header specifies the time-to-live in seconds. After an hour, this store will expire and be garbage collected.

Reading works similarly. Just send a POST request to the snapshot endpoint.

```bash
STORE_ID="v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9..."

curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  http://localhost/api/v1/snapshot/$STORE_ID
```

The modify protocol over HTTP looks just like the CLI version, except you're dealing with HTTP headers instead of command-line flags.

```bash
# Begin the modify operation
curl -D - -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  http://localhost/api/v1/begin-modify/$STORE_ID

# Look for the BigBunny-Lock-ID header in the response
# Then complete the modification with the lock ID

curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: my-app" \
  -H "BigBunny-Lock-ID: <lock-id-from-previous-response>" \
  -d "modified data" \
  http://localhost/api/v1/complete-modify/$STORE_ID
```

## Checking Node Status

You can ask Big Bunny about its current state at any time.

```bash
./bbd status
```

This shows you which role the node is playing (primary, secondary, or joining), how many stores it's managing, how much memory it's using, and information about its peers. Add `--json` if you want machine-readable output.

## A Practical Example: Rate Limiting Counters

Let's put it all together with a realistic example. Imagine you're implementing rate limiting and need to track how many requests a user has made.

```bash
# Create a counter for a user
COUNTER=$(./bbd create -data "0")

# Later, when a request comes in, increment it
LOCK=$(./bbd begin-modify $COUNTER)
CURRENT=$(./bbd get $COUNTER)
NEW_VALUE=$((CURRENT + 1))
./bbd complete-modify -lock "$LOCK" -data "$NEW_VALUE" $COUNTER
```

The lock ensures that even if two requests try to increment the counter simultaneously, they won't step on each other. One will acquire the lock, increment the counter, and release it. The other will wait for the lock, then increment the already-updated value.

## Trying Failover with Two Nodes

Want to see automatic failover in action? Start a second Big Bunny node and watch them form a cluster. Open two more terminal windows.

In the first new terminal, start node1 as the primary.

```bash
./bbd --dev --host-id=node1 --tcp=:8081 --uds=/tmp/bbd1.sock \
  --peers=node2@localhost:8082
```

In the second new terminal, start node2 as the secondary.

```bash
./bbd --dev --host-id=node2 --tcp=:8082 --uds=/tmp/bbd2.sock \
  --peers=node1@localhost:8081
```

The node with the lexicographically smaller ID (node1) becomes the primary. Now create a store through node1.

```bash
./bbd create -uds=/tmp/bbd1.sock -data "replicated data"
```

Check node2's status to confirm it received the replication.

```bash
./bbd status -uds=/tmp/bbd2.sock
```

You should see one store. Now kill node1 (Ctrl+C in its terminal) and watch node2 promote itself. After about four seconds, check node2's status again.

```bash
./bbd status -uds=/tmp/bbd2.sock
```

The role should have changed from secondary to primary. Your store is still there, and requests that previously went to node1 can now be routed to node2. That's automatic failover. No manual intervention required, no data loss as long as replication finished before node1 died.

## Understanding Errors

When something goes wrong, Big Bunny tells you about it using error codes in the response headers. Let's provoke a few errors to see how they work.

Try to acquire a lock that's already held. In one terminal, begin a modify operation and don't complete it immediately.

```bash
LOCK=$(./bbd begin-modify $STORE_ID)
```

In another terminal, try to begin another modify on the same store.

```bash
./bbd begin-modify $STORE_ID
```

You'll get a `StoreLocked` error. The store is locked by your first request, and the second request can't proceed until either the lock is released or it times out after 500 milliseconds.

Try to read a store that doesn't exist.

```bash
./bbd get v1:0:invalid-store-id
```

You'll get a `NotFound` error. The error handling is designed to be clear and machine-readable, with error codes that tell you exactly what went wrong and whether retrying makes sense.

## Where to Go Next

You now know the basics of working with Big Bunny. You've created stores, read them, modified them safely with locks, and even seen automatic failover in action.

The [Usage Guide](usage.md) covers every command and API endpoint in detail, including all the options and error cases. The [API Reference](api-reference.md) documents the complete HTTP interface. If you want to understand how replication and failover work under the hood, check out the [Architecture](architecture.md) document. And when you're ready to deploy this in production, the [Operations](operations.md) guide will walk you through everything you need to know about monitoring, capacity planning, and troubleshooting.

Big Bunny's API is deliberately simple. There aren't many operations to learn, and the ones that exist behave predictably. Once you've run through this quick start, you've seen most of what there is to see. The rest is just details and edge cases.
