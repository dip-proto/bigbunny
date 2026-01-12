# Installation Guide

Getting Big Bunny up and running is straightforward. This guide will walk you through everything from building the binary to setting up a production cluster with proper encryption and security.

## What You'll Need

Before you start, make sure you have Go 1.21 or later installed on your system. Big Bunny is written in Go and doesn't have any unusual dependencies beyond the standard toolchain. You'll also want `openssl` handy for generating encryption keys, though that's optional if you're just testing things out in development mode.

The system requirements are modest. Big Bunny runs happily on Linux or macOS with at least 512MB of RAM per node, though you'll want to allocate more based on how many stores you plan to keep in memory. You'll also need TCP connectivity between cluster nodes if you're running a multi-node setup.

## Building from Source

Start by cloning the repository and building the binary. Big Bunny is written in Go, so you'll need Go 1.21 or later installed.

```bash
git clone https://github.com/bigbunny/bbd.git
cd bbd
```

The simplest way to build is with Make:

```bash
make
```

This produces a `bbd` binary in your current directory. If you don't have `make` installed or prefer to use Go directly, that works too:

```bash
go build ./cmd/bbd
```

Either way, you'll end up with a single binary called `bbd` that contains both the daemon and the command-line client tools. Check that it works by asking for help.

```bash
./bbd --help
```

You should see usage information listing all the available commands and flags.

## Configuring for Development vs Production

Big Bunny needs three secrets: encryption keys to secure store IDs, a routing secret to prevent targeted host attacks, and an internal token to authenticate replication traffic between nodes. During development, you can use the `--dev` flag to skip this configuration and use weak, deterministic values. This is convenient for testing but completely insecure, so never use it in production.

For production, you'll need to generate real secrets. The encryption keys should be 32 bytes of random data (required for AES-128-SIV), typically represented as 64 hexadecimal characters:

```bash
openssl rand -hex 32
```

This produces something like `a1b2c3d4e5f6...` (64 hex characters representing 32 bytes). You'll also need a routing secret (32 bytes) to prevent attackers from pre-computing which stores map to which hosts:

```bash
openssl rand -hex 32
```

And an internal token for authenticating replication traffic between nodes:

```bash
openssl rand -hex 16
```

This gives you a 16-byte (128-bit) random token. Save all three secrets somewhere secure—you'll need them for every node in your cluster.

### Development Mode

For local testing and development, Big Bunny includes a development mode that uses weak, deterministic keys. This makes it easy to get started without generating real keys, but it's completely unsuitable for production because the keys are predictable.

To start in development mode, just add the `--dev` flag:

```bash
./bbd --dev --host-id=node1 --tcp=:8081 --uds=/tmp/bbd.sock
```

Development mode is convenient, but don't ever use it in production. The encryption keys are deterministic and published in the source code, which means anyone can decrypt your store IDs.

### Production Configuration

In production, you need to provide your own encryption keys and internal authentication token. Start by generating a strong encryption key. This is just a 32-byte random value (required for AES-128-SIV), which you'll represent as 64 hexadecimal characters.

```bash
openssl rand -hex 32
```

You'll see something like `a1b2c3d4...` (64 hex characters representing 32 bytes). This is your master encryption key. Guard it carefully. If someone gets this key, they can decrypt all your store IDs.

You also need an internal token for authenticating replication traffic between nodes.

```bash
openssl rand -hex 16
```

This generates a 16-byte (128-bit) token, which is plenty for a shared secret.

Now you can start Big Bunny in production mode. You'll need to provide these keys explicitly, either as command-line flags or environment variables. Most people prefer environment variables because they don't show up in process listings.

```bash
export SERIALD_STORE_KEYS="0:a1b2c3d4e5f6789012345678901234567890123456789012345678901234"
export SERIALD_STORE_KEY_CURRENT=0
export SERIALD_INTERNAL_TOKEN="f1e2d3c4b5a6789012345678"

./bbd --host-id=node1 --tcp=:8081 --uds=/tmp/bbd.sock
```

The key ID (the `0:` part) can be any identifier from 1 to 16 characters, starting with an alphanumeric character. Simple numbers like `0` and `1` work fine. You might use something like `2024q1` if you're rotating keys quarterly and want the IDs to be meaningful to humans.

## Setting Up a Cluster

A production cluster needs at least two nodes for failover. Setting this up is straightforward because both nodes use almost identical configuration—they just need to know about each other.

Let's say you're setting up two nodes in your New York datacenter. Node 1 will become the primary (because `node1` comes before `node2` alphabetically), and node 2 will be the secondary.

On the first node, you'd start Big Bunny like this:

```bash
./bbd \
  --host-id=node1 \
  --site=nyc01 \
  --tcp=:8081 \
  --uds=/var/run/bbd/bbd.sock \
  --peers=node2@10.0.1.2:8082 \
  --memory-limit=4294967296 \
  --store-keys="$STORE_KEYS" \
  --store-key-current=0 \
  --internal-token="$INTERNAL_TOKEN"
```

On the second node, you'd use nearly identical configuration, just swapping the host ID and adjusting the TCP port and peer list. The node with the lexicographically smaller ID becomes primary, so if you name your nodes `node1` and `node2`, node1 will naturally become the primary.

### Key Generation

Before you can run Big Bunny in production, you need to generate encryption keys. These keys protect store IDs from tampering and ensure customer isolation. Each key is 32 bytes of random data (required for AES-128-SIV), expressed as 64 hexadecimal characters.

Generate a key using OpenSSL:

```bash
openssl rand -hex 32
```

This produces something like `a1b2c3d4e5f6789012345678901234567890123456789012345678901234`. That's your master encryption key. Guard it carefully. If someone gets this key, they can decrypt all store IDs and potentially access stores they shouldn't.

You'll also need an internal authentication token for communication between Big Bunny nodes. This can be shorter since it's just used for authentication, not encryption.

```bash
openssl rand -hex 16
```

Store these securely. Don't put them in Git, don't put them in shell history, don't log them. Use a secrets management system if you have one, or at minimum, store them in files with restrictive permissions.

## Configuration Approaches

There are three ways to configure Big Bunny, and you should choose based on your deployment environment.

For development and testing, you can use the `--dev` flag, which uses deterministic encryption keys that are the same every time. This is convenient for local testing because you don't have to manage keys, but it's completely insecure. Never, ever use development mode in production.

For production, you have two options for providing configuration. You can pass everything via command-line flags, which works but means your keys are visible in process listings. Anyone who can run `ps` on the machine can see your encryption keys, which is not ideal.

The better approach is to use environment variables. Generate your keys securely, put them in a file with restricted permissions, and load them as environment variables when starting the daemon. This keeps them out of process listings and makes it easy to integrate with secrets management systems.

## Setting Up a Single Node

If you're just testing or running in a non-critical environment, a single node is the simplest way to get started. Build the binary first:

```bash
make
```

This produces a `bbd` binary in your current directory. If you don't have `make` installed, you can build directly with Go:

```bash
go build ./cmd/bbd
```

For production deployments, you'll need to generate encryption keys. These are 32-byte values (required for AES-128-SIV) used for encrypting store IDs. Generate them with OpenSSL:

```bash
openssl rand -hex 32
```

You'll get back 64 hexadecimal characters. That's your encryption key. Generate a second one for the internal authentication token:

```bash
openssl rand -hex 16
```

Now start Big Bunny with these keys:

```bash
./bbd \
  --host-id=node1 \
  --tcp=:8081 \
  --uds=/tmp/bbd.sock \
  --store-keys="0:$(openssl rand -hex 32)" \
  --store-key-current=0 \
  --routing-secret="$(openssl rand -hex 32)" \
  --internal-token="$(openssl rand -hex 16)"
```

The `--host-id` identifies this node uniquely. The `--tcp` flag specifies where to listen for replication traffic (you need this even for a single node because the architecture assumes it). The `--uds` flag sets the Unix socket path where the local API will be available.

If you want to use environment variables instead (recommended), create a file with your secrets:

```bash
# /etc/bigbunny/secrets.env
export SERIALD_STORE_KEYS="0:$(openssl rand -hex 32)"
export SERIALD_STORE_KEY_CURRENT=0
export SERIALD_ROUTING_SECRET="$(openssl rand -hex 32)"
export SERIALD_INTERNAL_TOKEN="$(openssl rand -hex 16)"
```

Make sure this file is readable only by the user running Big Bunny:

```bash
chmod 600 /etc/bigbunny/secrets.env
```

Then source it and start the daemon:

```bash
source /etc/bigbunny/secrets.env
./bbd --host-id=node1 --tcp=:8081 --uds=/tmp/bbd.sock
```

## Setting Up a Two-Node Cluster

The real value of Big Bunny comes from having two nodes that replicate to each other and fail over automatically. Setting this up requires a bit more coordination, but it's not complicated.

First, decide which node will be the primary. This is deterministic based on host IDs—the lexicographically smaller ID becomes primary. So if you're setting up `node1` and `node2`, node1 will be primary because it sorts before node2.

Both nodes need to share the same encryption keys, routing secret, and internal token. Generate these once and use them on both nodes:

```bash
# Generate once, use everywhere
STORE_KEY=$(openssl rand -hex 32)
ROUTING_SECRET=$(openssl rand -hex 32)
INTERNAL_TOKEN=$(openssl rand -hex 16)
```

On the first node (which will become primary):

```bash
./bbd \
  --host-id=node1 \
  --tcp=:8081 \
  --uds=/tmp/bbd.sock \
  --peers=node2@10.0.1.2:8082 \
  --store-keys="0:$STORE_KEY" \
  --store-key-current=0 \
  --routing-secret="$ROUTING_SECRET" \
  --internal-token="$INTERNAL_TOKEN"
```

On the second node (which will become secondary):

```bash
./bbd \
  --host-id=node2 \
  --tcp=:8082 \
  --uds=/tmp/bbd.sock \
  --peers=node1@10.0.1.1:8081 \
  --store-keys="0:$STORE_KEY" \
  --store-key-current=0 \
  --routing-secret="$ROUTING_SECRET" \
  --internal-token="$INTERNAL_TOKEN"
```

The `--peers` flag tells each node about the other. The format is `id@address:port`. If you're running on the same machine for testing, use `localhost` for both addresses but different ports:

```bash
# Terminal 1
./bbd --dev --host-id=node1 --tcp=:8081 --uds=/tmp/bbd1.sock \
  --peers=node2@localhost:8082

# Terminal 2
./bbd --dev --host-id=node2 --tcp=:8082 --uds=/tmp/bbd2.sock \
  --peers=node1@localhost:8081
```

Notice we used `--dev` here. That's a shortcut that enables development mode with weak, deterministic keys. Never use this in production, but it's fine for local testing.

## Key Rotation

Over time, you'll want to rotate your encryption keys. Maybe it's a security requirement, maybe a key got compromised, or maybe you're just being cautious. Big Bunny supports key rotation without downtime.

The trick is that you can have multiple keys configured simultaneously. New stores use the current key, but stores encrypted with old keys remain readable.

Let's say you currently have key `0` as your only key. Generate a new key and give it ID `1`:

```bash
NEW_KEY=$(openssl rand -hex 32)
```

Update your configuration to include both keys and set the current one to the new key:

```bash
./bbd \
  --host-id=node1 \
  --tcp=:8081 \
  --uds=/tmp/bbd.sock \
  --store-keys="0:$OLD_KEY,1:$NEW_KEY" \
  --store-key-current=1 \
  --routing-secret="$ROUTING_SECRET" \
  --internal-token="$INTERNAL_TOKEN"
```

Deploy this to all nodes. New stores will be encrypted with key `1`, but stores encrypted with key `0` can still be decrypted and read.

Wait for all stores encrypted with the old key to expire. The default TTL is 14 days, so after two weeks, all the old stores are gone. Now you can remove the old key from your configuration:

```bash
./bbd \
  --host-id=node1 \
  --tcp=:8081 \
  --uds=/tmp/bbd.sock \
  --store-keys="1:$NEW_KEY" \
  --store-key-current=1 \
  --routing-secret="$ROUTING_SECRET" \
  --internal-token="$INTERNAL_TOKEN"
```

If a key is compromised and you need to rotate immediately, you can force-expire old stores, but that's an operational decision with consequences. Any clients holding store IDs encrypted with the old key will get errors when they try to use them.

## Memory Limits

Big Bunny keeps everything in RAM, which is great for performance but means you need to think about memory capacity. Set a memory limit to prevent the daemon from consuming all available memory:

```bash
./bbd --memory-limit=4294967296  # 4 GB
```

When you hit this limit, new stores can't be created. Existing stores continue working, but create operations return a `CapacityExceeded` error. This is a hard limit—there's no spillover to disk or fancy eviction policy. You either have room for the store or you don't.

For capacity planning, figure that each store needs about 4KB total (2KB for the body plus ~2KB for metadata). A 4GB limit gives you roughly one million stores per node. Remember that stores are replicated, so with RF=2, you're storing two copies of everything.

## File Permissions

The Unix socket is created with restrictive permissions by default (0600, meaning only the owner can read and write). If you need other users or processes to access the API, you can adjust this.

After starting Big Bunny, change the permissions:

```bash
chmod 660 /tmp/bbd.sock
chown :mygroup /tmp/bbd.sock
```

Now anyone in `mygroup` can access the API. Be thoughtful about this—anyone who can access the socket can operate on any customer's stores (customer isolation is enforced by the API, but it assumes the socket itself is secured).

A better approach is to run your client processes as the same user that runs Big Bunny. This avoids permission issues entirely.

## Verifying the Installation

After starting Big Bunny, verify that it's working by checking its status:

```bash
./bbd status --uds=/tmp/bbd.sock
```

You should see output showing the node ID, role (primary if solo, or primary/secondary in a cluster), epoch number, number of stores (probably zero initially), and memory usage.

Try creating a store:

```bash
curl -X POST --unix-socket /tmp/bbd.sock \
  -H "X-Customer-ID: test-customer" \
  -d "hello world" \
  http://localhost/api/v1/create
```

You'll get back an encrypted store ID. That's your confirmation that everything is working.

## Common Installation Issues

If Big Bunny refuses to start and complains about invalid store keys, check that your keys are exactly 64 hex characters (32 bytes). The output from `openssl rand -hex 32` is the right format.

If you see "bind: address already in use", something else is using the TCP port. Either kill that process or choose a different port with `--tcp=:8082` or whatever is available.

If you can't access the Unix socket, check the file permissions. The socket file should be readable and writable by your user. If you're running the client as a different user than the daemon, you'll need to adjust permissions or add both users to a shared group.

If nodes in a cluster can't connect to each other, verify the network. Can you telnet from one node to the other on the replication port? Are there firewalls blocking the connection? Make sure the `--peers` addresses are correct and reachable.

## What's Next

Now that Big Bunny is installed, the [Quick Start](quickstart.md) guide will walk you through basic operations. If you're ready to deploy in production, the [Operations Guide](operations.md) covers monitoring, capacity planning, and troubleshooting. And if something goes wrong, check the logs—Big Bunny logs errors and warnings that usually point you in the right direction.
