# Architecture

This document explains how Big Bunny actually works under the hood. We'll walk through the algorithms, protocols, and design decisions that make it tick. If you're curious about why things work the way they do, or if you need to debug complex issues in production, this is where you'll find those answers.

## System Overview

At its core, Big Bunny is built on four key ideas working together. First, asynchronous replication keeps write latency low by not waiting for network round trips. Second, lease-based failover handles host failures automatically without requiring manual intervention. Third, epoch fencing prevents old primaries from causing problems after a failover. And fourth, lock-based serialization ensures that concurrent updates never step on each other.

These pieces fit together to create a system that's fast, reliable, and surprisingly simple once you understand how each part works. The design deliberately avoids distributed consensus protocols like Raft or Paxos. Instead, it makes pragmatic trade-offs that work well for session storage where some data loss is acceptable but speed matters a lot.

## How Replication Works

Every store in Big Bunny lives on exactly two nodes within the same Point of Presence. One node is the primary, which handles all writes. The other is the secondary, which receives updates asynchronously and can serve reads (though they might be slightly stale).

When you write to Big Bunny, the write goes to the primary. The primary applies your change locally, assigns it a version number, and immediately returns success to you. In the background, completely asynchronously, it queues a replication message to send to the secondary. The secondary receives this message, checks the version number to ensure it's newer than what it has, and applies the update.

This async approach is what makes writes so fast. Your write completes in microseconds because it's just an in-memory operation on the primary. There's no waiting for network round trips, no coordinating with other nodes, no consensus quorum. The trade-off is that if the primary dies before the secondary receives the replication message, that write is lost. For session storage, this is usually an acceptable trade-off.

The version numbers are crucial here. Every store has a monotonically increasing version counter. When the primary modifies a store, it increments the version. When the secondary receives a replication message, it only applies updates with a version higher than what it currently has. This makes replication idempotent—if the same message arrives twice, the second one is harmlessly ignored.

### Choosing Which Nodes Store What

Big Bunny uses rendezvous hashing (also called highest random weight hashing) to decide which two nodes should store each replica. This is a deterministic algorithm that produces the same answer every time you ask it, without requiring any centralized coordination.

The way it works is simple: for each candidate node, you compute a hash of the store's shard ID combined with the node's ID. The node with the highest hash becomes the primary. The node with the second-highest hash becomes the secondary. Because hashing is deterministic, everyone who performs this calculation arrives at the same answer.

What makes this elegant is what happens when nodes join or leave the cluster. Only the stores whose chosen nodes actually changed need to move. If you add a third node, most stores stay exactly where they are. Only the ones that would now prefer the new node need to relocate. This minimizes churn and keeps things simple.

## Primary Election

In Big Bunny, becoming the primary isn't about winning an election—it's about being chosen by deterministic selection and then staying primary as long as you're healthy. When a cluster starts up, both nodes look at their host IDs and agree on which one should be primary. The node with the lexicographically smaller ID wins. So if you have `node1` and `node2`, node1 becomes primary simply because `node1` comes before `node2` alphabetically.

This deterministic approach solves a really important problem: it prevents split-brain at startup. Both nodes run the same algorithm and arrive at the same conclusion about who should be primary. There's no race condition, no need to coordinate, no possibility of both nodes thinking they're primary when they first start up.

But what happens when the primary fails? That's where lease-based promotion comes in.

### The Lease System

The primary continuously sends heartbeat messages to the secondary every 200 milliseconds. Each heartbeat is like the primary saying "I'm still alive and still in charge." The secondary tracks when it last heard from the primary and maintains a lease timer.

The lease duration is set to two seconds, which is ten times the heartbeat interval. This gives plenty of buffer for network hiccups and temporary delays. On top of that, there's an additional two-second grace period. So the secondary won't promote itself until at least four seconds have passed without hearing from the primary.

This conservative approach is deliberate. False positives (promoting when the primary is actually fine) are much worse than slightly slower failover. A false positive can cause split-brain situations where both nodes think they're primary. A slightly slower failover just means a few seconds of unavailability, which is acceptable for session storage.

When the lease finally expires and the grace period passes, the secondary promotes itself to primary. It increments a special counter called the epoch number, which we'll talk about next, and starts accepting writes. From that point forward, it's the new primary until it fails or something forces it to step down.

## Epoch Fencing

Here's a scenario that would be disastrous without epoch fencing: the primary appears to fail because of a network partition. The secondary promotes itself and starts accepting writes. Then the network partition heals. Now you have two primaries, both accepting writes, both thinking they're in charge. This is called split-brain, and it's a data corruption nightmare.

Epoch fencing prevents this. Every replica set has an epoch number that starts at 1 and increments each time there's a promotion. When the secondary promotes itself, it bumps the epoch to 2. All of its heartbeats now carry epoch 2.

When the network partition heals and the old primary (still thinking it's primary with epoch 1) receives a heartbeat with epoch 2, it realizes something has happened. It sees that there's a newer epoch out there, which means someone else has promoted themselves. The old primary immediately demotes itself to a joining state and starts the recovery protocol to catch up with the new primary.

The same logic applies to replication messages. If the old primary tries to send a replication message with epoch 1 to a secondary that's now at epoch 2, the secondary rejects it. The message is stale, from an old regime, and applying it could overwrite newer data.

This isn't perfect split-brain prevention. There's a window during the partition where both nodes think they're primary and might accept writes. But once the partition heals, epoch fencing ensures that only one survives. The node with the higher epoch wins, and the other one steps down gracefully.

## The Lock Protocol

When you need to modify a store, you use Big Bunny's three-phase lock protocol. It's designed to prevent race conditions while being simple enough that you can reason about it without a Ph.D. in distributed systems.

Phase one is beginning the modification. You send a `begin-modify` request to the primary. The primary checks if anyone else holds the lock on this store. If not, it generates a random lock ID (a UUID), marks the store as locked, sets a timeout for 500 milliseconds in the future, and returns the lock ID along with the current store contents.

Phase two is where you do your work. You've got the current contents, you can compute what the new contents should be, and you have up to 500 milliseconds to finish. In practice, this usually takes just a few milliseconds unless you're doing something complex.

Phase three is completing the modification. You send a `complete-modify` request with your lock ID and the new contents. The primary verifies that your lock ID matches what it has recorded. If it does, it applies your changes, increments the version number, clears the lock, and queues replication to the secondary.

If you realize you don't want to make changes after all, you can send `cancel-modify` instead. This just releases the lock without modifying anything.

The 500-millisecond timeout is important. If your code crashes or your network connection dies after acquiring the lock, you don't want the store locked forever. After 500 milliseconds, the lock automatically expires and other requests can proceed. This makes the system self-healing without requiring operator intervention to clear stuck locks.

### What Happens to Locks During Failover

This is subtle and important. When the secondary promotes itself to primary, it doesn't know the exact state of locks that were held on the old primary. The old primary might have granted a lock right before it died. That lock state might not have replicated yet.

To handle this uncertainty safely, Big Bunny declares a lock-state-unknown window. For 500 milliseconds after promotion (the same duration as the lock timeout), any lock-related operation returns a special `LockStateUnknown` error. This tells the client "I just became primary and I'm not sure about lock state yet, wait a bit and retry."

After 500 milliseconds, any lock that existed on the old primary has definitely expired. The new primary can now safely accept lock operations, knowing that there are no phantom locks still held by clients of the old primary.

## Store ID Encryption

When Big Bunny creates a store, it generates an encrypted identifier that clients use to reference that store in future operations. This encryption serves multiple purposes beyond just security.

The encryption algorithm is AES-128-SIV, which stands for Synthetic Initialization Vector. This is a special variant of AES that's designed for deterministic encryption. Big Bunny doesn't use a nonce at all (AES-SIV makes the nonce optional), which simplifies the implementation and makes the encryption deterministic: encrypting the same store ID plaintext with the same customer key always produces the same ciphertext.

Why does this matter for Big Bunny? Because each customer gets their own encryption key derived from the master key using HKDF (a key derivation function). The derivation formula incorporates the customer ID, so customer A and customer B get completely different keys even though they share the same master key. This provides cryptographic isolation—customer A literally cannot decrypt customer B's store IDs, even if they try.

The plaintext inside the encrypted store ID contains three pieces of information: the site identifier (which PoP this store lives in), a shard ID (random bytes used for routing), and a unique identifier (more random bytes to prevent collisions). When someone presents a store ID, Big Bunny decrypts it using a key derived from the customer ID in the request. If decryption fails, it's either an invalid store ID or a store ID for a different customer. Either way, the request is rejected.

This cryptographic approach is stronger than just checking customer IDs in application code. Even if there's a bug in the routing logic, the encryption prevents cross-customer access. The system fails closed rather than open.

## The Name Registry

Sometimes you want to refer to a store by a name like "user-session" instead of tracking an opaque encrypted identifier. The name registry makes this possible, but it requires a careful protocol to avoid race conditions.

The challenge is this: if two requests try to create a store with the same name simultaneously, you want exactly one to succeed and the other to fail with "name already exists." But with distributed systems and async replication, how do you ensure this happens correctly?

Big Bunny uses a two-phase reservation protocol. When you create a named store, the first phase reserves the name. The primary creates a registry entry in the "Creating" state with a unique reservation ID and replicates this to the secondary. At this point, the name is reserved but not yet associated with a store.

The second phase creates the actual store and then commits the name mapping. The commit operation updates the registry entry to "Active" state and records the store ID. This must provide the reservation ID from phase one. If the reservation ID doesn't match (maybe it expired and someone else reserved the name), the commit fails.

If anything goes wrong between the reservation and commit phases, the reservation has a five-second timeout. After five seconds, it automatically expires and the name becomes available again. This prevents names from getting stuck in "Creating" state forever if a client crashes between phases.

The registry itself is replicated just like stores, using the same async replication mechanism. Registry entries have version numbers and are deduplicated on the secondary to ensure idempotency.

## Tombstones and Deletion

When you delete a store, Big Bunny doesn't simply remove it from memory. Instead, it creates a tombstone—a marker that says "this store was deleted at this timestamp." Tombstones are crucial for preventing resurrection of deleted stores.

Here's the problem they solve: imagine you delete a store, but there's a delayed replication message still in flight that contains an earlier create or update for that store. Without tombstones, when that message arrives at the secondary, it would create the store, effectively resurrecting something you explicitly deleted.

With tombstones, when the delayed message arrives, the secondary checks its tombstone list. It sees "oh, this store was deleted," and ignores the message. The deletion wins, as it should.

Tombstones are kept for 24 hours, which is far longer than any reasonable replication delay. After 24 hours, they're garbage collected to free up memory. This gives a huge safety margin while preventing memory leaks from accumulating tombstones forever.

## Recovery Protocol

When a node comes back after failing or being restarted, it can't just start accepting writes immediately. It needs to catch up with what happened while it was gone. This is the recovery protocol, and it's designed to be simple while handling most edge cases correctly.

A recovering node starts in the "joining" state. In this state, it rejects client requests and doesn't participate in replication. It's essentially offline from the cluster's perspective. The first thing it does is find the primary by probing peer nodes' status endpoints.

Once it knows who the primary is, it requests a snapshot. This is a consistent point-in-time copy of all stores, tombstones, and registry entries, along with the current epoch number. The primary pauses its replication queue briefly to generate this snapshot, then sends it over.

The recovering node validates the snapshot's epoch number. If the snapshot's epoch is less than the node's current epoch, something weird has happened—maybe the wrong node got promoted. The recovery is aborted. This is epoch regression protection, and it prevents a node from rolling back to an older state.

Assuming the epoch checks out, the recovering node applies the snapshot. It throws away its old data and replaces it with the snapshot data. It updates its own epoch to match the snapshot. Then it transitions from "joining" to "secondary" state and starts accepting normal replication messages again.

If recovery fails for any reason, the node stays in "joining" state and tries again when the next heartbeat arrives. This retry logic handles transient issues like network timeouts without requiring operator intervention.

## Garbage Collection

Big Bunny runs several garbage collection loops to clean up expired data. These run on the primary node every 30 seconds.

The store expiry loop walks through all stores and checks if their TTL has expired. If a store's expiry timestamp is in the past and the store isn't currently locked, it gets deleted. The "isn't currently locked" check is important—you don't want to delete a store while someone is in the middle of modifying it. That would cause their `complete-modify` to fail with a mysterious "not found" error.

The registry cleanup loop looks for stale entries. If a registry entry points to a store that no longer exists, the entry is deleted. This can happen if a store expires naturally but the registry entry isn't updated. The cleanup happens lazily when you try to look up a name—if the store is gone, the name is unregistered as part of handling the lookup.

There's also cleanup for stuck reservations. If a registry entry has been in "Creating" state for longer than its timeout (five seconds), it gets deleted. This handles cases where a client reserved a name but crashed before committing.

Orphaned stores are another concern. These are stores that have a "pending_name" field (indicating they're part of an incomplete create-by-name operation) but no corresponding registry entry. If a store has been in this state for more than twice the reservation timeout, it's considered orphaned and deleted.

All of this cleanup is designed to be self-healing. The system should recover from most failure modes automatically without requiring an operator to manually clean up state.

## Consistency Model

Let's be precise about what consistency guarantees Big Bunny provides, because this matters when you're building applications on top of it.

For writes, Big Bunny provides linearizability on the primary, but only because there's a single primary and it serializes writes per store using locks. If you acquire a lock, modify a store, and release the lock, those operations happen in order and no other write can interleave.

For reads, Big Bunny provides eventual consistency. When you do a snapshot read (one that doesn't acquire a lock), you might see stale data. The replication is asynchronous, so the secondary might be slightly behind the primary. If you read from the secondary, you might see an older version. If you read from the primary, you'll see the most recent committed version, but "most recent" only considers completed operations, not ones currently in progress.

Within the modify protocol, reads are consistent. When you begin a modify, you acquire a lock, which means no one else can modify the store until you're done. The value you read during begin-modify is stable—it won't change under you.

There's no support for transactions across multiple stores. Each operation affects exactly one store. If you need to update two stores atomically, you can't do that with Big Bunny. You'd need to design your application to handle eventual consistency across stores or keep related data in a single store.

## Performance Characteristics

Understanding the performance characteristics helps you use Big Bunny effectively and debug issues when they arise.

Write operations on the primary complete in roughly 100 microseconds. This includes acquiring the lock (if needed), applying the change, incrementing the version, and queuing the replication message. It's fast because it's all in-memory operations with no disk I/O and no synchronous network calls.

Read operations are even faster, around 50 microseconds, because they don't acquire locks or do any writes. They just look up the store in memory and return its contents.

The replication lag is typically under 100 milliseconds under normal load. Messages sit in the replication queue for a short time and then get sent over the network. The queue provides natural batching—if multiple updates happen quickly, they can be sent in a single network round trip.

Failover takes about four seconds. This is the lease timeout (two seconds) plus the grace period (two seconds). During this time, the primary is unresponsive but the secondary hasn't yet promoted itself. After failover completes, requests can be routed to the new primary and things resume normally.

Lock throughput per store is limited by the 500-millisecond timeout. If each modify operation takes the full timeout to complete (which would be unusual), you can only do two operations per second per store. In practice, modify operations complete in a few milliseconds, so you can do hundreds per second per store. The important thing is that you can't have parallel modifications—they're fundamentally serialized.

Total cluster throughput scales with the number of unique stores. If you have a million stores and each one can handle 100 modifications per second, your cluster can theoretically handle 100 million modifications per second. In practice, you'll be limited by CPU, memory bandwidth, and network capacity long before you hit per-store limits.

## Design Trade-Offs

Every system makes trade-offs, and Big Bunny makes specific choices optimized for session storage. Understanding these trade-offs helps you know when Big Bunny is the right tool and when it isn't.

The biggest trade-off is choosing async replication over sync replication. Async replication means writes are fast (microseconds) but you can lose recent writes if both replicas fail simultaneously. Sync replication would guarantee no data loss but would make writes much slower (milliseconds, waiting for network round trips). For session storage, speed usually matters more than perfect durability.

The lease-based promotion trade-off is between failover speed and split-brain risk. Big Bunny chooses conservative timeouts (four seconds total) to minimize false positives. A more aggressive system might fail over in one second but would have more split-brain incidents during transient network issues. For session storage, a few extra seconds of unavailability is usually better than data corruption from split-brain.

The in-memory storage trade-off is between performance and durability. Everything lives in RAM, which makes it incredibly fast but means data doesn't survive host reboots or power failures. A disk-based system would survive those scenarios but would be much slower. Session data is ephemeral by nature, so this trade-off usually makes sense.

The lack of external coordination (no etcd, no ZooKeeper) makes Big Bunny simpler to deploy and operate but means it can't provide perfect split-brain prevention. True split-brain prevention requires external consensus, which requires external dependencies and added complexity. Big Bunny chooses operational simplicity over theoretical correctness.

## What Could Go Wrong

Understanding the failure modes helps you design resilient applications and debug problems in production.

If both replicas fail simultaneously, all data is lost. There's no recovery from this scenario. Applications need to handle stores suddenly not existing and recreate them as needed. This is why Big Bunny isn't suitable for data that must never be lost.

If there's a prolonged network partition between the primary and secondary, you can get a split-brain situation where both nodes think they're primary. When the partition heals, epoch fencing will resolve this by making the lower-epoch node step down. But during the partition, both might have accepted writes, and the lower-epoch node's writes will be lost when it steps down.

If clock skew between nodes is significant (more than a few seconds), lease timeouts can behave unexpectedly. A node with a fast clock might promote itself prematurely. A node with a slow clock might wait too long to promote. Big Bunny assumes clocks are roughly synchronized, typically via NTP.

If replication falls behind significantly, the replication queue can grow large and consume memory. Under extreme write load with a slow network, this could theoretically exhaust memory. The system doesn't have explicit backpressure, so it's possible to overwhelm it with writes.

If a lock is held during a crash, it will automatically expire after 500 milliseconds. But during that time, the store is unavailable for modification. Applications need to handle `StoreLocked` errors appropriately, typically by waiting and retrying.

## Future Directions

Big Bunny makes specific trade-offs that work well for session storage, but there are extensions that could be valuable for different use cases.

Synchronous replication mode could be added as an option for applications that need zero data loss. The client would specify in a header that they want sync replication, and the primary would wait for the secondary to acknowledge before returning success. This would increase write latency but eliminate the data loss window.

Cross-region replication would extend Big Bunny beyond a single PoP. Stores could be replicated asynchronously to other regions, with configurable home regions for named stores. This would provide better geographic distribution but at the cost of additional complexity and eventual consistency across regions.

Larger stores could be supported by adjusting the size limit. The current 2KB limit is somewhat arbitrary. Applications needing slightly larger session state (say, 10KB) could be accommodated without major architectural changes.

Formal verification would increase confidence in the protocols. Using a specification language like TLA+ to model the epoch fencing, lock state unknown window, and recovery protocol would help find edge cases that aren't obvious from code review or testing.

But for now, Big Bunny focuses on doing one thing well: fast, consistent, replicated session storage with automatic failover. The design is deliberately simple, making it easy to understand, operate, and debug. Sometimes simplicity is more valuable than additional features.
