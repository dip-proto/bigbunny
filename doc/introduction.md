# Introduction to Big Bunny

## What is Big Bunny?

Big Bunny (`bbd`) is a fast, in-memory session store built for edge computing environments. Think of it as living somewhere between a cache and a database—faster than a database, more reliable than a cache, and designed specifically for the kinds of small, frequently-updated data that edge applications need.

The service keeps your data in RAM for microsecond-level access times, but unlike a simple cache, it includes built-in replication and automatic failover. If one host dies, your data stays available. It handles stores up to 2KB in size, which turns out to be perfect for session state, counters, shopping carts, and similar use cases where you need something faster than a database but more reliable than a cache.

What makes Big Bunny different is its approach to concurrent access. Instead of requiring you to reason about merge conflicts and CRDTs, it provides a simple lock-based modify protocol. When you need to update a store, you acquire a lock, make your changes, and release it. This serialized access model means you never have to worry about race conditions or lost updates.

## The Problem

Modern edge computing platforms have a gap in their storage stack. You can choose between durable key-value stores that are too slow for high-frequency updates, or fast caches that can't guarantee consistency. Neither option works well for the kinds of data that edge applications actually need to store.

Consider what happens when you're building an edge application today. If you need to track API quotas, you want a counter that updates hundreds of times per second with perfect accuracy. Key-value stores are too slow, maxing out at one write per second per key. Caches are fast, but they can't guarantee you won't lose updates when two requests modify the counter simultaneously. You end up either accepting inaccurate counts or falling back to a centralized database, which defeats the purpose of running at the edge.

The same problem shows up everywhere in edge computing. Shopping carts need fast updates without race conditions. Session state needs to survive across requests but doesn't need to last forever. Marketing systems tracking user behavior need consistent counters that can handle hundreds of updates per second. None of the existing storage options quite fit.

Traditional key-value stores like the ones found in most edge platforms offer great durability and global distribution, but they're deliberately throttled to around one write per second per key. That works fine for configuration data, but it's far too slow for a counter that needs to increment on every request or a shopping cart that updates with every click.

Caches are fast enough, but they offer no consistency guarantees. Two requests trying to update the same counter at the same time can step on each other's toes, and there's nothing stopping the cache from evicting your data at the worst possible moment. You might read a value, modify it, and write it back, only to discover that someone else's update just got silently lost.

Databases solve the consistency problem, but they're typically too far away. If your edge service is in São Paulo and your database is in Virginia, you're looking at 100+ milliseconds of latency per request. That defeats the entire point of edge computing.

This gap has real consequences. Customers who need accurate counters for rate limiting, consistent session state for shopping carts, or reliable behavioral tracking end up using competitor platforms. That's not hypothetical, it's already happening.

## The Solution

Big Bunny bridges this gap by bringing fast, consistent storage directly to the edge. Think of it as sitting in the sweet spot between a cache and a database. It's faster than a database because everything lives in RAM, but unlike a cache, it provides consistency guarantees and survives single-host failures.

The core design revolves around three principles. First, data lives entirely in memory, giving you microsecond-level access times without any disk I/O bottlenecks. Second, every store has a lock-based modify protocol that guarantees serialized access, which means you never have to worry about concurrent updates causing race conditions or lost writes. Third, automatic replication to a secondary node ensures your data survives a host failure, with failover happening automatically in about four seconds.

What makes Big Bunny different from traditional caches is the consistency guarantee. When you acquire a lock on a store, you know that no other request can modify it until you're done. This eliminates an entire class of concurrency bugs that plague cache-based solutions. You don't need complex CRDTs or conflict resolution logic. You just read the current value, modify it, and write it back, confident that nobody else can interfere.

At the same time, Big Bunny isn't trying to be a database. Store IDs are encrypted and opaque, making them unsuitable as long-term references. Data lives in RAM, which means a simultaneous failure of both replicas means data loss. TTLs are enforced strictly, with stores expiring after their configured lifetime. This is ephemeral storage designed for speed and convenience, not permanence.

The sweet spot is session data that matters for minutes to days, where you need consistency guarantees stronger than a cache but don't want the latency of a database roundtrip.

## What Makes This Different

Big Bunny doesn't try to be everything to everyone. It's not a database replacement. It's not a cache. It's not a message queue. It's a session store, and it's really good at being a session store.

The design makes strong choices that work well for this specific use case. Stores are small (up to 2KB) because sessions don't need to be large. Data lives in memory because sessions need to be fast. Replication is asynchronous because sessions need low latency more than perfect durability. The API provides serialized access because sessions need consistency more than massive parallel throughput.

These constraints aren't limitations to work around. They're features that make the system simpler, faster, and more reliable for its intended purpose. By focusing on doing one thing well, Big Bunny can make design choices that would be inappropriate for a general-purpose data store but are exactly right for session storage.

## Who Should Use This

Big Bunny makes sense if you're building edge applications that need to maintain state between requests. You might be implementing rate limiting and need accurate counters. You might be tracking user sessions and need to update them frequently. You might be accumulating behavioral data for analytics and need to ensure no updates are lost to race conditions.

If you're currently using a competitor's solution for this purpose, Big Bunny can replace it. If you're currently trying to make a key-value store work despite its write rate limits, Big Bunny can replace it. If you're currently using a cache and dealing with race conditions, Big Bunny can replace it.

On the other hand, if you need to store large objects, keep data forever, replicate across multiple regions, or have zero data loss under all failure scenarios, Big Bunny isn't the right tool. It's designed for a specific use case, and using it for something else would be like using a sports car to haul gravel.

## Getting Started

The [Installation Guide](installation.md) will walk you through building and deploying Big Bunny. If you just want to try it out quickly, the [Quick Start](quickstart.md) guide gets you running in five minutes. Once you're comfortable with the basics, the [Architecture](architecture.md) document explains how everything works under the hood, and the [Operations Guide](operations.md) covers running it in production.

Big Bunny is designed to be approachable. You don't need to understand Raft consensus or vector clocks to use it effectively. The API is straightforward HTTP over a Unix socket, and the semantics are simple: create stores, read them, modify them with locks, delete them. Everything else is implementation details that the system handles for you.
