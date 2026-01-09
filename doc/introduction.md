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

## The Problem

Modern edge platforms offer various storage solutions, but they each come with significant trade-offs that leave a gap in the middle. On one end, you have key-value stores that provide durability and global distribution, but they're limited to about one write per second per key. That's fine for configuration data that rarely changes, but it falls apart when you need to track hundreds of updates per second for things like API quotas or shopping cart modifications.

On the other end of the spectrum, caches are fast, but they offer no consistency guarantees. When two requests try to update the same data simultaneously, one update can silently overwrite the other. There's also no protection against data loss from cache eviction. For session data that needs to be modified frequently and reliably, caches simply aren't enough.

Traditional databases solve the consistency problem, but they're too slow for edge use cases. When you're running code at the edge to minimize latency for end users, the last thing you want is to add tens or hundreds of milliseconds reaching back to a centralized database for every session read or write.

This gap has real consequences. One customer needed accurate rate limiting counters at the edge and ended up using a competitor's platform specifically for this feature. Other customers want to track user behavior in real-time or maintain shopping cart state with high update frequencies. They need something faster than a database, more consistent than a cache, and more capable than existing key-value stores.

### What customers actually need

Consider a content delivery platform enforcing API quotas at the edge. They sell plans that include a certain number of API requests per month, and they need to track usage accurately as requests come in. Every non-cached request needs to increment a counter, and that counter needs to be consistent. If two edge workers both try to increment the counter at the same time, neither update can be lost.

A cache won't work because cache evictions would lose count data. A traditional key-value store works but hits the write rate limit almost immediately. What they need is something in between: fast enough for edge deployment, consistent enough to prevent lost updates, but without the durability requirements of a database.

Another common pattern we see is shopping cart state. Users add items to their cart throughout a session, requiring frequent updates. Each cart modification needs to be consistent (you can't lose cart items due to a race condition), but the data doesn't need to survive a datacenter outage. If a user's session data disappears after a few hours, they'll just start over. The important thing is that while the session exists, the data is fast, consistent, and survives individual host failures.

Big Bunny was built specifically for these use cases.

## The Problem

Modern edge computing platforms face a frustrating gap in their storage offerings. If you look at what's available today, you'll find key-value stores that are durable and globally distributed but limited to about one write per second per key. You'll find caches that are fast but inconsistent, with no real guarantees against losing your data or serving stale reads. And you'll find databases that are persistent and reliable, but far too slow for edge use cases that need hundreds of writes per second with millisecond latency.

This gap leaves several important use cases struggling to find a home. Consider rate limiting at the edge. You need a counter that updates hundreds of times per second with perfect accuracy. A key-value store is too slow. A cache might lose your count. A database is too far away.

Or think about session state for a shopping cart. You need fast reads and writes, consistency so you don't lose items, and you need it to live for hours or days, not just seconds. A cache might evict your data, a database adds too much latency, and existing solutions force you to choose between speed and correctness.

This is the gap that Big Bunny fills. It gives you the speed of a cache with the consistency of a database, while accepting that your data doesn't need to live forever. Session data is ephemeral by nature, and Big Bunny embraces this reality rather than fighting it.

## The Solution

Big Bunny provides fast, consistent, replicated storage that sits right where you need it: at the edge, in memory, with automatic failover if a host goes down. Think of it as a specialized tool for a specific job. You wouldn't use a hammer to drive a screw, and you shouldn't use a global database for temporary session counters.

The core insight is that session storage has different requirements than persistent storage. Sessions are temporary, updates are frequent, and consistency matters more than durability. Big Bunny is built around these truths. It keeps your data in RAM for microsecond-level access times, replicates it to a second node for failover, and gives you a simple lock-based API that prevents race conditions without requiring you to understand CRDTs or operational transforms.

When you create a store in Big Bunny, you get back an encrypted identifier. This isn't just security theater. The encryption means that clients can't see where their data lives or how it's routed, and more importantly, it prevents customers from accessing each other's stores. Every store belongs to exactly one customer, enforced cryptographically, not just by application logic.

The replication happens asynchronously, which means your writes return immediately without waiting for network round trips. This is a deliberate trade-off: you get low latency at the cost of potentially losing recent writes if both replicas fail simultaneously. For session storage, this is usually the right choice. Nobody wants their shopping cart update to take 50 milliseconds when it could take 100 microseconds.

Failover is automatic. If the primary node goes down, the secondary detects this within about four seconds and promotes itself. Your application just retries the request and everything works. No manual intervention, no operator on call at 3 AM. The system is designed to handle the most common failure mode (single host failure) gracefully and automatically.

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
