# Security

Security in Big Bunny operates on multiple layers, from cryptographic isolation between customers to network-level protections. This document explains the security model, analyzes potential threats, and provides guidance for securing your deployment.

## Understanding the Security Model

Big Bunny divides the world into three trust zones, each with different security assumptions and protections.

Clients are untrusted. These are the application servers accessing Big Bunny through the Unix socket. They can't be trusted with routing information or cross-customer access, so the system enforces isolation cryptographically rather than relying on application-level checks.

Cluster nodes are trusted within a PoP. Big Bunny daemons in the same cluster share encryption keys and internal tokens. They trust each other for replication and failover. This trust is reasonable because they're typically on the same private network, managed by the same operations team.

Operators are privileged. System administrators with host access can do anything—they can read memory, access the Unix socket, and manipulate configuration. The security model assumes operators are trustworthy but provides tools to limit the blast radius of mistakes.

The security goals flow from this trust model. Customer isolation is paramount—customers must never access each other's data, even if there's a bug in the routing logic. Tamper protection ensures store IDs can't be forged or modified. Confidentiality keeps routing information hidden from clients. Integrity checking detects corruption. And availability means the system resists denial-of-service attacks within reasonable resource limits.

## How Store ID Encryption Works

When Big Bunny creates a store, it generates an encrypted identifier that clients use to reference that store. The encryption isn't just about security—it also enables customer isolation and prevents tampering.

The algorithm is AES-128-SIV, which stands for Synthetic Initialization Vector. This variant of AES is designed for deterministic encryption—it doesn't require a nonce at all. Big Bunny doesn't use a nonce, which simplifies the implementation and makes the encryption deterministic: encrypting the same store ID plaintext with the same customer key always produces the same ciphertext. This deterministic property is exactly what Big Bunny needs because each customer gets their own encryption key derived from the master key, and the derivation needs to be deterministic and repeatable.

Here's how it works. When you start Big Bunny, you provide a master encryption key. This is a 32-byte random value that all nodes in your cluster share. For each customer, Big Bunny derives a unique encryption key using HKDF (a key derivation function) with the customer ID as input. So customer A and customer B get completely different encryption keys, even though they share the same master key.

When encrypting a store ID, Big Bunny takes the plaintext (which contains the site identifier, a shard ID for routing, and a unique identifier), derives the customer-specific key, and encrypts using AES-SIV. The result is a ciphertext that only that customer can decrypt. If customer A tries to decrypt customer B's store ID, the decryption fails because they're using different derived keys.

The encrypted store ID has this format: `v1:0:8ahePLwi-iJB-h_8AbZYvK4jK9...`. The `v1` indicates the version. The `0` is the key ID (you can have multiple keys for rotation). The remaining part is base64url-encoded ciphertext from AES-SIV.

Inside the plaintext, before encryption, there are three pieces of information. The site identifies which PoP this store lives in. The shard ID is random bytes used for deterministic replica placement. The unique identifier is more random bytes to prevent collisions. When combined and encrypted, these create an opaque token that clients can't tamper with or forge.

There's also Associated Authenticated Data (AAD) included in the encryption. The AAD is just the string `"storeid:v1:"` concatenated with the customer ID. This binds the ciphertext to the customer ID cryptographically. Even if someone could somehow use the wrong key (they can't), the AAD mismatch would cause decryption to fail. It's defense in depth—the primary protection is the per-customer key derivation, but the AAD provides an additional layer.

## Key Management

The master encryption keys are the crown jewels of your Big Bunny deployment. If someone gets these keys, they can decrypt all store IDs and potentially access data they shouldn't. Treat them accordingly.

Generate keys using a cryptographically secure random number generator. On most systems, `openssl rand -hex 32` works perfectly. This gives you 32 bytes of random data (required for AES-128-SIV), represented as 64 hexadecimal characters. That's your master key.

Store keys securely. Don't put them in Git repositories. Don't leave them in shell history. Don't log them. Use a secrets management system like HashiCorp Vault or AWS Secrets Manager if you have one. If not, at minimum, store them in files with restrictive permissions (mode 0600, readable only by the Big Bunny user).

Each key has an identifier, which can be any string from 1 to 16 characters, starting with an alphanumeric character. Simple numbers like `0` and `1` work fine. Some people prefer meaningful identifiers like `2024q1` for quarterly rotation schedules. The identifier becomes part of the encrypted store ID format, so keep it short.

Big Bunny supports multiple keys simultaneously, which enables rotation without downtime. When you rotate keys, you add the new key to the configuration and mark it as current. New stores get encrypted with the new key, but stores encrypted with old keys remain readable. After all the old stores expire (based on their TTL), you can remove the old key from the configuration.

Key rotation should happen regularly. A reasonable schedule is every 90 days, though your security requirements might dictate something different. If a key gets compromised, you need to rotate immediately, which means some clients might suddenly find their store IDs become invalid. This is unfortunate but necessary.

All nodes in a cluster must share the same keys. If they don't, they won't be able to decrypt each other's store IDs, and replication will break. Make sure your key distribution mechanism guarantees consistency across nodes.

## Internal Authentication

Big Bunny nodes communicate with each other over TCP for replication and heartbeats. These internal endpoints need authentication to prevent unauthorized nodes from injecting fake replication traffic or eavesdropping.

The authentication mechanism is simple: a shared secret token that all nodes in the cluster know. This token is configured via the `--internal-token` flag or the `SERIALD_INTERNAL_TOKEN` environment variable. When a node sends a replication request, it includes this token in the `X-Internal-Token` header. The receiving node checks that the token matches before processing the request.

Generate the token the same way you generate encryption keys: `openssl rand -hex 16` gives you a 16-byte (128-bit) random value, which is plenty for a shared secret.

In the current implementation, this token is sent in plaintext over the network. If you're running on a trusted private network, this is acceptable. If you're running on an untrusted network, you'd want to add TLS encryption. Big Bunny doesn't currently support TLS for internal endpoints, but the architecture is designed to accommodate it. Future versions could add `--internal-tls-cert`, `--internal-tls-key`, and `--internal-tls-ca` flags for mTLS.

Development mode bypasses the internal token requirement and logs a warning. This makes testing easier but obviously isn't suitable for production. The warning is loud and annoying on purpose—it should be impossible to accidentally run in development mode and not notice.

## Unix Socket Security

The Unix socket is how clients access Big Bunny locally. By default, it's created with mode 0600, meaning only the owner can read and write. This is restrictive by design. If you need multiple users to access it, you have options.

You can change the permissions after Big Bunny starts. Run `chmod 660 /var/run/bbd/bbd.sock` to make it group-readable and group-writable, then `chown :appgroup /var/run/bbd/bbd.sock` to set the group. Now anyone in `appgroup` can access the socket.

Alternatively, run your client processes as the same user that runs Big Bunny. This is often simpler and avoids permission complications.

Be thoughtful about socket permissions. Anyone who can access the socket can operate on any customer's stores. The customer isolation is enforced by the API (through encryption), but it assumes the socket itself is properly secured. Don't make the socket world-readable unless you really trust everyone on the system.

For production deployments, run Big Bunny as a dedicated user, not as root. Create a `bbd` user and group, set up the socket directory with appropriate ownership, and run the daemon as that user. If you're using systemd, the `User=bbd` directive in your unit file handles this automatically.

## Analyzing Potential Threats

Let's walk through various attack scenarios and see how Big Bunny handles them.

**Forging store IDs**: An attacker tries to craft a fake store ID to access arbitrary data. This doesn't work because store IDs are encrypted with AES-SIV, which includes an authentication tag. Without knowing the encryption key, you can't create a valid store ID. Even if you could somehow create valid ciphertext (you can't), the decryption would fail because you don't know the customer-specific derived key.

**Tampering with store IDs**: An attacker modifies an existing store ID to change the routing or access different data. AES-SIV's authentication tag detects any modification. Even changing a single bit causes decryption to fail. The AAD (which includes the customer ID) provides additional protection—if you could somehow swap ciphertexts between customers (you can't), the AAD mismatch would cause failure.

**Cross-customer access**: Customer A tries to use a store ID belonging to customer B. This fails at two levels. First, the encryption keys are different (derived from the same master key but with different customer IDs as input), so customer A literally cannot decrypt customer B's store ID. Second, even if the keys were somehow the same, the AAD includes the customer ID, so the decryption would fail on AAD mismatch. This double protection is intentional—even if there's a bug in the key derivation, the AAD provides a safety net.

**Replay attacks within a customer**: Customer A captures a valid request and replays it. This is allowed by design. Clients legitimately replay requests when retrying on timeout. Store IDs are reusable within their TTL, and idempotent operations (like snapshot reads and deletes) are safe to replay. Non-idempotent operations (like creates and modifies) might have side effects, but the lock-based serialization prevents concurrent replay from causing inconsistency.

**Cross-customer replay attacks**: Customer A captures customer B's request and tries to replay it as customer A. This fails because when customer A presents customer B's store ID with customer A's customer ID header, the decryption uses customer A's derived key, which doesn't match the encryption. The request gets rejected as an invalid store ID.

**Injecting fake replication traffic**: An attacker tries to send bogus replication messages to a secondary. In the current implementation, this requires knowing the internal authentication token. Without the token, the request gets rejected with 401 Unauthorized. If the attacker has the token (maybe from network sniffing on an unencrypted network), they can inject traffic. This is why running on trusted private networks is important, and why future versions should support mTLS for internal endpoints.

**Man-in-the-middle on replication**: An attacker intercepts replication traffic between nodes. With the current plaintext implementation, they can read the traffic and steal the internal token. With that token, they could inject fake traffic. The mitigation is to run on trusted networks. The proper fix is mTLS, which would encrypt the traffic and verify peer certificates.

**Denial of service**: An attacker floods with requests to exhaust resources. Big Bunny has some protections. Memory limits prevent unbounded memory consumption. Lock timeouts prevent indefinite locking. Per-store serialization limits the blast radius. But fundamentally, if someone sends millions of requests per second, they can overwhelm any system. The real defense is rate limiting at the load balancer or network layer, not in Big Bunny itself.

**Information leakage**: An attacker tries to infer information from store IDs or error messages. Store ID length reveals the site name length, which is minor. Deterministic encryption means identical plaintexts produce identical ciphertexts, but the unique identifier in each store ID prevents this from being useful. Error messages are generic—both invalid store IDs and wrong-customer IDs return "400 Bad Request" without details. Timing attacks are theoretically possible, but AES-SIV is implemented in constant time in the crypto libraries Big Bunny uses.

Here's a summary of the risk levels:

| Attack                 | Risk     | Protected?                      |
| ---------------------- | -------- | ------------------------------- |
| Store ID forgery       | None     | Yes (AES-SIV authentication)    |
| Store ID tampering     | None     | Yes (SIV tag + AAD)             |
| Cross-customer access  | None     | Yes (per-customer keys + AAD)   |
| Within-customer replay | Low      | Partial (by design for retries) |
| Cross-customer replay  | None     | Yes (key derivation + AAD)      |
| Replication injection  | Moderate | Partial (token, but plaintext)  |
| Man-in-the-middle      | Moderate | No (plaintext currently)        |
| Denial of service      | Moderate | Partial (resource limits)       |
| Information leakage    | Low      | Yes (minimal leakage)           |

## Best Practices for Deployment

When deploying Big Bunny in production, follow these practices to maintain security.

Generate strong keys using cryptographically secure random sources. The command `openssl rand -hex 32` works on most systems. Don't use weak random sources or predictable values. Never reuse keys across environments—development, staging, and production should each have their own keys.

Store keys securely. Use a secrets management system if available. If not, store them in files with mode 0600 and ownership restricted to the Big Bunny user. Never commit keys to version control. Never log keys in application logs. Never pass keys via command-line arguments where they'd be visible in process listings.

Rotate keys regularly. Every 90 days is reasonable for most deployments. Add the new key, mark it current, deploy to all nodes, wait for old stores to expire, then remove the old key. If a key is compromised, rotate immediately even if it means invalidating active store IDs.

Run on private networks. Big Bunny nodes should communicate over a private network that's not accessible from the internet. Use firewalls to restrict who can reach the TCP replication ports. Only peer nodes should be able to connect.

Use restrictive Unix socket permissions. The default 0600 (owner-only) is safest. If you need group access, use 0660 with a dedicated group. Never use 0666 (world-readable) unless you fully trust everyone on the system.

Run as a non-root user. Create a dedicated `bbd` user and group. Set up the socket directory with appropriate ownership. Use systemd's `User=` directive or run manually as that user. Never run Big Bunny as root.

Monitor for security issues. Watch for failed authentication attempts on internal endpoints. Track decryption failures which might indicate attack attempts. Alert on unusual patterns like sudden spikes in 400 errors or 401 unauthorized responses.

Keep clocks synchronized. Clock skew can cause security issues—a node with a fast clock might accept stale replication messages it should reject. Use NTP to keep clocks within a second of each other across all nodes.

## Incident Response

If you discover a security issue, you need to respond quickly and systematically.

For compromised encryption keys, rotate immediately. Generate a new key, add it to the configuration with a new key ID, mark it current, and deploy to all nodes. Accept that existing store IDs encrypted with the compromised key will become invalid. This is unfortunate but necessary—you can't un-compromise a key.

Assess the scope of the compromise. Can attackers decrypt existing store IDs? Have they already exfiltrated data? Check your logs for suspicious access patterns. Look for unusual API calls, replication failures, or error spikes that might indicate exploitation.

For compromised internal tokens, rotate them the same way you'd rotate keys. Generate a new token, update configuration, deploy to all nodes. Monitor for replication failures from unauthorized sources trying to use the old token.

If you detect unauthorized access to customer data, you need to notify affected customers according to your policies and legal requirements. Determine which customers were affected, what data was accessed, and provide them with actionable information about the breach.

For denial-of-service attacks, the immediate response is to identify and block the attack source. Check your load balancer logs for the source IPs. Update firewall rules or rate limits to block or throttle the attacker. If the attack is distributed, work with your network team or DDoS mitigation service.

After resolving the immediate issue, conduct a post-mortem. What went wrong? How was the breach detected? How quickly was it contained? What can be improved? Update your runbooks, monitoring, and alerting based on lessons learned.

## Compliance and Data Handling

Big Bunny's design has implications for regulatory compliance that you should understand.

Data residency is enforced by the architecture. Store IDs encode the site (PoP) where data lives, and data never leaves that PoP. This supports geo-fencing requirements—if you need European data to stay in Europe, deploy a Big Bunny cluster in your European PoP and it will naturally stay there.

Encryption at rest is not provided. Store bodies are held in plaintext in RAM. If someone gets a memory dump, they can read the data. This makes Big Bunny unsuitable for highly sensitive data like payment card information, health records, or social security numbers. For that kind of data, use client-side encryption—encrypt data before storing it in Big Bunny, decrypt after retrieving it.

Encryption in transit varies by path. Client-to-Big Bunny communication happens over a Unix socket, which is local only (no network), so encryption isn't needed. Big Bunny-to-Big Bunny replication happens over TCP in plaintext currently. For untrusted networks, you'd need to add TLS/mTLS support.

Audit logging covers authentication and administrative actions. Big Bunny logs authentication attempts (success and failure), administrative operations (promote, release-lock), and replication events. It does not log store contents (too large and privacy concern) or full store IDs (contains encrypted routing info). Logs go to stdout/stderr by default, which you'd typically capture with your logging infrastructure.

Data deletion is immediate for active stores. When you delete a store, it's removed from the active set right away. A tombstone is created to prevent resurrection during replication, but that tombstone doesn't contain the store body. After 24 hours, even the tombstone is garbage collected. Stores that expire naturally are deleted by the GC loop and their memory is freed.

There's no mechanism for secure deletion (zeroing memory). Big Bunny relies on the Go garbage collector, which doesn't zero memory pages before freeing them. If secure deletion is critical for your use case, you'd need to implement it at a different layer (like using encrypted memory or secure OS features).

## Future Security Improvements

Several security enhancements would make sense for production deployments beyond the PoC.

mTLS for internal endpoints would encrypt replication traffic and verify peer identities. Nodes would exchange certificates during the TLS handshake and verify them against a trusted CA. This prevents eavesdropping and man-in-the-middle attacks on replication traffic.

Per-customer resource quotas would limit the blast radius of abuse. You could restrict each customer to a maximum number of stores, maximum memory usage, or maximum operation rate. This prevents one customer from consuming all resources and impacting others.

Enhanced audit logging would provide better visibility into what's happening. Structured logging with customer IDs, operation types, store IDs (hashed for privacy), timestamps, and outcomes would feed into SIEM systems for security analysis.

Rate limiting per customer would prevent abuse and denial of service. Track creates, modifies, and deletes per customer per time window, and reject requests exceeding the limit. This protects the system from both malicious and buggy clients.

Certificate pinning for mTLS would prevent rogue certificate authorities from issuing valid certificates for your nodes. Pin the expected certificate fingerprints in configuration and reject connections with unexpected certificates, even if they're signed by a trusted CA.

## Getting Help with Security

For general security questions about Big Bunny, consult this documentation first, then the [Architecture](architecture.md) document for technical details. For operational security, see the [Operations Guide](operations.md).

For security vulnerabilities, do not file public GitHub issues. Instead, contact the security team at security@bigbunny.example (this is placeholder—update with real contact). Provide details about the vulnerability, steps to reproduce, and potential impact. The team will work with you on coordinated disclosure.

Big Bunny is a proof of concept. The security model is sound for session storage on trusted networks, but production hardening would require additional features like mTLS, enhanced audit logging, and per-customer quotas. Evaluate whether the current security posture meets your requirements before deploying in production.
