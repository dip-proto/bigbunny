package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dip-proto/bigbunny/internal/api"
	"github.com/dip-proto/bigbunny/internal/auth"
	"github.com/dip-proto/bigbunny/internal/ratelimit"
	"github.com/dip-proto/bigbunny/internal/registry"
	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/internal/routing"
	"github.com/dip-proto/bigbunny/internal/store"
)

func main() {
	// Print help if no arguments provided
	if len(os.Args) == 1 {
		printUsage()
		return
	}

	// Check for CLI subcommands before parsing daemon flags
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "status":
			runStatusCommand(os.Args[2:])
			return
		case "promote":
			runPromoteCommand(os.Args[2:])
			return
		case "release-lock":
			runReleaseLockCommand(os.Args[2:])
			return
		case "create":
			runCreateCommand(os.Args[2:])
			return
		case "get":
			runGetCommand(os.Args[2:])
			return
		case "delete":
			runDeleteCommand(os.Args[2:])
			return
		case "delete-named":
			runDeleteNamedCommand(os.Args[2:])
			return
		case "lookup":
			runLookupCommand(os.Args[2:])
			return
		case "begin-modify":
			runBeginModifyCommand(os.Args[2:])
			return
		case "complete-modify":
			runCompleteModifyCommand(os.Args[2:])
			return
		case "cancel-modify":
			runCancelModifyCommand(os.Args[2:])
			return
		case "counter-create":
			runCounterCreateCommand(os.Args[2:])
			return
		case "counter-get":
			runCounterGetCommand(os.Args[2:])
			return
		case "counter-increment":
			runCounterIncrementCommand(os.Args[2:])
			return
		case "counter-decrement":
			runCounterDecrementCommand(os.Args[2:])
			return
		case "counter-set":
			runCounterSetCommand(os.Args[2:])
			return
		case "help", "-h", "--help":
			printUsage()
			return
		}
	}

	// Daemon mode
	var (
		hostID                    = flag.String("host-id", "host1", "unique host identifier")
		site                      = flag.String("site", "local", "site identifier")
		udsPath                   = flag.String("uds", "/tmp/bbd.sock", "unix domain socket path")
		tcpAddr                   = flag.String("tcp", ":8080", "TCP address for inter-host communication")
		peers                     = flag.String("peers", "", "comma-separated peer list (id@host:port,...)")
		memoryLimit               = flag.Int64("memory-limit", 0, "max memory for stores in bytes (0 = no limit)")
		customerMemoryQuota       = flag.Int64("customer-memory-quota", 0, "max memory per customer in bytes (0 = no limit)")
		storeKeys                 = flag.String("store-keys", "", "encryption keys (id:hexkey,... or 'dev')")
		storeKeyCurrent           = flag.String("store-key-current", "", "current encryption key ID")
		routingSecret             = flag.String("routing-secret", "", "routing secret (hex, or 'dev')")
		internalToken             = flag.String("internal-token", "", "shared secret for internal endpoints")
		devMode                   = flag.Bool("dev", false, "enable dev mode (no auth required)")
		rateLimit                 = flag.Int("rate-limit", 100, "max requests per second per customer (0 = no limit)")
		burstSize                 = flag.Int("burst-size", 200, "burst capacity per customer")
		tombstonePerCustomerLimit = flag.Int("tombstone-customer-limit", 0, "max tombstones per customer (0 = no limit)")
		tombstoneGlobalLimit      = flag.Int("tombstone-global-limit", 0, "max tombstones globally (0 = no limit)")
		httpReadTimeout           = flag.Duration("http-read-timeout", 30*time.Second, "HTTP read timeout")
		httpReadHeaderTimeout     = flag.Duration("http-read-header-timeout", 10*time.Second, "HTTP read header timeout")
		httpWriteTimeout          = flag.Duration("http-write-timeout", 30*time.Second, "HTTP write timeout")
		httpIdleTimeout           = flag.Duration("http-idle-timeout", 120*time.Second, "HTTP idle timeout")
		httpMaxHeaderBytes        = flag.Int("http-max-header-bytes", 1<<20, "HTTP max header bytes (1MB default)")
		disableSiteVerification   = flag.Bool("disable-site-verification", false, "disable site verification (allows cross-site replication)")
	)
	flag.Parse()

	// Check environment variables for keys and token
	if *storeKeys == "" {
		if envKeys := os.Getenv("SERIALD_STORE_KEYS"); envKeys != "" {
			*storeKeys = envKeys
		}
	}
	if *storeKeyCurrent == "" {
		if envCurrent := os.Getenv("SERIALD_STORE_KEY_CURRENT"); envCurrent != "" {
			*storeKeyCurrent = envCurrent
		}
	}
	if *routingSecret == "" {
		if envSecret := os.Getenv("SERIALD_ROUTING_SECRET"); envSecret != "" {
			*routingSecret = envSecret
		}
	}
	if *internalToken == "" {
		if envToken := os.Getenv("SERIALD_INTERNAL_TOKEN"); envToken != "" {
			*internalToken = envToken
		}
	}

	// Dev mode defaults to "dev" keys if none specified
	if *devMode && *storeKeys == "" {
		*storeKeys = "dev"
	}
	if *devMode && *routingSecret == "" {
		*routingSecret = "dev"
	}

	// Treat --store-keys=dev as equivalent to --dev for consistency
	isDevMode := *devMode || *storeKeys == "dev"

	// Production guardrails
	if !isDevMode {
		if *storeKeys == "" {
			log.Fatal("production mode requires --store-keys or SERIALD_STORE_KEYS (use --dev for dev mode)")
		}
		if *routingSecret == "" {
			log.Fatal("production mode requires --routing-secret or SERIALD_ROUTING_SECRET (use --dev for dev mode)")
		}
		if *internalToken == "" {
			log.Fatal("production mode requires --internal-token or SERIALD_INTERNAL_TOKEN (use --dev for dev mode)")
		}
	}

	// Configure encryption (always required)
	if *storeKeys == "" {
		log.Fatal("encryption keys required: use --store-keys or --dev")
	}
	keySet, err := auth.ParseKeyConfig(*storeKeys, *storeKeyCurrent)
	if err != nil {
		log.Fatalf("failed to parse store keys: %v", err)
	}
	var cipherOpts []auth.CipherOption
	if *disableSiteVerification {
		cipherOpts = append(cipherOpts, auth.WithDisableSiteVerification())
		log.Printf("WARNING: site verification disabled (cross-site replication allowed)")
	}
	cipher := auth.NewCipher(keySet, cipherOpts...)
	if *storeKeys == "dev" {
		log.Printf("WARNING: using dev mode encryption keys (not for production)")
	} else {
		log.Printf("encryption enabled with key ID %q", keySet.CurrentKeyID())
	}

	// Configure routing secret (always required)
	if *routingSecret == "" {
		log.Fatal("routing secret required: use --routing-secret or --dev")
	}
	if *routingSecret == "dev" {
		log.Printf("WARNING: using dev mode routing secret (not for production)")
	} else {
		log.Printf("routing secret configured")
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("starting bbd: host=%s site=%s", *hostID, *site)
	log.Printf("HTTP timeouts: read=%v readHeader=%v write=%v idle=%v maxHeader=%d",
		*httpReadTimeout, *httpReadHeaderTimeout, *httpWriteTimeout, *httpIdleTimeout, *httpMaxHeaderBytes)

	// Initialize components
	var storeMgr *store.Manager
	if *memoryLimit > 0 {
		storeMgr = store.NewManagerWithLimit(*memoryLimit)
		log.Printf("memory limit: %d bytes", *memoryLimit)
	} else {
		storeMgr = store.NewManager()
	}

	// Set per-customer memory quota if configured
	if *customerMemoryQuota > 0 {
		storeMgr.SetCustomerMemoryQuota(*customerMemoryQuota)
		log.Printf("customer memory quota: %d bytes", *customerMemoryQuota)
	}

	// Build initial host list
	hosts := buildHostList(*hostID, *tcpAddr, *peers)
	hasher := routing.NewRendezvousHasher(hosts, *routingSecret)

	replicaCfg := replica.DefaultConfig(*hostID, *site)
	replicaCfg.TCPAddress = *tcpAddr
	replicaCfg.InternalToken = *internalToken
	replicaCfg.TombstonePerCustomerLimit = *tombstonePerCustomerLimit
	replicaCfg.TombstoneGlobalLimit = *tombstoneGlobalLimit
	replicaCfg.DisableSiteVerification = *disableSiteVerification
	replicaMgr := replica.NewManager(replicaCfg, storeMgr, hasher)

	// Log tombstone limits if configured
	if *tombstonePerCustomerLimit > 0 || *tombstoneGlobalLimit > 0 {
		log.Printf("tombstone limits: per-customer=%d global=%d", *tombstonePerCustomerLimit, *tombstoneGlobalLimit)
	}

	registryMgr := registry.NewManager()
	replicaMgr.SetRegistry(registryMgr)

	// Create rate limiter if configured (nil disables rate limiting)
	var rateLimiter *ratelimit.Limiter
	if *rateLimit > 0 {
		rateLimiter = ratelimit.NewLimiter(*rateLimit, *burstSize)
		log.Printf("rate limiting enabled: %d req/s per customer (burst: %d)", *rateLimit, *burstSize)
	} else {
		log.Printf("rate limiting disabled")
	}

	apiCfg := &api.Config{
		Site:          *site,
		HostID:        *hostID,
		DefaultTTL:    14 * 24 * time.Hour,
		MaxBodySize:   2 * 1024,
		ModifyTimeout: 500 * time.Millisecond,
		Cipher:        cipher,
		InternalToken: *internalToken,
	}
	apiServer := api.NewServer(apiCfg, storeMgr, replicaMgr, hasher, rateLimiter)

	// Set up HTTP mux - separate for UDS (local ops) and TCP (replication only)
	udsMux := http.NewServeMux()
	tcpMux := http.NewServeMux()
	apiServer.RegisterRoutes(udsMux)    // All routes on UDS
	apiServer.RegisterRoutes(tcpMux)    // All routes on TCP
	apiServer.RegisterOpsRoutes(udsMux) // Ops routes only on UDS (local access)

	// Wrap TCP handler with internal auth middleware
	var tcpHandler http.Handler = tcpMux
	if *internalToken != "" {
		tcpHandler = internalAuthMiddleware(tcpMux, *internalToken)
	} else if isDevMode {
		log.Printf("WARNING: internal endpoints have no authentication (dev mode)")
	}

	// Start replica manager background tasks
	replicaMgr.Start()

	// Determine initial role (first host in sorted order becomes primary)
	if len(hosts) > 0 && hosts[0].ID == *hostID {
		replicaMgr.SetRole(replica.RolePrimary)
		log.Printf("starting as PRIMARY")
	} else {
		// Non-primary nodes start in JOINING state and initiate recovery
		log.Printf("starting as JOINING, will recover from primary")
		go replicaMgr.StartRecovery()
	}

	// Start UDS server for local component access (includes ops endpoints)
	udsServer := startUDSServer(*udsPath, udsMux, *httpReadTimeout, *httpReadHeaderTimeout, *httpWriteTimeout, *httpIdleTimeout, *httpMaxHeaderBytes)

	// Start TCP server for inter-host communication (no ops endpoints)
	tcpServer := startTCPServer(*tcpAddr, tcpHandler, *httpReadTimeout, *httpReadHeaderTimeout, *httpWriteTimeout, *httpIdleTimeout, *httpMaxHeaderBytes)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	replicaMgr.Stop()
	if rateLimiter != nil {
		rateLimiter.Stop()
	}
	if err := udsServer.Shutdown(ctx); err != nil {
		log.Printf("UDS server shutdown error: %v", err)
	}
	if err := tcpServer.Shutdown(ctx); err != nil {
		log.Printf("TCP server shutdown error: %v", err)
	}

	log.Println("shutdown complete")
}

func buildHostList(selfID, selfAddr, peersStr string) []*routing.Host {
	hosts := []*routing.Host{
		{ID: selfID, Address: selfAddr, Healthy: true},
	}

	if peersStr != "" {
		for _, peer := range splitPeers(peersStr) {
			// Parse peer as "id@address" or just "address" (use address as ID)
			peerID, peerAddr, valid := parsePeer(peer)
			if !valid {
				continue
			}
			hosts = append(hosts, &routing.Host{
				ID:      peerID,
				Address: peerAddr,
				Healthy: true, // Assume healthy initially
			})
		}
	}

	// Sort hosts by ID for deterministic ordering across all nodes
	routing.SortHostsByID(hosts)

	return hosts
}

func parsePeer(peer string) (id, addr string, valid bool) {
	peer = strings.TrimSpace(peer)
	if peer == "" {
		return "", "", false
	}

	for i, c := range peer {
		if c == '@' {
			id, addr = peer[:i], peer[i+1:]
			if id == "" || addr == "" {
				log.Printf("warning: malformed peer %q (empty id or address), skipping", peer)
				return "", "", false
			}
			return id, addr, true
		}
	}
	// No @ found, use address as ID
	if peer == "" {
		return "", "", false
	}
	return peer, peer, true
}

func splitPeers(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func internalAuthMiddleware(next http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only protect /internal/* routes
		if strings.HasPrefix(r.URL.Path, "/internal/") {
			providedToken := r.Header.Get("X-Internal-Token")
			if providedToken != token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func startUDSServer(path string, handler http.Handler, readTimeout, readHeaderTimeout, writeTimeout, idleTimeout time.Duration, maxHeaderBytes int) *http.Server {
	// Remove existing socket file (ignore error if it doesn't exist)
	_ = os.Remove(path)

	listener, err := net.Listen("unix", path)
	if err != nil {
		log.Fatalf("failed to listen on UDS %s: %v", path, err)
	}

	// Set socket permissions - owner only by default for security
	// (ops endpoints like promote/release-lock are accessible via UDS)
	if err := os.Chmod(path, 0o600); err != nil {
		log.Printf("warning: failed to chmod socket: %v", err)
	}

	server := &http.Server{
		Handler:           handler,
		ReadTimeout:       readTimeout,
		ReadHeaderTimeout: readHeaderTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
	go func() {
		log.Printf("UDS server listening on %s", path)
		if err := server.Serve(listener); err != http.ErrServerClosed {
			log.Printf("UDS server error: %v", err)
		}
	}()

	return server
}

func startTCPServer(addr string, handler http.Handler, readTimeout, readHeaderTimeout, writeTimeout, idleTimeout time.Duration, maxHeaderBytes int) *http.Server {
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadTimeout:       readTimeout,
		ReadHeaderTimeout: readHeaderTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}

	go func() {
		log.Printf("TCP server listening on %s", addr)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("TCP server error: %v", err)
		}
	}()

	return server
}

func init() {
	// Ensure tmp directory exists for socket
	if err := os.MkdirAll("/tmp", 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not create /tmp: %v\n", err)
	}
}

// CLI Commands

func printUsage() {
	fmt.Println(`bbd - session store daemon

Usage:
  bbd [daemon flags]           Start the daemon

Ops commands:
  bbd status [options]                       Show node status
  bbd promote [options]                      Force promotion to primary
  bbd release-lock [options] <store-id>      Force release a lock

Store commands:
  bbd create [options]                       Create a blob store (anonymous or named)
  bbd get [options] <store-id>               Get store contents
  bbd delete [options] <store-id>            Delete a store by ID
  bbd delete-named [options] <name>          Delete a store by name
  bbd lookup [options] <name>                Lookup store ID by name

Modify commands:
  bbd begin-modify [options] <store-id>      Begin modify (returns lock ID)
  bbd complete-modify -lock <lock-id> [options] <store-id>
                                             Complete modify with new data
  bbd cancel-modify -lock <lock-id> [options] <store-id>
                                             Cancel modify operation

Counter commands:
  bbd counter-create [options]               Create a counter store (anonymous or named)
  bbd counter-get [options] <store-id>       Get counter value
  bbd counter-increment [-delta N] [options] <store-id>
                                             Increment counter
  bbd counter-decrement [-delta N] [options] <store-id>
                                             Decrement counter
  bbd counter-set -value N [options] <store-id>
                                             Set counter to specific value

Daemon flags:
  --host-id           Unique host identifier (default: host1)
  --site              Site identifier (default: local)
  --uds               Unix socket path (default: /tmp/bbd.sock)
  --tcp               TCP address for replication (default: :8080)
  --peers             Comma-separated peer list in format: id@host:port,...
                      Example: node2@localhost:8082,node3@localhost:8083
  --memory-limit      Max memory in bytes (default: 0 = no limit)
  --customer-memory-quota  Max memory per customer in bytes (default: 0 = no limit)
  --store-keys        Encryption keys (id:hexkey,... or 'dev')
  --store-key-current Current encryption key ID
  --routing-secret    Routing secret (32-byte hex, or 'dev')
  --internal-token    Shared secret for internal endpoints
  --dev               Enable dev mode (no auth required)
  --rate-limit        Max requests per second per customer (default: 100, 0 = no limit)
  --burst-size        Burst capacity per customer (default: 200)
  --tombstone-customer-limit  Max tombstones per customer (default: 0 = no limit)
  --tombstone-global-limit    Max tombstones globally (default: 0 = no limit)
  --disable-site-verification Disable site verification (allows cross-site replication)
  --http-read-timeout         HTTP read timeout (default: 30s)
  --http-read-header-timeout  HTTP read header timeout (default: 10s)
  --http-write-timeout        HTTP write timeout (default: 30s)
  --http-idle-timeout         HTTP idle timeout (default: 120s)
  --http-max-header-bytes     Max header bytes (default: 1MB)

Environment variables:
  SERIALD_STORE_KEYS        Same as --store-keys
  SERIALD_STORE_KEY_CURRENT Same as --store-key-current
  SERIALD_ROUTING_SECRET    Same as --routing-secret
  SERIALD_INTERNAL_TOKEN    Same as --internal-token

Common options (all commands):
  --uds               Unix socket path (default: /tmp/bbd.sock)
  --customer          Customer ID (default: test-customer)

Status options:
  --json              Output raw JSON

Create options:
  --ttl               TTL in seconds (0 = default 14 days)
  --data              Store data (or read from stdin)
  --name              Optional name for named store
  --reuse             Reuse existing store if name exists

Get options:
  --ttl               Show remaining TTL

Begin-modify options:
  --data              Also print current data to stderr

Complete-modify options:
  --lock              Lock ID from begin-modify (required)
  --ttl               New TTL in seconds (0 = keep existing)
  --data              New store data (or read from stdin)

Cancel-modify options:
  --lock              Lock ID from begin-modify (required)

Counter-create options:
  --value             Initial counter value (default: 0)
  --min               Minimum value (requires --with-min)
  --max               Maximum value (requires --with-max)
  --with-min          Enable minimum bound
  --with-max          Enable maximum bound
  --name              Optional name for named counter
  --reuse             Reuse existing named counter if exists
  --ttl               TTL in seconds (0 = default 14 days)

Counter-increment/decrement options:
  --delta             Amount to increment/decrement (default: 1)
  --ttl               TTL in seconds (0 = keep existing)

Counter-set options:
  --value             Value to set (required)
  --ttl               TTL in seconds (0 = keep existing)

Examples:
  # Start a single node (dev mode)
  bbd --dev --host-id=node1 --tcp=:8081 --uds=/tmp/bbd.sock

  # Start a two-node cluster (dev mode)
  # Node 1 (becomes primary - lexicographically smallest host-id)
  bbd --dev --host-id=node1 --tcp=:8081 --uds=/tmp/bbd1.sock \
    --peers=node2@localhost:8082

  # Node 2 (becomes secondary)
  bbd --dev --host-id=node2 --tcp=:8082 --uds=/tmp/bbd2.sock \
    --peers=node1@localhost:8081

  # Production mode (requires explicit keys, routing secret, and internal token)
  bbd --host-id=node1 --tcp=:8081 --uds=/tmp/bbd.sock \
    --store-keys="0:$(openssl rand -hex 32)" --store-key-current=0 \
    --routing-secret="$(openssl rand -hex 32)" \
    --internal-token="$(openssl rand -hex 16)"

  # Create a store with inline data
  bbd create --data "hello world"

  # Create a store from stdin
  echo '{"key": "value"}' | bbd create

  # Create a named store
  bbd create --name my-session --data "session data"

  # Get store contents
  bbd get --uds=/tmp/bbd.sock <store-id>

  # Modify a store (atomic read-modify-write)
  LOCK=$(bbd begin-modify --uds=/tmp/bbd.sock <store-id>)
  bbd complete-modify --uds=/tmp/bbd.sock --lock "$LOCK" --data "new data" <store-id>

  # Cancel a modify operation
  bbd cancel-modify --uds=/tmp/bbd.sock --lock "$LOCK" <store-id>

  # Check node status
  bbd status --uds=/tmp/bbd.sock

  # Force promotion to primary (ops command)
  bbd promote --uds=/tmp/bbd.sock

  # Force release a stuck lock (ops command)
  bbd release-lock --uds=/tmp/bbd.sock <store-id>

  # Create an unbounded counter
  bbd counter-create --value 0

  # Create a bounded counter with min/max
  bbd counter-create --value 50 --with-min --min 0 --with-max --max 100

  # Create a named counter (for rate limiting)
  bbd counter-create --name rate-limit:customer1 --value 0 --with-max --max 100

  # Increment a counter
  bbd counter-increment <store-id>

  # Increment by 5
  bbd counter-increment --delta 5 <store-id>

  # Decrement a counter
  bbd counter-decrement --delta 3 <store-id>

  # Get counter value
  bbd counter-get <store-id>

  # Set counter to specific value
  bbd counter-set --value 50 <store-id>

  # Create named counter with bounds (for rate limiting)
  bbd counter-create --name rate-limit:customer1 --value 0 --max 100 --with-max --ttl 60`)
}

func runStatusCommand(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	jsonOutput := fs.Bool("json", false, "output raw JSON")
	mustParseFlagSet(fs, args)

	resp, err := doUDSRequest("GET", *udsPath, "/status", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading response: %v\n", err)
		os.Exit(1)
	}

	if *jsonOutput {
		fmt.Println(string(body))
		return
	}

	// Parse and format nicely
	var status map[string]any
	if err := json.Unmarshal(body, &status); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Host ID:        %v\n", status["host_id"])
	fmt.Printf("Role:           %v\n", status["role"])
	fmt.Printf("Leader Epoch:   %v\n", status["leader_epoch"])
	fmt.Printf("Last Leader:    %v ago\n", status["last_leader_seen"])
	fmt.Printf("Store Count:    %v\n", status["store_count"])
	fmt.Printf("Memory Usage:   %v bytes\n", status["memory_usage"])
	fmt.Printf("Queue Length:   %v\n", status["queue_length"])
	fmt.Printf("Tombstones:     %v\n", status["tombstone_count"])
	fmt.Printf("Registry Count: %v\n", status["registry_count"])

	if peers, ok := status["peers"].(map[string]any); ok && len(peers) > 0 {
		fmt.Println("Peers:")
		for id, lastSeen := range peers {
			fmt.Printf("  %s: %v ago\n", id, lastSeen)
		}
	}

	if states, ok := status["registry_states"].(map[string]any); ok && len(states) > 0 {
		fmt.Println("Registry States:")
		for state, count := range states {
			fmt.Printf("  %s: %v\n", state, count)
		}
	}
}

func runPromoteCommand(args []string) {
	fs := flag.NewFlagSet("promote", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	mustParseFlagSet(fs, args)

	resp, err := doUDSRequest("POST", *udsPath, "/internal/promote", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "promote failed: %s\n", string(body))
		os.Exit(1)
	}

	fmt.Println("promoted to PRIMARY")
}

func runReleaseLockCommand(args []string) {
	fs := flag.NewFlagSet("release-lock", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	mustParseFlagSet(fs, args)

	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(os.Stderr, "usage: bbd release-lock [options] <store-id>\n")
		os.Exit(1)
	}
	storeID := remaining[0]

	resp, err := doUDSRequest("POST", *udsPath, "/internal/release-lock/"+storeID, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "release-lock failed: %s\n", string(body))
		os.Exit(1)
	}

	fmt.Printf("lock released on %s\n", storeID)
}

func doUDSRequest(method, socketPath, path string, body io.Reader) (*http.Response, error) {
	return doUDSRequestWithHeaders(method, socketPath, path, body, nil)
}

func mustParseFlagSet(fs *flag.FlagSet, args []string) {
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing flags: %v\n", err)
		os.Exit(1)
	}
}

func readBodyData(data string) []byte {
	if data != "" {
		return []byte(data)
	}
	bodyData, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading stdin: %v\n", err)
		os.Exit(1)
	}
	return bodyData
}

func checkResponse(resp *http.Response, operation string) {
	if resp.StatusCode == http.StatusOK {
		return
	}
	body, _ := io.ReadAll(resp.Body)
	fmt.Fprintf(os.Stderr, "%s failed (%d): %s\n", operation, resp.StatusCode, string(body))
	if errCode := resp.Header.Get("BigBunny-Error-Code"); errCode != "" {
		fmt.Fprintf(os.Stderr, "error code: %s\n", errCode)
	}
	os.Exit(1)
}

func printWarning(resp *http.Response) {
	if warning := resp.Header.Get("BigBunny-Warning"); warning != "" {
		fmt.Fprintf(os.Stderr, "warning: %s\n", warning)
	}
}

func printJSONResponse(r io.Reader) {
	body, _ := io.ReadAll(r)
	var result map[string]any
	if err := json.Unmarshal(body, &result); err == nil {
		prettyJSON, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(prettyJSON))
	} else {
		fmt.Println(string(body))
	}
}

func doUDSRequestWithHeaders(method, socketPath, path string, body io.Reader, headers map[string]string) (*http.Response, error) {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", socketPath)
			},
		},
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequest(method, "http://localhost"+path, body)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	return client.Do(req)
}

func runCreateCommand(args []string) {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	ttl := fs.Int("ttl", 0, "TTL in seconds (0 = default 14 days)")
	data := fs.String("data", "", "store data (or read from stdin if empty)")
	name := fs.String("name", "", "optional name for named store")
	reuse := fs.Bool("reuse", false, "reuse existing store if name exists")
	mustParseFlagSet(fs, args)

	bodyData := readBodyData(*data)

	headers := map[string]string{"X-Customer-ID": *customerID}
	if *ttl > 0 {
		headers["BigBunny-Not-Valid-After"] = fmt.Sprintf("%d", *ttl)
	}
	if *reuse {
		headers["BigBunny-Reuse-If-Exists"] = "true"
	}

	endpoint := "/api/v1/create"
	if *name != "" {
		endpoint = "/api/v1/create-by-name/" + *name
	}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, endpoint, strings.NewReader(string(bodyData)), headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "create")

	body, _ := io.ReadAll(resp.Body)
	fmt.Println(strings.TrimSpace(string(body)))
	printWarning(resp)
}

func runGetCommand(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	showTTL := fs.Bool("ttl", false, "show remaining TTL")
	mustParseFlagSet(fs, args)

	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(os.Stderr, "usage: bbd get [options] <store-id>\n")
		os.Exit(1)
	}
	storeID := remaining[0]

	headers := map[string]string{"X-Customer-ID": *customerID}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, "/api/v1/snapshot/"+storeID, nil, headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "get")

	if *showTTL {
		if ttlStr := resp.Header.Get("BigBunny-Not-Valid-After"); ttlStr != "" {
			fmt.Fprintf(os.Stderr, "TTL: %s seconds\n", ttlStr)
		}
	}

	body, _ := io.ReadAll(resp.Body)
	fmt.Print(string(body))
}

func runDeleteCommand(args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	mustParseFlagSet(fs, args)

	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(os.Stderr, "usage: bbd delete [options] <store-id>\n")
		os.Exit(1)
	}
	storeID := remaining[0]

	headers := map[string]string{"X-Customer-ID": *customerID}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, "/api/v1/delete/"+storeID, nil, headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "delete")
	fmt.Println("deleted")
	printWarning(resp)
}

func runDeleteNamedCommand(args []string) {
	fs := flag.NewFlagSet("delete-named", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	mustParseFlagSet(fs, args)

	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(os.Stderr, "usage: bbd delete-named [options] <name>\n")
		os.Exit(1)
	}
	name := remaining[0]

	headers := map[string]string{"X-Customer-ID": *customerID}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, "/api/v1/delete-by-name/"+name, nil, headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "delete-named")
	fmt.Println("deleted")
	printWarning(resp)
}

func runLookupCommand(args []string) {
	fs := flag.NewFlagSet("lookup", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	mustParseFlagSet(fs, args)

	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(os.Stderr, "usage: bbd lookup [options] <name>\n")
		os.Exit(1)
	}
	name := remaining[0]

	headers := map[string]string{"X-Customer-ID": *customerID}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, "/api/v1/lookup-id-by-name/"+name, nil, headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "lookup")

	body, _ := io.ReadAll(resp.Body)
	fmt.Println(strings.TrimSpace(string(body)))
}

func runBeginModifyCommand(args []string) {
	fs := flag.NewFlagSet("begin-modify", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	showData := fs.Bool("data", false, "also print current data")
	mustParseFlagSet(fs, args)

	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(os.Stderr, "usage: bbd begin-modify [options] <store-id>\n")
		os.Exit(1)
	}
	storeID := remaining[0]

	headers := map[string]string{"X-Customer-ID": *customerID}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, "/api/v1/begin-modify/"+storeID, nil, headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "begin-modify")

	fmt.Println(resp.Header.Get("BigBunny-Lock-ID"))

	if ttlStr := resp.Header.Get("BigBunny-Not-Valid-After"); ttlStr != "" {
		fmt.Fprintf(os.Stderr, "TTL: %s seconds\n", ttlStr)
	}

	if *showData {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Data: %s\n", string(body))
	}
}

func runCompleteModifyCommand(args []string) {
	fs := flag.NewFlagSet("complete-modify", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	lockID := fs.String("lock", "", "lock ID from begin-modify (required)")
	ttl := fs.Int("ttl", 0, "new TTL in seconds (0 = keep existing)")
	data := fs.String("data", "", "new store data (or read from stdin if empty)")
	mustParseFlagSet(fs, args)

	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(os.Stderr, "usage: bbd complete-modify -lock <lock-id> [options] <store-id>\n")
		os.Exit(1)
	}
	storeID := remaining[0]

	if *lockID == "" {
		fmt.Fprintf(os.Stderr, "error: -lock is required\n")
		os.Exit(1)
	}

	bodyData := readBodyData(*data)

	headers := map[string]string{
		"X-Customer-ID":    *customerID,
		"BigBunny-Lock-ID": *lockID,
	}
	if *ttl > 0 {
		headers["BigBunny-Not-Valid-After"] = fmt.Sprintf("%d", *ttl)
	}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, "/api/v1/complete-modify/"+storeID, strings.NewReader(string(bodyData)), headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "complete-modify")
	fmt.Println("modified")
	printWarning(resp)
}

func runCancelModifyCommand(args []string) {
	fs := flag.NewFlagSet("cancel-modify", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	lockID := fs.String("lock", "", "lock ID from begin-modify (required)")
	mustParseFlagSet(fs, args)

	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(os.Stderr, "usage: bbd cancel-modify -lock <lock-id> [options] <store-id>\n")
		os.Exit(1)
	}
	storeID := remaining[0]

	if *lockID == "" {
		fmt.Fprintf(os.Stderr, "error: -lock is required\n")
		os.Exit(1)
	}

	headers := map[string]string{
		"X-Customer-ID":    *customerID,
		"BigBunny-Lock-ID": *lockID,
	}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, "/api/v1/cancel-modify/"+storeID, nil, headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "cancel-modify")
	fmt.Println("cancelled")
}

func runCounterCreateCommand(args []string) {
	fs := flag.NewFlagSet("counter-create", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	ttl := fs.Int("ttl", 0, "TTL in seconds (0 = default 14 days)")
	value := fs.Int64("value", 0, "initial counter value")
	min := fs.Int64("min", 0, "minimum value (use -with-min to set)")
	max := fs.Int64("max", 0, "maximum value (use -with-max to set)")
	withMin := fs.Bool("with-min", false, "enable minimum bound")
	withMax := fs.Bool("with-max", false, "enable maximum bound")
	name := fs.String("name", "", "optional name for named counter")
	reuse := fs.Bool("reuse", false, "reuse existing named counter if exists")
	mustParseFlagSet(fs, args)

	// Build JSON request body
	reqBody := map[string]any{
		"type":  "counter",
		"value": *value,
	}
	if *withMin {
		reqBody["min"] = *min
	}
	if *withMax {
		reqBody["max"] = *max
	}

	bodyJSON, _ := json.Marshal(reqBody)

	headers := map[string]string{
		"X-Customer-ID": *customerID,
		"Content-Type":  "application/json",
	}
	if *ttl > 0 {
		headers["BigBunny-Not-Valid-After"] = fmt.Sprintf("%d", *ttl)
	}
	if *reuse {
		headers["BigBunny-Reuse-If-Exists"] = "true"
	}

	endpoint := "/api/v1/create"
	if *name != "" {
		endpoint = "/api/v1/create-by-name/" + *name
	}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, endpoint, strings.NewReader(string(bodyJSON)), headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "counter-create")

	body, _ := io.ReadAll(resp.Body)
	fmt.Println(strings.TrimSpace(string(body)))
	printWarning(resp)
}

func runCounterGetCommand(args []string) {
	fs := flag.NewFlagSet("counter-get", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	mustParseFlagSet(fs, args)

	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(os.Stderr, "usage: bbd counter-get [options] <store-id>\n")
		os.Exit(1)
	}
	storeID := remaining[0]

	headers := map[string]string{"X-Customer-ID": *customerID}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, "/api/v1/snapshot/"+storeID, nil, headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "counter-get")
	printJSONResponse(resp.Body)
	printWarning(resp)
}

func runCounterIncrementCommand(args []string) {
	runCounterDeltaCommand("increment", args)
}

func runCounterDecrementCommand(args []string) {
	runCounterDeltaCommand("decrement", args)
}

func runCounterDeltaCommand(op string, args []string) {
	fs := flag.NewFlagSet("counter-"+op, flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	delta := fs.Int64("delta", 1, "amount to "+op+" (default 1)")
	ttl := fs.Int("ttl", 0, "TTL in seconds (0 = keep existing)")
	mustParseFlagSet(fs, args)

	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(os.Stderr, "usage: bbd counter-%s [-delta N] [options] <store-id>\n", op)
		os.Exit(1)
	}
	storeID := remaining[0]

	reqBody := map[string]any{"delta": *delta}
	bodyJSON, _ := json.Marshal(reqBody)

	headers := map[string]string{
		"X-Customer-ID": *customerID,
		"Content-Type":  "application/json",
	}
	if *ttl > 0 {
		headers["BigBunny-Not-Valid-After"] = fmt.Sprintf("%d", *ttl)
	}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, "/api/v1/"+op+"/"+storeID, strings.NewReader(string(bodyJSON)), headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "counter-"+op)
	printJSONResponse(resp.Body)
	printWarning(resp)
}

func runCounterSetCommand(args []string) {
	fs := flag.NewFlagSet("counter-set", flag.ExitOnError)
	udsPath := fs.String("uds", "/tmp/bbd.sock", "unix socket path")
	customerID := fs.String("customer", "test-customer", "customer ID")
	value := fs.Int64("value", 0, "value to set (required)")
	ttl := fs.Int("ttl", 0, "TTL in seconds (0 = keep existing)")
	mustParseFlagSet(fs, args)

	remaining := fs.Args()
	if len(remaining) != 1 {
		fmt.Fprintf(os.Stderr, "usage: bbd counter-set -value N [options] <store-id>\n")
		os.Exit(1)
	}
	storeID := remaining[0]

	reqBody := map[string]any{"value": *value}
	bodyJSON, _ := json.Marshal(reqBody)

	headers := map[string]string{
		"X-Customer-ID": *customerID,
		"Content-Type":  "application/json",
	}
	if *ttl > 0 {
		headers["BigBunny-Not-Valid-After"] = fmt.Sprintf("%d", *ttl)
	}

	resp, err := doUDSRequestWithHeaders("POST", *udsPath, "/api/v1/update/"+storeID, strings.NewReader(string(bodyJSON)), headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	checkResponse(resp, "counter-set")
	fmt.Println("ok")
	printWarning(resp)
}
