package replica

import "errors"

// ErrJoinInProgress is returned when a node cannot accept replication messages because it is currently recovering state from the primary.
var ErrJoinInProgress = errors.New("recovery in progress")

// ErrTombstoneLimitExceeded is returned when adding a tombstone would exceed either the per-customer or global tombstone limit, which helps prevent memory exhaustion from rapid create-delete cycles.
var ErrTombstoneLimitExceeded = errors.New("tombstone limit exceeded")
