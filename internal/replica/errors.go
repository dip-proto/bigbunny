package replica

import "errors"

var (
	ErrJoinInProgress         = errors.New("recovery in progress")
	ErrTombstoneLimitExceeded = errors.New("tombstone limit exceeded")
)
