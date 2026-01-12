package store

import "errors"

var (
	ErrStoreNotFound    = errors.New("store not found")
	ErrStoreExists      = errors.New("store already exists")
	ErrStoreExpired     = errors.New("store expired")
	ErrStoreLocked      = errors.New("store is locked")
	ErrLockMismatch     = errors.New("lock ID mismatch")
	ErrUnauthorized     = errors.New("unauthorized")
	ErrCapacityExceeded = errors.New("capacity exceeded")
	ErrTypeMismatch     = errors.New("store type mismatch")
	ErrOverflow         = errors.New("counter overflow")
	ErrValueOutOfBounds = errors.New("value out of bounds")
	ErrInvalidBounds    = errors.New("invalid bounds: min must be <= max")
)
