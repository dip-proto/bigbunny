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
)
