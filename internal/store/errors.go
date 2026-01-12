package store

import "errors"

// ErrStoreNotFound is returned when attempting to access a store that does not exist.
var ErrStoreNotFound = errors.New("store not found")

// ErrStoreExists is returned when trying to create a store with an ID that already exists.
var ErrStoreExists = errors.New("store already exists")

// ErrStoreExpired is returned when accessing a store whose TTL has elapsed.
var ErrStoreExpired = errors.New("store expired")

// ErrStoreLocked is returned when trying to modify a store that another operation has locked.
var ErrStoreLocked = errors.New("store is locked")

// ErrLockMismatch is returned when the provided lock ID does not match the store's active lock.
var ErrLockMismatch = errors.New("lock ID mismatch")

// ErrUnauthorized is returned when a customer tries to access a store they do not own.
var ErrUnauthorized = errors.New("unauthorized")

// ErrCapacityExceeded is returned when the global memory limit would be exceeded by an operation.
var ErrCapacityExceeded = errors.New("capacity exceeded")

// ErrCustomerQuotaExceeded is returned when a customer's per-tenant memory quota would be exceeded.
var ErrCustomerQuotaExceeded = errors.New("customer quota exceeded")

// ErrTypeMismatch is returned when performing a counter operation on a blob or vice versa.
var ErrTypeMismatch = errors.New("store type mismatch")

// ErrOverflow is returned when a counter increment or decrement would cause integer overflow.
var ErrOverflow = errors.New("counter overflow")

// ErrValueOutOfBounds is returned when a counter value falls outside its configured min/max bounds.
var ErrValueOutOfBounds = errors.New("value out of bounds")

// ErrInvalidBounds is returned when creating a counter with min greater than max.
var ErrInvalidBounds = errors.New("invalid bounds: min must be <= max")
