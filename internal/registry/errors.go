package registry

import "errors"

// ErrNameExists is returned when trying to reserve a name that already has an active entry.
var ErrNameExists = errors.New("name already exists")

// ErrNameReserved is returned when a name has a pending reservation that hasn't expired yet.
var ErrNameReserved = errors.New("name reservation in progress")

// ErrNameDeleting is returned when trying to reserve a name that is currently being deleted.
var ErrNameDeleting = errors.New("name deletion in progress")

// ErrEntryNotFound is returned when looking up a name that has no registry entry.
var ErrEntryNotFound = errors.New("registry entry not found")

// ErrUnauthorized is returned when a customer tries to access an entry they don't own.
var ErrUnauthorized = errors.New("unauthorized access to registry entry")

// ErrInvalidState is returned when an operation is attempted on an entry in the wrong state.
var ErrInvalidState = errors.New("invalid entry state for operation")

// ErrReservationMismatch is returned when the provided reservation ID doesn't match the entry's.
var ErrReservationMismatch = errors.New("reservation ID mismatch")
