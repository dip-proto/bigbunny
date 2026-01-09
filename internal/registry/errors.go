package registry

import "errors"

var (
	ErrNameExists          = errors.New("name already exists")
	ErrNameReserved        = errors.New("name reservation in progress")
	ErrNameDeleting        = errors.New("name deletion in progress")
	ErrEntryNotFound       = errors.New("registry entry not found")
	ErrUnauthorized        = errors.New("unauthorized access to registry entry")
	ErrInvalidState        = errors.New("invalid entry state for operation")
	ErrReservationMismatch = errors.New("reservation ID mismatch")
)
