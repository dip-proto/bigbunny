package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/dip-proto/bigbunny/internal/registry"
	"github.com/dip-proto/bigbunny/internal/replica"
	"github.com/dip-proto/bigbunny/internal/store"
)

type ErrorCode string

const (
	ErrCodeLeaderChanged          ErrorCode = "LeaderChanged"
	ErrCodeStoreUnavailable       ErrorCode = "StoreUnavailable"
	ErrCodeLeaseExpired           ErrorCode = "LeaseExpired"
	ErrCodeLockStateUnknown       ErrorCode = "LockStateUnknown"
	ErrCodeNameCreating           ErrorCode = "NameCreating"
	ErrCodeStoreLocked            ErrorCode = "StoreLocked"
	ErrCodeStoreExpired           ErrorCode = "StoreExpired"
	ErrCodeLockMismatch           ErrorCode = "LockMismatch"
	ErrCodeUnauthorized           ErrorCode = "Unauthorized"
	ErrCodeNotFound               ErrorCode = "NotFound"
	ErrCodeCapacityExceeded       ErrorCode = "CapacityExceeded"
	ErrCodeCustomerQuotaExceeded  ErrorCode = "CustomerQuotaExceeded"
	ErrCodeTypeMismatch           ErrorCode = "TypeMismatch"
	ErrCodeOverflow               ErrorCode = "Overflow"
	ErrCodeValueOutOfBounds       ErrorCode = "ValueOutOfBounds"
	ErrCodeInvalidBounds          ErrorCode = "InvalidBounds"
	ErrCodeTombstoneLimitExceeded ErrorCode = "TombstoneLimitExceeded"
)

type httpError struct {
	code    ErrorCode
	message string
	status  int
}

var errorMap = map[error]httpError{
	store.ErrStoreNotFound:         {ErrCodeNotFound, "store not found", http.StatusNotFound},
	store.ErrStoreExists:           {"", "store already exists", http.StatusConflict},
	store.ErrStoreExpired:          {ErrCodeStoreExpired, "store expired", http.StatusGone},
	store.ErrStoreLocked:           {ErrCodeStoreLocked, "store is locked", http.StatusConflict},
	store.ErrLockMismatch:          {ErrCodeLockMismatch, "lock mismatch", http.StatusConflict},
	store.ErrUnauthorized:          {ErrCodeUnauthorized, "unauthorized", http.StatusForbidden},
	store.ErrCapacityExceeded:      {ErrCodeCapacityExceeded, "capacity exceeded", http.StatusInsufficientStorage},
	store.ErrCustomerQuotaExceeded: {ErrCodeCustomerQuotaExceeded, "customer quota exceeded", http.StatusInsufficientStorage},
	store.ErrTypeMismatch:          {ErrCodeTypeMismatch, "store type mismatch", http.StatusBadRequest},
	store.ErrOverflow:              {ErrCodeOverflow, "counter overflow", http.StatusConflict},
	store.ErrValueOutOfBounds:      {ErrCodeValueOutOfBounds, "value out of bounds", http.StatusBadRequest},
	store.ErrInvalidBounds:         {ErrCodeInvalidBounds, "invalid bounds", http.StatusBadRequest},

	registry.ErrNameExists:          {"", "name already exists", http.StatusConflict},
	registry.ErrNameReserved:        {"", "name reservation in progress", http.StatusConflict},
	registry.ErrNameDeleting:        {"", "name deletion in progress", http.StatusConflict},
	registry.ErrEntryNotFound:       {ErrCodeNotFound, "name not found", http.StatusNotFound},
	registry.ErrUnauthorized:        {ErrCodeUnauthorized, "unauthorized", http.StatusForbidden},
	registry.ErrInvalidState:        {"", "invalid state for operation", http.StatusConflict},
	registry.ErrReservationMismatch: {"", "reservation ID mismatch", http.StatusConflict},

	replica.ErrTombstoneLimitExceeded: {ErrCodeTombstoneLimitExceeded, "tombstone limit exceeded", http.StatusTooManyRequests},
}

func writeHTTPError(w http.ResponseWriter, err error, fallbackMsg string) bool {
	if he, ok := errorMap[err]; ok {
		if he.code != "" {
			writeErrorWithCode(w, he.code, he.message, he.status)
		} else {
			http.Error(w, he.message, he.status)
		}
		return true
	}
	if fallbackMsg != "" {
		http.Error(w, fallbackMsg, http.StatusInternalServerError)
		return true
	}
	return false
}

const (
	HeaderErrorCode  = "BigBunny-Error-Code"
	HeaderWarning    = "BigBunny-Warning"
	HeaderRetryAfter = "Retry-After"
	HeaderLockState  = "BigBunny-Lock-State"
)

const (
	WarningDegradedWrite = "DegradedWrite"
)

func writeErrorWithCode(w http.ResponseWriter, code ErrorCode, message string, status int) {
	w.Header().Set(HeaderErrorCode, string(code))
	http.Error(w, message, status)
}

func writeRetryableError(w http.ResponseWriter, code ErrorCode, message string, retryAfter time.Duration) {
	w.Header().Set(HeaderErrorCode, string(code))
	setRetryAfter(w, retryAfter)
	http.Error(w, message, http.StatusServiceUnavailable)
}

func setRetryAfter(w http.ResponseWriter, d time.Duration) {
	seconds := max(int(d.Seconds()), 1)
	w.Header().Set(HeaderRetryAfter, strconv.Itoa(seconds))
}

func setDegradedWriteWarning(w http.ResponseWriter) {
	w.Header().Set(HeaderWarning, WarningDegradedWrite)
}
