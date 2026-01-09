package replica

import "errors"

var ErrJoinInProgress = errors.New("recovery in progress")
