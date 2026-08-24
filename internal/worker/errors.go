package worker

import "errors"

var (
	ErrInvalidTask   = errors.New("invalid task")
	ErrDuplicateTask = errors.New("duplicate task")
	ErrMissingTask   = errors.New("missing task")
	ErrTaskRunning   = errors.New("task already running")
)
