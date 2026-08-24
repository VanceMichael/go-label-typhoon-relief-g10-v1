package domain

import "errors"

var (
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conflict")
	ErrInvalidState = errors.New("invalid state")
	ErrValidation   = errors.New("validation error")
	ErrForbidden    = errors.New("forbidden")
	ErrUnauthorized = errors.New("unauthorized")
	ErrLeaseLost    = errors.New("lease lost")
	ErrUnavailable  = errors.New("unavailable")
)
