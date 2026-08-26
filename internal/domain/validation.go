package domain

import (
	"fmt"
	"strings"
)

func Required(values ...string) error {
	for _, v := range values {
		if strings.TrimSpace(v) == "" {
			return ErrValidation
		}
	}
	return nil
}
func Positive(value int) error {
	if value <= 0 {
		return ErrValidation
	}
	return nil
}
func NonNegative(value int) error {
	if value < 0 {
		return ErrValidation
	}
	return nil
}
func RequireTransition(current, next string, allowed map[string][]string) error {
	targets, ok := allowed[current]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidState, current)
	}
	for _, candidate := range targets {
		if candidate == next {
			return nil
		}
	}
	return fmt.Errorf("%w: %s to %s", ErrInvalidState, current, next)
}
func NormalizePage(limit, offset int) (int, int) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
func ValidSeverity(value string) bool {
	switch value {
	case "advisory", "watch", "warning", "emergency":
		return true
	}
	return false
}
