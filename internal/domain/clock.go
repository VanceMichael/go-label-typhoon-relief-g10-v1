package domain

import "time"

type Clock interface{ Now() time.Time }
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now().UTC() }

type FixedClock struct{ Value time.Time }

func (c FixedClock) Now() time.Time         { return c.Value }
func IsExpired(now, until time.Time) bool   { return !until.IsZero() && !until.After(now) }
func Within(now, start, end time.Time) bool { return !now.Before(start) && now.Before(end) }
