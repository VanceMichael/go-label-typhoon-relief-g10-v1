package domain

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func NewID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("id entropy unavailable: %v", err))
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}
