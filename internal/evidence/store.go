package evidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"io"
	"sync"
)

type MemoryStore struct {
	mu    sync.RWMutex
	items map[string][]byte
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{items: map[string][]byte{}} }
func (s *MemoryStore) Put(ctx context.Context, uri string, body io.Reader) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if uri == "" {
		return "", domain.ErrValidation
	}
	v, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(v)
	s.mu.Lock()
	s.items[uri] = append([]byte(nil), v...)
	s.mu.Unlock()
	return hex.EncodeToString(sum[:]), nil
}
func (s *MemoryStore) Open(ctx context.Context, uri string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	v, ok := s.items[uri]
	copyV := append([]byte(nil), v...)
	s.mu.RUnlock()
	if !ok {
		return nil, domain.ErrNotFound
	}
	return io.NopCloser(&reader{v: copyV}), nil
}
func (s *MemoryStore) Exists(uri string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.items[uri]
	return ok
}

type reader struct {
	v      []byte
	offset int
}

func (r *reader) Read(p []byte) (int, error) {
	if r.offset >= len(r.v) {
		return 0, io.EOF
	}
	n := copy(p, r.v[r.offset:])
	r.offset += n
	return n, nil
}
func Digest(r io.Reader) (string, error) {
	sum := sha256.New()
	if _, err := io.Copy(sum, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

var _ = errors.Is
