package httpapi

import (
	"bytes"
	"context"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"sync"
)

type bufferedResponse struct {
	Status int
	Header map[string][]string
	Body   bytes.Buffer
}

func newBufferedResponse() *bufferedResponse {
	return &bufferedResponse{Status: 200, Header: map[string][]string{}}
}
func (r *bufferedResponse) Write(p []byte) (int, error) { return r.Body.Write(p) }

type ResponseCache struct {
	mu    sync.RWMutex
	items map[string]bufferedResponse
}

func NewResponseCache() *ResponseCache { return &ResponseCache{items: map[string]bufferedResponse{}} }
func (c *ResponseCache) Key(org, method, path, key string) string {
	return org + "\x00" + method + "\x00" + path + "\x00" + key
}
func (c *ResponseCache) Get(ctx context.Context, key string) (bufferedResponse, bool) {
	if err := ctx.Err(); err != nil {
		return bufferedResponse{}, false
	}
	c.mu.RLock()
	value, ok := c.items[key]
	c.mu.RUnlock()
	if !ok {
		return bufferedResponse{}, false
	}
	value.Body = *bytes.NewBuffer(append([]byte(nil), value.Body.Bytes()...))
	return value, true
}
func (c *ResponseCache) Put(ctx context.Context, key string, value bufferedResponse) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	value.Body = *bytes.NewBuffer(append([]byte(nil), value.Body.Bytes()...))
	c.mu.Lock()
	c.items[key] = value
	c.mu.Unlock()
	return nil
}
func (c *ResponseCache) Delete(key string) { c.mu.Lock(); defer c.mu.Unlock(); delete(c.items, key) }
func ensureIdempotencyKey(method, path, key string) error {
	if method == "" || path == "" || key == "" {
		return domain.ErrValidation
	}
	return nil
}
