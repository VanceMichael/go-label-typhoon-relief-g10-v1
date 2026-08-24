package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Message struct{ ID, StormID, Channel string }
type Sender interface {
	Send(context.Context, Message) error
}
type WebhookSender struct {
	URL    string
	Client *http.Client
}

func NewWebhookSender(url string) *WebhookSender {
	return &WebhookSender{URL: url, Client: &http.Client{Timeout: 10 * time.Second}}
}
func (s *WebhookSender) Send(ctx context.Context, m Message) error {
	if s.URL == "" {
		return nil
	}
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.Client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook send: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return readErr
		}
		return fmt.Errorf("webhook status %d: %s", resp.StatusCode, string(data))
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}
