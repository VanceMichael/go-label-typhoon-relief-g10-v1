package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr, DatabasePath, MigrationsPath, WebhookURL string
	SessionTTL, WorkerInterval, WorkerLease            time.Duration
}

func Load() Config {
	return Config{HTTPAddr: env("HTTP_ADDR", ":8080"), DatabasePath: env("DATABASE_PATH", "./typhoon-relief.db"), MigrationsPath: env("MIGRATIONS_PATH", "./migrations"), WebhookURL: os.Getenv("WEBHOOK_URL"), SessionTTL: duration("SESSION_TTL", 8*time.Hour), WorkerInterval: duration("WORKER_INTERVAL", 2*time.Second), WorkerLease: duration("WORKER_LEASE", time.Minute)}
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func duration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}
func Bool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}
