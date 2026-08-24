package main

import (
	"context"
	"errors"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/auth"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/config"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/httpapi"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/worker"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	db, err := sqlite.Open(cfg.DatabasePath, cfg.MigrationsPath)
	if err != nil {
		logger.Error("open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := auth.Bootstrap(context.Background(), db, "commander@example.test", "secret"); err != nil {
		logger.Error("bootstrap", "error", err)
		os.Exit(1)
	}
	runner := worker.New(db, cfg, logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go runner.Run(ctx)
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: httpapi.New(db, cfg, logger).Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		logger.Info("server listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	runner.Wait()
}
