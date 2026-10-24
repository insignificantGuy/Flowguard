// Package main is the entry point for the flowguard server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"flowguard/internal/config"
	"flowguard/internal/logger"
	"flowguard/internal/router"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	// --- Parse flags --------------------------------------------------------
	var (
		showVersion = flag.Bool("version", false, "print version and exit")
		port        = flag.Int("port", 0, "port to listen on (overrides PORT env)")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("flowguard %s (%s) built %s\n", version, commit, date)
		return
	}

	// --- Load config --------------------------------------------------------
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if *port != 0 {
		cfg.Port = *port
	}

	// --- Build app (fails fast if Kafka is misconfigured) ------------------
	app, err := router.Build(cfg)
	if err != nil {
		logger.Log.Fatal().Err(err).Msg("failed to build app")
	}

	// --- HTTP server --------------------------------------------------------
	srv := &http.Server{
		Addr:         cfg.Host + ":" + fmt.Sprint(cfg.Port),
		Handler:      app,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// --- Graceful shutdown --------------------------------------------------
	// Stop accepting new connections, finish in-flight requests, flush the
	// Kafka producer, then exit. Required for horizontal scaling behind a load
	// balancer so we don't drop in-flight requests during deploys.
	go func() {
		logger.Log.Info().
			Str("addr", srv.Addr).
			Str("env", cfg.Env).
			Str("version", version).
			Msg("server listening")

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Fatal().Err(err).Msg("server failed")
		}
	}()

	// Wait for SIGTERM or SIGINT
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop

	logger.Log.Info().Msg("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Log.Error().Err(err).Msg("graceful shutdown failed")
		os.Exit(1)
	}
	// HTTP has drained; stop consumer workers and release Kafka resources.
	if err := app.Close(); err != nil {
		logger.Log.Error().Err(err).Msg("kafka producer close failed")
	}

	logger.Log.Info().Msg("server stopped")
}
