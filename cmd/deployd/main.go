// Command deployd runs the control plane: the API server and the reconcile
// workers in one process. Run several replicas against one database for high
// availability; the queue makes sure each job is handled by one worker.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumanthbolle/deployd/internal/api"
	"github.com/sumanthbolle/deployd/internal/config"
	"github.com/sumanthbolle/deployd/internal/queue"
	"github.com/sumanthbolle/deployd/internal/reconcile"
	"github.com/sumanthbolle/deployd/internal/runtime"
	"github.com/sumanthbolle/deployd/internal/store"
	"github.com/sumanthbolle/deployd/internal/telemetry"
)

// version is set at build time: go build -ldflags "-X main.version=1.2.3"
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "deployd:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := telemetry.NewLogger(cfg.LogLevel)

	// SIGTERM is what Kubernetes sends before killing a pod.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.SetupTracing(ctx, version)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer shutdownTracing(context.Background()) //nolint:errcheck

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	tokens, err := api.ParseTokens(cfg.Tokens)
	if err != nil {
		return err
	}

	st := store.New(pool)
	q := queue.New(pool, cfg.Lease)
	host, _ := os.Hostname()

	workers := &reconcile.Pool{
		Queue: q,
		Reconciler: &reconcile.Reconciler{
			Store: st, Runtime: runtime.Simulated{}, Log: log,
			MaxSurge: cfg.MaxSurge, FailureThreshold: cfg.FailureThreshold,
		},
		Log: log, Workers: cfg.Workers, ID: host,
		PollInterval: cfg.PollInterval, PassTimeout: cfg.PassTimeout, Requeue: cfg.Requeue,
	}

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           api.NewMux(&api.Server{Store: st, Queue: q}, tokens),
		ReadHeaderTimeout: 5 * time.Second,
		Protocols:         new(http.Protocols),
	}
	// gRPC needs HTTP/2. Allow it over plain TCP too (h2c) so it works
	// behind a TLS-terminating load balancer.
	srv.Protocols.SetHTTP1(true)
	srv.Protocols.SetHTTP2(true)
	srv.Protocols.SetUnencryptedHTTP2(true)
	if cfg.TLSCert != "" {
		if srv.TLSConfig, err = tlsConfig(cfg); err != nil {
			return err
		}
	}

	// Ops server: plain HTTP for probes and metrics, never exposed publicly.
	ops := &http.Server{Addr: cfg.OpsAddr, Handler: api.NewOpsMux(pool), ReadHeaderTimeout: 5 * time.Second}

	workersDone := make(chan struct{})
	go func() {
		workers.Run(ctx)
		close(workersDone)
	}()

	serveErr := make(chan error, 2)
	go func() { serveErr <- ops.ListenAndServe() }()
	go func() {
		log.Info("listening", "addr", cfg.Addr, "ops_addr", cfg.OpsAddr, "tls", cfg.TLSCert != "", "mtls", cfg.ClientCA != "", "version", version)
		if cfg.TLSCert != "" {
			serveErr <- srv.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey)
		} else {
			serveErr <- srv.ListenAndServe()
		}
	}()

	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
	}

	// Graceful shutdown: stop accepting requests, let in-flight requests and
	// reconcile passes finish, then exit.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	ops.Shutdown(shutdownCtx) //nolint:errcheck
	select {
	case <-workersDone:
	case <-shutdownCtx.Done():
		log.Warn("workers did not stop in time; their job leases will expire and another replica will retry")
	}
	return nil
}

func tlsConfig(cfg config.Config) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.ClientCA != "" {
		pem, err := os.ReadFile(cfg.ClientCA)
		if err != nil {
			return nil, fmt.Errorf("read client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("client CA file has no PEM certificates")
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tc, nil
}
