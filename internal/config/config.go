// Package config reads settings from environment variables (12-factor style),
// so the same binary runs locally, in Docker and in Kubernetes.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr        string // API listen address, e.g. ":8080"
	OpsAddr     string // health and metrics listen address, e.g. ":9090"
	DatabaseURL string // postgres://user:pass@host:5432/db
	Tokens      string // "token:viewer,token:deployer"
	LogLevel    string

	Workers          int
	MaxSurge         int
	FailureThreshold int
	PollInterval     time.Duration
	Lease            time.Duration
	PassTimeout      time.Duration
	Requeue          time.Duration
	ShutdownTimeout  time.Duration

	// TLS. Set both cert and key to serve HTTPS. Also set ClientCA to require
	// client certificates signed by that CA (mutual TLS).
	TLSCert  string
	TLSKey   string
	ClientCA string
}

func Load() (Config, error) {
	c := Config{
		Addr:        env("DEPLOYD_ADDR", ":8080"),
		OpsAddr:     env("DEPLOYD_OPS_ADDR", ":9090"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		Tokens:      os.Getenv("DEPLOYD_TOKENS"),
		LogLevel:    env("DEPLOYD_LOG_LEVEL", "info"),
		TLSCert:     os.Getenv("DEPLOYD_TLS_CERT"),
		TLSKey:      os.Getenv("DEPLOYD_TLS_KEY"),
		ClientCA:    os.Getenv("DEPLOYD_CLIENT_CA"),
	}
	var errs []error
	c.Workers = envInt("DEPLOYD_WORKERS", 4, &errs)
	c.MaxSurge = envInt("DEPLOYD_MAX_SURGE", 1, &errs)
	c.FailureThreshold = envInt("DEPLOYD_FAILURE_THRESHOLD", 3, &errs)
	c.PollInterval = envDur("DEPLOYD_POLL_INTERVAL", 500*time.Millisecond, &errs)
	c.Lease = envDur("DEPLOYD_JOB_LEASE", 30*time.Second, &errs)
	c.PassTimeout = envDur("DEPLOYD_PASS_TIMEOUT", 10*time.Second, &errs)
	c.Requeue = envDur("DEPLOYD_REQUEUE", time.Second, &errs)
	c.ShutdownTimeout = envDur("DEPLOYD_SHUTDOWN_TIMEOUT", 20*time.Second, &errs)

	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if c.Tokens == "" {
		errs = append(errs, errors.New("DEPLOYD_TOKENS is required"))
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		errs = append(errs, errors.New("set both DEPLOYD_TLS_CERT and DEPLOYD_TLS_KEY, or neither"))
	}
	if c.ClientCA != "" && c.TLSCert == "" {
		errs = append(errs, errors.New("DEPLOYD_CLIENT_CA needs TLS enabled"))
	}
	if c.PassTimeout >= c.Lease {
		errs = append(errs, errors.New("DEPLOYD_PASS_TIMEOUT must be shorter than DEPLOYD_JOB_LEASE"))
	}
	return c, errors.Join(errs...)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int, errs *[]error) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		*errs = append(*errs, fmt.Errorf("%s must be a positive integer", k))
		return def
	}
	return n
}

func envDur(k string, def time.Duration, errs *[]error) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		*errs = append(*errs, fmt.Errorf("%s must be a positive duration like 500ms or 30s", k))
		return def
	}
	return d
}
