package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	deploydv1 "github.com/sumanthbolle/deployd/gen/deployd/v1"
	"github.com/sumanthbolle/deployd/gen/deployd/v1/deploydv1connect"
	"github.com/sumanthbolle/deployd/internal/telemetry"
)

// NewMux wires the public API:
//
//	/deployd.v1.DeployService/*  gRPC, gRPC-Web and Connect (HTTP/JSON)
//	GET /v1/services             REST, read-only
//	GET /v1/services/{name}      REST, read-only
func NewMux(srv *Server, tokens Tokens) *http.ServeMux {
	mux := http.NewServeMux()

	path, h := deploydv1connect.NewDeployServiceHandler(srv,
		connect.WithInterceptors(ObserveInterceptor(), AuthInterceptor(tokens)))
	mux.Handle(path, h)

	mux.Handle("GET /v1/services", requireViewer(tokens, func(w http.ResponseWriter, r *http.Request) {
		res, err := srv.ListServices(r.Context(), connect.NewRequest(&deploydv1.ListServicesRequest{}))
		writeProto(w, res, err)
	}))
	mux.Handle("GET /v1/services/{name}", requireViewer(tokens, func(w http.ResponseWriter, r *http.Request) {
		res, err := srv.getService(r.Context(), r.PathValue("name"))
		writeProto(w, res, err)
	}))
	return mux
}

// NewOpsMux serves operational endpoints on a separate, internal-only port:
//
//	GET /healthz  liveness: the process is up
//	GET /readyz   readiness: the database answers
//	GET /metrics  Prometheus
//
// Keeping them off the API port matters with mutual TLS: the kubelet and
// Prometheus do not present client certificates, so they could not reach
// probes or metrics on an mTLS-only listener.
func NewOpsMux(pool *pgxpool.Pool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n")) //nolint:errcheck
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ready\n")) //nolint:errcheck
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(telemetry.Registry, promhttp.HandlerOpts{}))
	return mux
}

func requireViewer(tokens Tokens, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tokens.RoleFor(r.Header.Get("Authorization")) < RoleViewer {
			http.Error(w, `{"error":"missing or invalid bearer token"}`, http.StatusUnauthorized)
			return
		}
		next(w, r)
	})
}

type messageHolder interface{ Any() any }

// writeProto writes either a proto message (or a Connect response wrapping
// one) as JSON, or the matching HTTP error.
func writeProto(w http.ResponseWriter, res any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		var ce *connect.Error
		status := http.StatusInternalServerError
		if errors.As(err, &ce) {
			status = httpStatus(ce.Code())
		}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]string{"error": connect.CodeOf(err).String()}) //nolint:errcheck
		return
	}
	if h, ok := res.(messageHolder); ok {
		res = h.Any()
	}
	b, err := protojson.Marshal(res.(proto.Message))
	if err != nil {
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
		return
	}
	w.Write(b) //nolint:errcheck
}

func httpStatus(c connect.Code) int {
	switch c {
	case connect.CodeNotFound:
		return http.StatusNotFound
	case connect.CodeInvalidArgument:
		return http.StatusBadRequest
	case connect.CodeUnauthenticated:
		return http.StatusUnauthorized
	case connect.CodePermissionDenied:
		return http.StatusForbidden
	case connect.CodeAlreadyExists:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
