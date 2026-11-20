package api

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	"github.com/sumanthbolle/deployd/internal/telemetry"
)

// Role is a coarse RBAC role. Viewers read; deployers read and write.
type Role int

const (
	RoleNone Role = iota
	RoleViewer
	RoleDeployer
)

// Tokens maps bearer tokens to roles.
type Tokens map[string]Role

// ParseTokens reads "token1:viewer,token2:deployer".
func ParseTokens(s string) (Tokens, error) {
	t := Tokens{}
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		tok, role, ok := strings.Cut(pair, ":")
		if !ok || tok == "" {
			return nil, fmt.Errorf("bad token entry %q, want token:role", pair)
		}
		switch role {
		case "viewer":
			t[tok] = RoleViewer
		case "deployer":
			t[tok] = RoleDeployer
		default:
			return nil, fmt.Errorf("unknown role %q", role)
		}
	}
	if len(t) == 0 {
		return nil, errors.New("no API tokens configured")
	}
	return t, nil
}

// RoleFor checks a raw Authorization header value. Every comparison runs in
// constant time so response timing does not leak how much of a token matched.
func (t Tokens) RoleFor(header string) Role {
	got, ok := strings.CutPrefix(header, "Bearer ")
	if !ok {
		return RoleNone
	}
	role := RoleNone
	for tok, r := range t {
		if subtle.ConstantTimeCompare([]byte(got), []byte(tok)) == 1 {
			role = r
		}
	}
	return role
}

// writeProcedures need RoleDeployer; everything else needs RoleViewer.
var writeProcedures = map[string]bool{
	"/deployd.v1.DeployService/CreateService": true,
	"/deployd.v1.DeployService/UpdateService": true,
}

// AuthInterceptor enforces RBAC on every RPC.
func AuthInterceptor(tokens Tokens) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			role := tokens.RoleFor(req.Header().Get("Authorization"))
			need := RoleViewer
			if writeProcedures[req.Spec().Procedure] {
				need = RoleDeployer
			}
			switch {
			case role == RoleNone:
				return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing or invalid bearer token"))
			case role < need:
				return nil, connect.NewError(connect.CodePermissionDenied, errors.New("token lacks the deployer role"))
			}
			return next(ctx, req)
		}
	}
}

// ObserveInterceptor traces and counts every RPC.
func ObserveInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			ctx, span := telemetry.Tracer.Start(ctx, req.Spec().Procedure)
			defer span.End()
			res, err := next(ctx, req)
			code := "ok"
			if err != nil {
				code = connect.CodeOf(err).String()
			}
			telemetry.RequestsTotal.WithLabelValues(req.Spec().Procedure, code).Inc()
			return res, err
		}
	}
}
