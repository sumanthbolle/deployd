package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"

	deploydv1 "github.com/sumanthbolle/deployd/gen/deployd/v1"
	"github.com/sumanthbolle/deployd/gen/deployd/v1/deploydv1connect"
	"github.com/sumanthbolle/deployd/internal/api"
	"github.com/sumanthbolle/deployd/internal/queue"
	"github.com/sumanthbolle/deployd/internal/store"
	"github.com/sumanthbolle/deployd/internal/testdb"
)

const (
	viewerToken   = "view-token"
	deployerToken = "deploy-token"
)

func setup(t *testing.T) (*httptest.Server, *queue.Queue) {
	srv, q, _ := setupWithOps(t)
	return srv, q
}

func setupWithOps(t *testing.T) (*httptest.Server, *queue.Queue, *httptest.Server) {
	pool := testdb.New(t)
	tokens, err := api.ParseTokens(viewerToken + ":viewer," + deployerToken + ":deployer")
	if err != nil {
		t.Fatal(err)
	}
	q := queue.New(pool, time.Minute)
	srv := httptest.NewServer(api.NewMux(&api.Server{Store: store.New(pool), Queue: q}, tokens))
	t.Cleanup(srv.Close)
	ops := httptest.NewServer(api.NewOpsMux(pool))
	t.Cleanup(ops.Close)
	return srv, q, ops
}

func client(srv *httptest.Server, token string) deploydv1connect.DeployServiceClient {
	return deploydv1connect.NewDeployServiceClient(srv.Client(), srv.URL,
		connect.WithInterceptors(connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
			return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				if token != "" {
					req.Header().Set("Authorization", "Bearer "+token)
				}
				return next(ctx, req)
			}
		})))
}

func TestRBAC(t *testing.T) {
	srv, _ := setup(t)
	ctx := context.Background()
	req := &deploydv1.CreateServiceRequest{Name: "web", Image: "web:1", Replicas: 2}

	_, err := client(srv, "").CreateService(ctx, connect.NewRequest(req))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("no token: got %v, want Unauthenticated", err)
	}
	_, err = client(srv, viewerToken).CreateService(ctx, connect.NewRequest(req))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("viewer create: got %v, want PermissionDenied", err)
	}
	if _, err := client(srv, deployerToken).CreateService(ctx, connect.NewRequest(req)); err != nil {
		t.Fatalf("deployer create: %v", err)
	}
	if _, err := client(srv, viewerToken).GetService(ctx, connect.NewRequest(&deploydv1.GetServiceRequest{Name: "web"})); err != nil {
		t.Fatalf("viewer get: %v", err)
	}
}

func TestCreateValidatesAndEnqueues(t *testing.T) {
	srv, q := setup(t)
	ctx := context.Background()
	c := client(srv, deployerToken)

	for _, bad := range []*deploydv1.CreateServiceRequest{
		{Name: "Web", Image: "web:1", Replicas: 1},
		{Name: "web", Image: "", Replicas: 1},
		{Name: "web", Image: "web:1", Replicas: 0},
		{Name: "web", Image: "web:1", Replicas: 51},
	} {
		if _, err := c.CreateService(ctx, connect.NewRequest(bad)); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%v: got %v, want InvalidArgument", bad, err)
		}
	}

	good := &deploydv1.CreateServiceRequest{Name: "web", Image: "web:1", Replicas: 2}
	res, err := c.CreateService(ctx, connect.NewRequest(good))
	if err != nil {
		t.Fatal(err)
	}
	if res.Msg.Service.Status != "Progressing" || res.Msg.Service.Generation != 1 {
		t.Fatalf("unexpected service %+v", res.Msg.Service)
	}
	if _, err := c.CreateService(ctx, connect.NewRequest(good)); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("duplicate: got %v, want AlreadyExists", err)
	}
	if n, _ := q.Depth(ctx); n != 1 {
		t.Fatalf("queue depth %d, want 1 reconcile job", n)
	}
}

func TestRESTAndProbes(t *testing.T) {
	srv, _, ops := setupWithOps(t)
	client(srv, deployerToken).CreateService(context.Background(), //nolint:errcheck
		connect.NewRequest(&deploydv1.CreateServiceRequest{Name: "web", Image: "web:1", Replicas: 1}))

	get := func(path, token string) *http.Response {
		base := srv.URL
		if path == "/healthz" || path == "/readyz" || path == "/metrics" {
			base = ops.URL
		}
		req, _ := http.NewRequest("GET", base+path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { res.Body.Close() })
		return res
	}

	if res := get("/v1/services/web", ""); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: %d", res.StatusCode)
	}
	res := get("/v1/services/web", viewerToken)
	var body struct {
		Service struct{ Name, Image string }
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil || body.Service.Image != "web:1" {
		t.Fatalf("GET /v1/services/web: status %d, body %+v, err %v", res.StatusCode, body, err)
	}
	if res := get("/v1/services/missing", viewerToken); res.StatusCode != http.StatusNotFound {
		t.Fatalf("missing: %d, want 404", res.StatusCode)
	}
	for _, p := range []string{"/healthz", "/readyz", "/metrics"} {
		if res := get(p, ""); res.StatusCode != http.StatusOK {
			t.Errorf("%s: %d", p, res.StatusCode)
		}
	}
}

// The same handler speaks the gRPC protocol over HTTP/2.
func TestGRPCProtocol(t *testing.T) {
	pool := testdb.New(t)
	tokens, _ := api.ParseTokens(deployerToken + ":deployer")
	ts := httptest.NewUnstartedServer(api.NewMux(&api.Server{Store: store.New(pool), Queue: queue.New(pool, time.Minute)}, tokens))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)

	c := deploydv1connect.NewDeployServiceClient(ts.Client(), ts.URL, connect.WithGRPC())
	req := connect.NewRequest(&deploydv1.CreateServiceRequest{Name: "grpc-svc", Image: "svc:1", Replicas: 1})
	req.Header().Set("Authorization", "Bearer "+deployerToken)
	res, err := c.CreateService(context.Background(), req)
	if err != nil {
		t.Fatalf("gRPC create: %v", err)
	}
	if res.Msg.Service.Name != "grpc-svc" {
		t.Fatalf("got %+v", res.Msg.Service)
	}
}
