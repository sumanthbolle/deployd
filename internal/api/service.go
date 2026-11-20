// Package api serves the control-plane API over gRPC, gRPC-Web and
// HTTP/JSON (via Connect), plus a small read-only REST surface.
package api

import (
	"context"
	"errors"
	"regexp"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	deploydv1 "github.com/sumanthbolle/deployd/gen/deployd/v1"
	"github.com/sumanthbolle/deployd/gen/deployd/v1/deploydv1connect"
	"github.com/sumanthbolle/deployd/internal/queue"
	"github.com/sumanthbolle/deployd/internal/reconcile"
	"github.com/sumanthbolle/deployd/internal/store"
)

// Server implements deploydv1connect.DeployServiceHandler.
type Server struct {
	Store *store.Store
	Queue *queue.Queue
}

var _ deploydv1connect.DeployServiceHandler = (*Server)(nil)

var (
	nameRE  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	imageRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._/:@-]{0,254}$`)
)

func (s *Server) CreateService(ctx context.Context, req *connect.Request[deploydv1.CreateServiceRequest]) (*connect.Response[deploydv1.CreateServiceResponse], error) {
	m := req.Msg
	if err := validate(m.Name, m.Image, m.Replicas, false); err != nil {
		return nil, err
	}
	var svc store.Service
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		if svc, err = s.Store.CreateService(ctx, tx, m.Name, m.Image, int(m.Replicas)); err != nil {
			return err
		}
		// Same transaction: the spec and the job to apply it commit together.
		return s.Queue.Enqueue(ctx, tx, reconcile.JobKind, m.Name, 0)
	})
	if err != nil {
		return nil, toConnectErr(err)
	}
	return connect.NewResponse(&deploydv1.CreateServiceResponse{Service: toProtoService(svc)}), nil
}

func (s *Server) UpdateService(ctx context.Context, req *connect.Request[deploydv1.UpdateServiceRequest]) (*connect.Response[deploydv1.UpdateServiceResponse], error) {
	m := req.Msg
	if err := validate(m.Name, m.Image, m.Replicas, true); err != nil {
		return nil, err
	}
	var svc store.Service
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		if svc, err = s.Store.UpdateService(ctx, tx, m.Name, m.Image, int(m.Replicas)); err != nil {
			return err
		}
		return s.Queue.Enqueue(ctx, tx, reconcile.JobKind, m.Name, 0)
	})
	if err != nil {
		return nil, toConnectErr(err)
	}
	return connect.NewResponse(&deploydv1.UpdateServiceResponse{Service: toProtoService(svc)}), nil
}

func (s *Server) GetService(ctx context.Context, req *connect.Request[deploydv1.GetServiceRequest]) (*connect.Response[deploydv1.GetServiceResponse], error) {
	res, err := s.getService(ctx, req.Msg.Name)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(res), nil
}

func (s *Server) getService(ctx context.Context, name string) (*deploydv1.GetServiceResponse, error) {
	svc, err := s.Store.GetService(ctx, s.Store.Pool, name)
	if err != nil {
		return nil, toConnectErr(err)
	}
	insts, err := s.Store.ListInstances(ctx, s.Store.Pool, name)
	if err != nil {
		return nil, toConnectErr(err)
	}
	res := &deploydv1.GetServiceResponse{Service: toProtoService(svc)}
	for _, in := range insts {
		res.Instances = append(res.Instances, &deploydv1.Instance{
			Id: in.ID, Image: in.Image, Healthy: in.Healthy, CreatedAt: ts(in.CreatedAt),
		})
	}
	if r, err := s.Store.LatestRollout(ctx, s.Store.Pool, name); err == nil {
		res.LatestRollout = toProtoRollout(r)
	}
	return res, nil
}

func (s *Server) ListServices(ctx context.Context, _ *connect.Request[deploydv1.ListServicesRequest]) (*connect.Response[deploydv1.ListServicesResponse], error) {
	svcs, err := s.Store.ListServices(ctx)
	if err != nil {
		return nil, toConnectErr(err)
	}
	res := &deploydv1.ListServicesResponse{}
	for _, sv := range svcs {
		res.Services = append(res.Services, toProtoService(sv))
	}
	return connect.NewResponse(res), nil
}

func (s *Server) ListRollouts(ctx context.Context, req *connect.Request[deploydv1.ListRolloutsRequest]) (*connect.Response[deploydv1.ListRolloutsResponse], error) {
	if _, err := s.Store.GetService(ctx, s.Store.Pool, req.Msg.Name); err != nil {
		return nil, toConnectErr(err)
	}
	rs, err := s.Store.ListRollouts(ctx, req.Msg.Name)
	if err != nil {
		return nil, toConnectErr(err)
	}
	res := &deploydv1.ListRolloutsResponse{}
	for _, r := range rs {
		res.Rollouts = append(res.Rollouts, toProtoRollout(r))
	}
	return connect.NewResponse(res), nil
}

func validate(name, image string, replicas int32, update bool) error {
	switch {
	case !nameRE.MatchString(name):
		return connect.NewError(connect.CodeInvalidArgument, errors.New("name must be lowercase letters, digits and dashes, starting with a letter"))
	case !(update && image == "") && !imageRE.MatchString(image):
		return connect.NewError(connect.CodeInvalidArgument, errors.New("image is not a valid image reference"))
	case !(update && replicas == 0) && (replicas < 1 || replicas > 50):
		return connect.NewError(connect.CodeInvalidArgument, errors.New("replicas must be between 1 and 50"))
	}
	return nil
}

// toConnectErr maps storage errors to API codes. Unknown errors become
// Internal and their text is not sent to the client.
func toConnectErr(err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, store.ErrAlreadyExists):
		return connect.NewError(connect.CodeAlreadyExists, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	default:
		return connect.NewError(connect.CodeInternal, errors.New("internal error"))
	}
}

func toProtoService(s store.Service) *deploydv1.Service {
	return &deploydv1.Service{
		Name: s.Name, Image: s.Image, Replicas: int32(s.Replicas),
		Generation: s.Generation, ObservedGeneration: s.ObservedGeneration,
		Status: s.Status, UpdatedAt: ts(s.UpdatedAt),
	}
}

func toProtoRollout(r store.Rollout) *deploydv1.Rollout {
	out := &deploydv1.Rollout{
		Id: r.ID, FromImage: r.FromImage, ToImage: r.ToImage, State: r.State,
		FailedChecks: int32(r.FailedChecks), CreatedAt: ts(r.CreatedAt),
	}
	if r.FinishedAt != nil {
		out.FinishedAt = ts(*r.FinishedAt)
	}
	return out
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }
