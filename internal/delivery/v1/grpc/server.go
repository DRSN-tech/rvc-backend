package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/DRSN-tech/go-backend/internal/cfg"
	"github.com/DRSN-tech/go-backend/internal/proto"
	"github.com/DRSN-tech/go-backend/internal/usecase"
	"github.com/DRSN-tech/go-backend/pkg/logger"
	"google.golang.org/grpc"
)

type GRPCServer struct {
	server *grpc.Server
	cfg    *cfg.GRPCConfig
	logger logger.Logger
}

func NewGRPCServer(cfg *cfg.GRPCConfig, logger logger.Logger) *GRPCServer {
	return &GRPCServer{
		server: grpc.NewServer(),
		cfg:    cfg,
		logger: logger,
	}
}

func (s *GRPCServer) RegisterServices(prUC usecase.ProductUC, logger logger.Logger) {
	proto.RegisterProductServiceServer(s.server, NewProductService(prUC, logger))
}

func (s *GRPCServer) Start() error {
	addr := fmt.Sprintf(":%s", s.cfg.Port)
	lis, err := net.Listen(s.cfg.NetworkMode, addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	return s.server.Serve(lis)
}

func (s *GRPCServer) Stop(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Infof("gRPC server stopped gracefully")
		return nil
	case <-ctx.Done():
		s.server.Stop()
		s.logger.Warnf("gRPC server forced to stop after timeout")
		return ctx.Err()
	}
}
