package http

import (
	"context"
	"net/http"

	"github.com/DRSN-tech/go-backend/internal/cfg"
)

type Server struct {
	httpServer *http.Server
}

func NewServer(handler http.Handler, cfg *cfg.HTTPConfig) *Server {
	return &Server{
		httpServer: &http.Server{
			Addr:         ":" + cfg.Port,
			Handler:      handler,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			IdleTimeout:  cfg.IdleTimeout,
		},
	}
}

func NewMetricsServer(handler http.Handler, cfg *cfg.MetricsConfig) *Server {
	return &Server{
		httpServer: &http.Server{
			Addr:    ":" + cfg.Port,
			Handler: handler,
		},
	}
}

func (s *Server) Run() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
