package http

import (
	"net/http"
	"strconv"
	"time"

	"github.com/DRSN-tech/go-backend/internal/infrastructure/metrics"
)

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		dur := time.Since(start)

		metrics.HttpRequestsTotal.WithLabelValues(
			r.Method,
			r.RequestURI,
			strconv.Itoa(wrapped.status),
		).Inc()

		metrics.HttpRequestDuration.WithLabelValues(
			r.Method,
			r.RequestURI,
		).Observe(dur.Seconds())
	})
}
