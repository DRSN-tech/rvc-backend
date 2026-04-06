package http

import (
	_ "github.com/DRSN-tech/go-backend/docs" // Импорт сгенерированных файлов
	"github.com/DRSN-tech/go-backend/internal/infrastructure/metrics"
	"github.com/DRSN-tech/go-backend/internal/usecase"
	"github.com/DRSN-tech/go-backend/pkg/logger"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

type Router struct {
	router *chi.Mux
	logger logger.Logger
	prUC   usecase.ProductUC
}

func NewRouter(router *chi.Mux, logger logger.Logger, prUC usecase.ProductUC) *Router {
	return &Router{router: router, logger: logger, prUC: prUC}
}

func (r *Router) Init() {
	r.router.Use(middleware.Logger)    // Пишет логи запросов в консоль
	r.router.Use(middleware.Recoverer) // Не дает серверу упасть при панике
	r.router.Use(MetricsMiddleware)

	r.router.Get("/swagger/*", httpSwagger.Handler(
		httpSwagger.URL("http://localhost:8080/swagger/doc.json"), // ссылка на JSON
	))

	r.router.Route("/api/v1", func(v1 chi.Router) {
		prHandler := NewProductHandler(r.prUC, r.logger)
		registerProductRoutes(v1, prHandler)
	})
}

func registerProductRoutes(router chi.Router, prHandler *ProductHandler) {
	router.Route("/products", func(pr chi.Router) {
		pr.Post("/", prHandler.registerNewProduct)
	})
}

func InitMetricsRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Handle("/metrics", metrics.Handler())

	return r
}
