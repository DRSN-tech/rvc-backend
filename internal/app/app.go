package app

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	config "github.com/DRSN-tech/go-backend/internal/cfg"
	v1Grpc "github.com/DRSN-tech/go-backend/internal/delivery/v1/grpc"
	v1Http "github.com/DRSN-tech/go-backend/internal/delivery/v1/http"
	"github.com/DRSN-tech/go-backend/internal/infrastructure/kafka"
	minioInfra "github.com/DRSN-tech/go-backend/internal/infrastructure/minio"
	ml_service "github.com/DRSN-tech/go-backend/internal/infrastructure/ml-service"
	"github.com/DRSN-tech/go-backend/internal/proto"
	s3Repo "github.com/DRSN-tech/go-backend/internal/repository/minio"
	"github.com/DRSN-tech/go-backend/internal/repository/pgdb"
	pgdbConv "github.com/DRSN-tech/go-backend/internal/repository/pgdb/converter/generated"
	qdrantRepo "github.com/DRSN-tech/go-backend/internal/repository/qdrant"
	"github.com/DRSN-tech/go-backend/internal/repository/redis"
	redisConv "github.com/DRSN-tech/go-backend/internal/repository/redis/converter/generated"
	"github.com/DRSN-tech/go-backend/internal/usecase"
	"github.com/DRSN-tech/go-backend/pkg/clients"
	"github.com/DRSN-tech/go-backend/pkg/closer"
	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/DRSN-tech/go-backend/pkg/logger"
	"github.com/DRSN-tech/go-backend/pkg/postgres"
	"github.com/go-chi/chi/v5"
	"github.com/jimlawless/whereami"
	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// App представляет приложение со всеми зависимостями.
type App struct {
	logger logger.Logger
	closer *closer.Closer
	cfg    *config.Config

	// Clients
	db           *postgres.PgDatabase
	redisClient  *clients.RedisClient
	qdrantClient *clients.QdrantClient
	minioClient  *minio.Client
	grpcConn     *grpc.ClientConn
	producer     *kafka.Producer

	// Infrastructure
	imagesInfra  *minioInfra.MinioInfrastructure
	outboxWorker *kafka.OutboxWorker
	workerCancel context.CancelFunc

	// Servers
	httpSrv    *v1Http.Server
	metricsSrv *v1Http.Server
	grpcSrv    *v1Grpc.GRPCServer
}

// NewApp создает и инициализирует все компоненты приложения.
func NewApp(cfg *config.Config, log logger.Logger) (*App, error) {
	a := &App{
		logger: log,
		closer: closer.NewCloser(5 * time.Second),
		cfg:    cfg,
	}

	if err := a.initDatabase(); err != nil {
		return nil, err
	}
	if err := a.initMinio(); err != nil {
		return nil, err
	}
	if err := a.initQdrant(); err != nil {
		return nil, err
	}
	if err := a.initRedis(); err != nil {
		return nil, err
	}
	if err := a.initGRPCClient(); err != nil {
		return nil, err
	}
	if err := a.initKafka(); err != nil {
		return nil, err
	}
	if err := a.initServers(); err != nil {
		return nil, err
	}

	return a, nil
}

// Run запускает HTTP и gRPC серверы и ожидает сигнала завершения.
func (a *App) Run() error {
	grpcErrCh := make(chan error, 1)
	go func() {
		a.logger.Infof("gRPC server starting on %s:%s", a.cfg.Grpc.NetworkMode, a.cfg.Grpc.Port)
		if err := a.grpcSrv.Start(); err != nil {
			a.logger.Errorf(err, "gRPC server failed")
			grpcErrCh <- err
		}
	}()

	httpErrCh := make(chan error, 1)
	go func() {
		a.logger.Infof("HTTP server started on port %s", a.cfg.Http.Port)
		if err := a.httpSrv.Run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.Errorf(err, "HTTP server failed")
			httpErrCh <- err
		}
	}()

	metricsErrCh := make(chan error, 1)
	go func() {
		a.logger.Infof("Metrics server started on port %s", a.cfg.Metrics.Port)
		if err := a.metricsSrv.Run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.Errorf(err, "Metrics server failed")
			metricsErrCh <- err
		}
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	var appErr error
	select {
	case appErr = <-httpErrCh:
		a.logger.Errorf(appErr, "HTTP server fatal error")
	case appErr = <-grpcErrCh:
		a.logger.Errorf(appErr, "gRPC server fatal error")
	case <-shutdown:
		a.logger.Infof("Received shutdown signal, stopping gracefully...")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := a.Shutdown(shutdownCtx); err != nil {
		a.logger.Errorf(err, "shutdown completed with errors")
	} else {
		a.logger.Infof("Application shutdown complete")
	}

	return appErr
}

// Shutdown корректно останавливает все компоненты приложения.
func (a *App) Shutdown(ctx context.Context) error {
	return a.closer.Close(ctx)
}

func (a *App) initDatabase() error {
	db, err := postgres.Connect(a.cfg.Db)
	if err != nil {
		a.logger.Errorf(err, "failed to connect to database")
		return e.Wrap(whereami.WhereAmI(), err)
	}

	if err := db.RunMigrations(a.logger); err != nil {
		a.logger.Errorf(err, "failed to run migrations")
		return e.Wrap(whereami.WhereAmI(), err)
	}

	if err := db.Ping(); err != nil {
		a.logger.Errorf(err, "failed to ping database")
		return e.Wrap(whereami.WhereAmI(), err)
	}

	a.db = db
	a.closer.Add(func(ctx context.Context) error {
		a.db.Close()
		return nil
	})

	return nil
}

func (a *App) initMinio() error {
	client, err := clients.NewMinIOClient(a.cfg)
	if err != nil {
		a.logger.Errorf(err, "failed to initialize minio client")
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := clients.EnsureBucket(ctx, client, a.cfg.Minio.BucketName); err != nil {
		a.logger.Errorf(err, "failed to initialize MinIO bucket")
		return err
	}

	a.minioClient = client
	return nil
}

func (a *App) initQdrant() error {
	client, err := clients.NewQdrantClient(a.cfg.Qdrant)
	if err != nil {
		a.logger.Errorf(err, "failed to initialize qdrant")
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.EnsureCollection(ctx); err != nil {
		a.logger.Errorf(err, "failed to initialize qdrant collection")
		return err
	}

	a.qdrantClient = client
	a.closer.Add(func(ctx context.Context) error {
		return a.qdrantClient.Client.Close()
	})

	return nil
}

func (a *App) initRedis() error {
	client := clients.NewRedisClient(a.cfg.Redis)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		a.logger.Errorf(err, "failed to connect to redis")
		return err
	}

	a.redisClient = client
	a.closer.Add(func(ctx context.Context) error {
		return a.redisClient.Client.Close()
	})

	return nil
}

func (a *App) initGRPCClient() error {
	conn, err := grpc.NewClient(
		a.cfg.Ml.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		a.logger.Errorf(err, "failed to initialize grpc client")
		return err
	}

	a.grpcConn = conn
	a.closer.Add(func(ctx context.Context) error {
		return a.grpcConn.Close()
	})

	return nil
}

func (a *App) initKafka() error {
	producer, err := kafka.NewProducer(a.logger, a.cfg.Kafka)
	if err != nil {
		a.logger.Errorf(err, "failed to initialize kafka producer")
		return err
	}

	if err := producer.EnsureTopic(10 * time.Second); err != nil {
		a.logger.Errorf(err, "failed to ensure kafka topic")
		return err
	}

	a.producer = producer
	a.closer.Add(func(ctx context.Context) error {
		return a.producer.Close()
	})

	return nil
}

func (a *App) initServers() error {
	// Converters
	catConv := &pgdbConv.CategoryConverterImpl{}
	prConv := &pgdbConv.ProductConverterImpl{}
	infoConv := &redisConv.ProductInfoConverterImpl{}
	outboxConv := &pgdbConv.OutboxEventConverterImpl{}

	// Repositories
	productRepo := pgdb.NewProductRepo(a.db.Pool, prConv)
	categoryRepo := pgdb.NewCategoryRepo(a.db.Pool, catConv)
	outboxRepo := pgdb.NewOutboxEventRepo(a.db.Pool, outboxConv)
	imageRepo := s3Repo.NewImageRepo(a.minioClient, a.cfg.Minio)
	embRepo := qdrantRepo.NewEmbeddingRepo(a.qdrantClient.Client, a.cfg.Qdrant)
	cacheRepo := redis.NewCacheRepo(a.redisClient, infoConv, a.cfg.Redis, a.logger)

	// Infrastructure
	mlClient := proto.NewMachineLearningServiceClient(a.grpcConn)
	ml := ml_service.NewMLService(mlClient, a.cfg.Ml, a.logger)

	a.imagesInfra = minioInfra.NewMinioInfrastructure(imageRepo, a.cfg.Minio, a.logger)
	a.closer.Add(func(ctx context.Context) error {
		return a.imagesInfra.WaitForCleanup(ctx)
	})

	// Outbox worker
	a.outboxWorker = kafka.NewOutboxWorker(outboxRepo, a.logger, a.producer, a.db.Dsn)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	a.workerCancel = workerCancel
	a.outboxWorker.Start(workerCtx)
	a.logger.Infof("Outbox worker started")

	a.closer.Add(func(ctx context.Context) error {
		a.workerCancel()
		a.outboxWorker.Stop()
		return nil
	})

	// Use case
	productUC := usecase.NewProductUC(
		productRepo,
		categoryRepo,
		a.db.Pool,
		ml,
		a.imagesInfra,
		embRepo,
		a.logger,
		cacheRepo,
		a.producer,
		outboxRepo,
	)

	// gRPC Server
	a.grpcSrv = v1Grpc.NewGRPCServer(a.cfg.Grpc, a.logger)
	a.grpcSrv.RegisterServices(productUC, a.logger)
	a.closer.Add(func(ctx context.Context) error {
		return a.grpcSrv.Stop(ctx)
	})

	// HTTP Server
	r := chi.NewRouter()
	router := v1Http.NewRouter(r, a.logger, productUC)
	router.Init()
	a.httpSrv = v1Http.NewServer(r, a.cfg.Http)
	a.closer.Add(func(ctx context.Context) error {
		return a.httpSrv.Stop(ctx)
	})

	// Metrics
	metricsMux := v1Http.InitMetricsRouter()
	a.metricsSrv = v1Http.NewMetricsServer(metricsMux, a.cfg.Metrics)
	a.closer.Add(func(ctx context.Context) error {
		return a.metricsSrv.Stop(ctx)
	})

	return nil
}
