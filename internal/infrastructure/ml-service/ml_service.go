package ml_service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DRSN-tech/go-backend/internal/cfg"
	infra "github.com/DRSN-tech/go-backend/internal/infrastructure"
	"github.com/DRSN-tech/go-backend/internal/proto"
	"github.com/DRSN-tech/go-backend/internal/usecase"
	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/DRSN-tech/go-backend/pkg/jitter"
	"github.com/DRSN-tech/go-backend/pkg/logger"
	"github.com/jimlawless/whereami"
	"golang.org/x/sync/errgroup"
)

// MLService клиент для взаимодействия с внешним ML-сервисом
type MLService struct {
	client proto.MachineLearningServiceClient
	cfg    *cfg.MLServiceCfg
	logger logger.Logger
}

func NewMLService(client proto.MachineLearningServiceClient, cfg *cfg.MLServiceCfg, logger logger.Logger) *MLService {
	return &MLService{
		client: client,
		cfg:    cfg,
		logger: logger,
	}
}

// VectorizeRequest выполняет векторизацию изображений с retry-логикой и экспоненциальной задержкой
func (m *MLService) VectorizeRequest(ctx context.Context, req *usecase.VectorizeReq) ([]usecase.VectorizeRes, error) {
	const (
		baseJitter = 1 * time.Second
		maxJitter  = 30 * time.Second
	)

	for attempt := 0; attempt < m.cfg.MaxRetries; attempt++ {
		vectors, err := m.vectorizeBatch(ctx, req)
		if err == nil {
			return vectors, nil
		}

		if attempt == m.cfg.MaxRetries-1 {
			return nil, e.Wrap(whereami.WhereAmI(), fmt.Errorf("all %d attempts failed", m.cfg.MaxRetries))
		}

		sleepTime := jitter.ExponentialBackoff(
			baseJitter,
			maxJitter,
			attempt,
			jitter.DefaultJitter,
		)

		m.logger.Warnf("vectorization failed, retrying in %v (attempt %d)", sleepTime, attempt+1)
		select {
		case <-time.After(sleepTime):
		case <-ctx.Done():
			return nil, e.Wrap(whereami.WhereAmI(), ctx.Err())
		}
	}

	return nil, e.Wrap(whereami.WhereAmI(), fmt.Errorf("unreachable"))
}

// vectorizeBatch отправляет батч изображений на векторизацию параллельно с ограничением конкурентности
func (m *MLService) vectorizeBatch(ctx context.Context, req *usecase.VectorizeReq) ([]usecase.VectorizeRes, error) {
	const op = "MLService.vectorizeBatch"

	sem := make(chan struct{}, m.cfg.MaxConcurrent)
	var mu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	vectors := make([]usecase.VectorizeRes, len(req.Images))
	for i, image := range req.Images {
		g.Go(func() error {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-gctx.Done():
				return gctx.Err()
			}

			ext, err := infra.GetExtensionFromMIME(image.MimeType)
			if err != nil {
				m.logger.Warnf("%s: image: %s, error: %s", whereami.WhereAmI(), image.Name, err.Error())
				return err
			}

			protoReq := proto.VectorizeRequest{
				ImageData: image.Data,
				ImageType: infra.ConvertExtensionToProtoEnum(ext),
			}

			res, err := m.client.VectorizeImage(gctx, &protoReq)
			if err != nil {
				m.logger.Warnf("VectorizeImage failed: %v", err)
				return err
			}

			mu.Lock()
			vectors[i] = *usecase.NewVectorizeRes(res.Vector, res.ModelVersion)
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, e.Wrap(op, err)
	}

	return vectors, nil
}
