package minio

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DRSN-tech/go-backend/internal/cfg"
	"github.com/DRSN-tech/go-backend/internal/domain"
	"github.com/DRSN-tech/go-backend/internal/infrastructure"
	"github.com/DRSN-tech/go-backend/internal/usecase"
	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/DRSN-tech/go-backend/pkg/logger"
	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"
)

// MinioInfrastructure управляет загрузкой и очисткой изображений в MinIO.
type MinioInfrastructure struct {
	minioRepo usecase.ImageRepository
	cfg       *cfg.MinIOCfg
	logger    logger.Logger
	wg        sync.WaitGroup
}

func NewMinioInfrastructure(minioRepo usecase.ImageRepository, cfg *cfg.MinIOCfg, logger logger.Logger) *MinioInfrastructure {
	return &MinioInfrastructure{
		minioRepo: minioRepo,
		cfg:       cfg,
		logger:    logger,
		wg:        sync.WaitGroup{},
	}
}

// UploadImages загружает изображения продукта в MinIO параллельно с ограничением одновременных операций.
// В случае ошибки отменяет остальные загрузки и запускает очистку уже загруженных файлов.
func (m *MinioInfrastructure) UploadImages(ctx context.Context, req *usecase.UploadImagesReq) (*usecase.UploadImagesRes, error) {
	const op = "MinioInfrastructure.UploadImages"
	sem := make(chan struct{}, m.cfg.UploadImagesLimit)

	g, gctx := errgroup.WithContext(ctx)
	var mu sync.Mutex
	keys := make([]string, len(req.Images))
	for i, image := range req.Images {
		g.Go(func() error {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case sem <- struct{}{}:
				defer func() { <-sem }()
			}

			imageID := uuid.NewString()
			ext, err := infrastructure.GetExtensionFromMIME(image.MimeType)
			if err != nil {
				return fmt.Errorf("invalid mime type %s for %s: %w", image.MimeType, image.Name, err)
			}
			objKey := fmt.Sprintf("%s/%s-%s.%s", req.Name, image.Name, imageID, ext)
			newImage := domain.NewImage(imageID, m.cfg.BucketName, objKey, image.Data, &image.Size, &image.MimeType)

			key, err := m.minioRepo.Upload(gctx, newImage)
			if err != nil {
				return fmt.Errorf("upload %s failed: %w", image.Name, err)
			}

			mu.Lock()
			keys[i] = key
			mu.Unlock()

			return nil
		})
	}

	ok := false
	defer func() {
		if !ok && len(keys) > 0 {
			var nonEmptyKeys []string
			for _, key := range keys {
				if key != "" {
					nonEmptyKeys = append(nonEmptyKeys, key)
				}
			}

			m.wg.Add(1)
			go m.cleanupUploadedKeys(nonEmptyKeys)
		}
	}()

	if err := g.Wait(); err != nil {
		return nil, e.Wrap(op, err)
	}

	ok = true
	return usecase.NewUploadImagesRes(keys), nil
}

// CleanupImages запускает фоновую очистку указанных ключей MinIO
func (m *MinioInfrastructure) CleanupImages(keys []string) {
	if len(keys) == 0 {
		return
	}
	m.wg.Add(1)
	go m.cleanupUploadedKeys(keys)
}

// cleanupUploadedKeys удаляет указанные объекты из MinIO с экспоненциальной задержкой и jitter.
func (m *MinioInfrastructure) cleanupUploadedKeys(keys []string) {
	defer m.wg.Done() // сигнализируем завершение компенсации
	const op = "MinioInfrastructure.cleanupUploadedKeys"
	m.logger.Infof("%s: Cleaning up uploaded keys", op)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, key := range keys {
		deleted := false
		backoff := time.Second
		for attempt := 0; attempt < 3; attempt++ {
			if err := m.minioRepo.Delete(ctx, key); err == nil {
				deleted = true
				break // Успешно удалено
			}

			if attempt < 2 {
				// Добавляем jitter для распределения нагрузки
				jitter := time.Duration(time.Now().UnixNano() % int64(time.Second))
				sleepTime := backoff + jitter

				time.Sleep(sleepTime)
				backoff *= 2
			}
		}

		if !deleted {
			m.logger.Warnf("%s: failed to delete key after 3 attempts: %s", op, key)
		}
	}

	m.logger.Infof("%s: cleanup finished", op)
}

// WaitForCleanup ожидает завершения всех фоновых задач очистки с учётом таймаута завершения приложения.
func (m *MinioInfrastructure) WaitForCleanup(shutdownTimeoutCtx context.Context) error {
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-shutdownTimeoutCtx.Done():
		return fmt.Errorf("minio cleanup timeout during shutdown: %w", shutdownTimeoutCtx.Err())
	}
}
