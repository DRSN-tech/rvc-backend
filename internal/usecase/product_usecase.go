package usecase

import (
	"context"
	"strings"
	"time"

	"github.com/DRSN-tech/go-backend/internal/domain"
	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/DRSN-tech/go-backend/pkg/logger"
	transaction "github.com/avito-tech/go-transaction-manager/drivers/pgxv5/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ProductUseCase реализует бизнес-логику управления продуктами.
type ProductUseCase struct {
	productRepo   ProductRepository
	categoryRepo  CategoryRepository
	dbPool        transaction.Transactional
	mlService     MlServiceInfra
	imagesInfra   ImagesInfra
	embeddingRepo EmbeddingRepository
	logger        logger.Logger
	cacheRepo     CacheRepository
	producer      MessageProducer
	outboxRepo    OutboxRepository
}

func NewProductUC(
	productRepo ProductRepository,
	categoryRepo CategoryRepository,
	dbPool transaction.Transactional,
	mlService MlServiceInfra,
	imagesInfra ImagesInfra,
	embeddingRepo EmbeddingRepository,
	logger logger.Logger,
	cacheRepo CacheRepository,
	producer MessageProducer,
	outboxRepo OutboxRepository,
) *ProductUseCase {
	return &ProductUseCase{
		productRepo:   productRepo,
		categoryRepo:  categoryRepo,
		dbPool:        dbPool,
		mlService:     mlService,
		imagesInfra:   imagesInfra,
		embeddingRepo: embeddingRepo,
		logger:        logger,
		cacheRepo:     cacheRepo,
		producer:      producer,
		outboxRepo:    outboxRepo,
	}
}

// RegisterNewProduct обрабатывает добавление нового продукта с изображениями, категорией, векторами и сохранением в хранилища.
func (p *ProductUseCase) RegisterNewProduct(ctx context.Context, req *AddNewProductReq) (*OutboxEvent, error) {
	const op = "ProductUseCase.RegisterNewProduct"

	// Валидация данных
	var err error
	err = p.validateProduct(req)
	if err != nil {
		return nil, e.Wrap(op, err)
	}

	var (
		imagesRes  *UploadImagesRes
		uploaded   bool
		embeddings []domain.Embedding
	)

	ctx, tx, err := transaction.NewTransaction(ctx, pgx.TxOptions{}, p.dbPool)
	if err != nil {
		return nil, e.Wrap(op, err)
	}
	// Если произошла ошибка, происходит Rollback транзакции и очистка загруженных изображений
	defer func() {
		if err != nil {
			if tx.IsActive() {
				tx.Rollback(ctx)
			}

			if uploaded && imagesRes != nil {
				p.logger.Warnf(
					"Cleaning up orphaned images after transaction failure. product_name: %s, error: %v",
					req.Name,
					e.Wrap(op, err),
				)

				p.imagesInfra.CleanupImages(imagesRes.ImagesKeys)
			}

			if len(embeddings) > 0 {
				if err := p.embeddingRepo.Delete(ctx, embeddings); err != nil {
					p.logger.Warnf("Failed to cleunup Qdrant points %v", err)
				}
			}
		}
	}()
	ctx = context.WithValue(ctx, "tx", tx.Transaction())

	category, err := p.createCategory(ctx, req.CategoryName)
	if err != nil {
		return nil, e.Wrap(op, err)
	}

	upsertRes, err := p.upsertProduct(ctx, req.Name, req.Price, category.ID)
	if err != nil {
		return nil, e.Wrap(op, err)
	}

	if upsertRes.NoChanges {
		return nil, e.Wrap(op, e.ErrNoChanges)
	}

	if req.Images == nil {
		err = tx.Commit(ctx)
		if err != nil {
			return nil, e.Wrap(op, err)
		}

		// Удаляем из кэша ПОСЛЕ успешного коммита
		if err := p.cacheRepo.DeleteProducts(ctx, []int64{upsertRes.Product.ID}); err != nil {
			p.logger.Warnf("Failed to delete products from cache: %v", e.Wrap(op, err))
		}

		return nil, nil
	}
	// Отправка изображение на ML Service для получения векторов
	vectors, err := p.getVectors(ctx, req.Images)
	if err != nil {
		return nil, e.Wrap(op, err)
	}

	// Сохранение изображений в MinIO
	imagesRes, err = p.uploadImages(ctx, req.Name, req.Images)
	if err != nil {
		return nil, e.Wrap(op, err)
	}
	uploaded = true

	embeddings, err = p.getEmbeddings(upsertRes.Product.ID, imagesRes.ImagesKeys, vectors)
	if err != nil {
		return nil, e.Wrap(op, err)
	}

	// Сохранение векторов с дополнительной информацией (S3 key, Product ID, Created At, Model Version)
	if _, err := p.upsertEmbeddings(ctx, embeddings); err != nil {
		return nil, e.Wrap(op, err)
	}

	outboxPayload, err := p.producer.GetPayloadBytes(NewWriteMessageReq(upsertRes.Product.ID, embeddings))
	if err != nil {
		return nil, e.Wrap(op, err)
	}

	event, err := p.outboxRepo.Create(ctx, NewOutboxEvent(uuid.New(), upsertRes.Product.ID, ProductEvent, outboxPayload))
	if err != nil {
		return nil, e.Wrap(op, err)
	}

	// Коммит изменений в бд
	err = tx.Commit(ctx)
	if err != nil {
		return nil, e.Wrap(op, err)
	}

	// Удаление из кэша старых данных товара
	if err := p.cacheRepo.DeleteProducts(ctx, []int64{upsertRes.Product.ID}); err != nil {
		p.logger.Warnf("Failed to delete products: %v", e.Wrap(op, err))
	}

	return event, nil
}

// GetProductsInfo возвращает информацию о продуктах по их идентификаторам.
func (p *ProductUseCase) GetProductsInfo(ctx context.Context, req *GetProductsReq) (*GetProductsRes, error) {
	const op = "ProductUseCase.GetProductsInfo"

	// Валидация
	if len(req.IDs) == 0 {
		return nil, e.Wrap(op, e.ErrNoProducts)
	}

	// Поиск продуктов в хэше
	cacheProductsMap, err := p.cacheRepo.GetProducts(ctx, req.IDs)
	var (
		nonCacheable []int64
		cacheable    []ProductInfo
	)
	if err != nil {
		for _, productId := range req.IDs {
			nonCacheable = append(nonCacheable, productId)
		}
	} else {
		for _, productId := range req.IDs {
			if product, ok := cacheProductsMap[productId]; ok {
				cacheable = append(cacheable, product)
			} else {
				nonCacheable = append(nonCacheable, productId)
			}
		}
	}

	// Получение продуктов из БД
	var productsInfoFromDB []ProductInfo
	if len(nonCacheable) > 0 {
		productsInfoFromDB, err = p.getProductsInfo(ctx, nonCacheable)
		if err != nil {
			return nil, e.Wrap(op, err)
		}

		// Фоновое добавление продуктов в хэш
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			if err := p.cacheRepo.SetProducts(bgCtx, productsInfoFromDB); err != nil {
				p.logger.Warnf("Failed to cache products in background: %v", e.Wrap(op, err))
			}
		}()
	}

	dbProductsMap := make(map[int64]ProductInfo, len(productsInfoFromDB))
	for _, productInfo := range productsInfoFromDB {
		dbProductsMap[productInfo.ID] = productInfo
	}

	// Формирование результата
	result := make([]ProductInfo, 0, len(req.IDs))
	notFoundProducts := make([]int64, 0)
	for _, id := range req.IDs {
		if pr, ok := cacheProductsMap[id]; ok {
			result = append(result, pr)
		} else if pr, ok := dbProductsMap[id]; ok {
			result = append(result, pr)
		} else {
			notFoundProducts = append(notFoundProducts, id)
		}
	}

	return NewGetProductsRes(result, notFoundProducts), nil
}

// getProductsInfo делегирует запрос репозиторию продуктов.
func (p *ProductUseCase) getProductsInfo(ctx context.Context, ids []int64) ([]ProductInfo, error) {
	return p.productRepo.GetProductsInfo(ctx, ids)
}

// getVectors запрашивает векторные представления изображений у ML-сервиса.
func (p *ProductUseCase) getVectors(ctx context.Context, images []ProductImage) ([]VectorizeRes, error) {
	vectors, err := p.mlService.VectorizeRequest(ctx, NewVectorizeReq(images))
	if err != nil {
		return nil, err
	}

	if len(vectors) == 0 {
		return nil, e.ErrEmptyVectors
	}

	return vectors, nil
}

// upsertProduct идемпотентно создаёт или обновляет продукт.
func (p *ProductUseCase) upsertProduct(ctx context.Context, name string, price int64, categoryID int64) (*UpsertProductRes, error) {
	return p.productRepo.Upsert(ctx, domain.NewProduct(name, price, categoryID))
}

// createCategory идемпотентно создаёт категорию по имени продукта.
func (p *ProductUseCase) createCategory(ctx context.Context, categoryName string) (*domain.Category, error) {
	return p.categoryRepo.Create(ctx, domain.NewCategory(categoryName))
}

// uploadImages сохраняет изображения продукта в MinIO.
func (p *ProductUseCase) uploadImages(ctx context.Context, name string, images []ProductImage) (*UploadImagesRes, error) {
	return p.imagesInfra.UploadImages(ctx, NewUploadImagesReq(name, images))
}

// getEmbeddings генерирует []domain.Embedding
func (p *ProductUseCase) getEmbeddings(productID int64, imageKeys []string, vectors []VectorizeRes) ([]domain.Embedding, error) {
	if len(imageKeys) != len(vectors) {
		return nil, e.ErrImageVectorMismatch
	}

	embeddings := make([]domain.Embedding, 0, len(imageKeys))
	for i, key := range imageKeys {
		if len(vectors[i].Vector) == 0 {
			return nil, e.ErrVectorEmbeddingEmpty
		}
		payload := domain.NewPayload(productID, key, vectors[i].ModelVersion)
		embeddings = append(embeddings, *domain.NewEmbedding(uuid.NewString(), vectors[i].Vector, payload))
	}

	return embeddings, nil
}

func (p *ProductUseCase) upsertEmbeddings(ctx context.Context, embeddings []domain.Embedding) ([]domain.Embedding, error) {
	return p.embeddingRepo.Upsert(ctx, embeddings)
}

// validateProduct проверяет корректность входных данных запроса на добавление продукта.
func (p *ProductUseCase) validateProduct(req *AddNewProductReq) error {
	if strings.TrimSpace(req.Name) == "" {
		return e.ErrProductNameRequired
	}

	if req.Price <= 0 {
		return e.ErrPriceMustBePositive
	}

	if len(req.Images) == 0 {
		return e.ErrNoImages
	}

	for _, img := range req.Images {
		if len(img.Data) == 0 || img.Size <= 0 {
			return e.ErrInvalidImage
		}

		if img.Size != int64(len(img.Data)) {
			return e.ErrInvalidImage
		}

		if !strings.HasPrefix(img.MimeType, "image/") {
			return e.ErrUnsupportedMediaType
		}
	}

	return nil
}
