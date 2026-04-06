package http

import (
	"net/http"

	"github.com/DRSN-tech/go-backend/internal/usecase"
	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/DRSN-tech/go-backend/pkg/logger"
)

type ProductHandler struct {
	productUsecase usecase.ProductUC
	logger         logger.Logger
}

func NewProductHandler(productUsecase usecase.ProductUC, logger logger.Logger) *ProductHandler {
	return &ProductHandler{productUsecase: productUsecase, logger: logger}
}

// TODO: добавить логи
// registerNewProduct
//
//	@Summary		Регистрация нового товара
//	@Description	Создает новый товар в каталоге с изображениями
//	@Tags			products
//	@Accept			multipart/form-data
//	@Produce		json
//	@Param			name			formData	string					true	"Название товара"
//	@Param			category_name	formData	string					true	"Категория"
//	@Param			price			formData	number					true	"Цена"
//	@Param			images			formData	file					true	"Изображения товара"
//	@Success		201				{object}	map[string]interface{}	"Успешное создание"
//	@Failure		400				{object}	ErrorResponse	"Ошибка валидации"
//	@Router			/products [post]
func (p *ProductHandler) registerNewProduct(w http.ResponseWriter, r *http.Request) {
	const (
		maxTotalRequestSize = 150 << 20
		maxMemory           = 32 << 20
	)

	r.Body = http.MaxBytesReader(w, r.Body, maxTotalRequestSize)

	if err := ensureMultipartForm(r, maxMemory); err != nil {
		p.logger.Warnf("%d %s: %s", http.StatusBadRequest, e.ErrStatusBadRequest.Error(), r.Header.Get("Content-Type"))
		WriteError(w, err)
		return
	}

	prMeta, err := parseProductForm(r)
	if err != nil {
		p.logger.Warnf("%d %s: %s", http.StatusBadRequest, e.ErrStatusBadRequest.Error(), err.Error())
		WriteError(w, err)
		return
	}

	images, err := parseImages(r.MultipartForm.File["images"])
	if err != nil {
		p.logger.Warnf("%d %s: %s", http.StatusBadRequest, e.ErrStatusBadRequest.Error(), err.Error())
		WriteError(w, err)
		return
	}

	event, err := p.productUsecase.RegisterNewProduct(r.Context(), usecase.NewAddNewProductReq(prMeta.Name, prMeta.CategoryName, prMeta.Price, images))
	if err != nil {
		p.logger.Warnf("%s", err.Error())
		WriteError(w, err)
		return
	}

	if event != nil {
		WriteSuccess(w, http.StatusCreated, map[string]interface{}{
			"EventID": event.EventID,
		})
	} else {
		WriteSuccess(w, http.StatusOK, map[string]interface{}{
			"Changed": true,
		})
	}
}
