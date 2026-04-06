package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/DRSN-tech/go-backend/internal/usecase"
	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/jimlawless/whereami"
	"github.com/shopspring/decimal"
)

type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type ProductMetadata struct {
	Name         string
	CategoryName string
	Price        int64
}

func NewErrorResponse(code int, message string) *ErrorResponse {
	return &ErrorResponse{
		Code:    code,
		Message: message,
	}
}

func ToHTTPResponse(err error) (int, string) {
	switch {
	case errors.Is(err, e.ErrStatusBadRequest):
		return http.StatusBadRequest, e.ErrStatusBadRequest.Error()
	case errors.Is(err, e.ErrExpectedMultipart):
		return http.StatusBadRequest, e.ErrExpectedMultipart.Error()
	case errors.Is(err, e.ErrMissingFields):
		return http.StatusBadRequest, e.ErrMissingFields.Error()
	case errors.Is(err, e.ErrInvalidPrice):
		return http.StatusBadRequest, e.ErrInvalidPrice.Error()
	case errors.Is(err, e.ErrPricePrecision):
		return http.StatusBadRequest, e.ErrPricePrecision.Error()
	case errors.Is(err, e.ErrTooManyImages):
		return http.StatusBadRequest, e.ErrTooManyImages.Error()
	case errors.Is(err, e.ErrNoImages):
		return http.StatusBadRequest, e.ErrNoImages.Error()
	case errors.Is(err, e.ErrFileTooLarge):
		return http.StatusBadRequest, e.ErrFileTooLarge.Error()
	case errors.Is(err, e.ErrInvalidImage):
		return http.StatusBadRequest, e.ErrInvalidImage.Error()
	case errors.Is(err, e.ErrNoChanges):
		return http.StatusBadRequest, e.ErrNoChanges.Error()
	case errors.Is(err, e.ErrUnsupportedMediaType):
		return http.StatusBadRequest, e.ErrUnsupportedMediaType.Error()
	default:
		return http.StatusInternalServerError, e.ErrInternalServerError.Error()
	}
}

func WriteError(w http.ResponseWriter, err error) {
	code, msg := ToHTTPResponse(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(NewErrorResponse(code, msg))
}

func WriteSuccess(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// parsePriceToCents converts a string like "599.99" or "600" to int64 cents.
// Returns error if:
// - invalid format
// - more than 2 decimal places
// - negative value
// - exceeds reasonable limit (e.g. 10^9 rubles)
func parsePriceToCents(s string) (int64, error) {
	if strings.TrimSpace(s) == "" {
		return 0, errors.New("price is empty")
	}

	d, err := decimal.NewFromString(s)
	if err != nil {
		return 0, e.ErrInvalidPrice
	}

	// Reject negative
	if d.LessThan(decimal.Zero) {
		return 0, e.ErrInvalidPrice
	}

	// Enforce max value (e.g. 1 billion rubles = 100_000_000_000 cents)
	maxPrice := decimal.NewFromInt(1_000_000_000).Mul(decimal.NewFromInt(100)) // 1B rub in cents
	if d.GreaterThan(maxPrice) {
		return 0, e.ErrInvalidPrice
	}

	// Check decimal places
	if d.Exponent() < -2 {
		return 0, e.ErrPricePrecision // "price must have at most 2 decimal places"
	}

	// Convert to cents: multiply by 100 and round
	cents := d.Mul(decimal.NewFromInt(100)).Round(0)

	// Safely convert to int64
	centsInt := cents.IntPart()
	if centsInt < 0 || centsInt > 9223372036854775807 { // int64 max, but we have maxPrice
		return 0, e.ErrInvalidPrice
	}

	return centsInt, nil
}

func ensureMultipartForm(r *http.Request, maxMemory int64) error {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		return e.Wrap(whereami.WhereAmI(), e.ErrExpectedMultipart)
	}
	return r.ParseMultipartForm(maxMemory)
}

func parseProductForm(r *http.Request) (*ProductMetadata, error) {
	name := r.FormValue("name")
	category_name := r.FormValue("category_name")
	priceStr := r.FormValue("price")

	if name == "" || category_name == "" || priceStr == "" {
		return nil, e.Wrap(fmt.Sprintf("name: %s, category_name: %s, price: %s\n", name, category_name, priceStr), e.ErrMissingFields)
	}

	priceCents, err := parsePriceToCents(priceStr)
	if err != nil {
		return nil, err
	}

	return &ProductMetadata{
		Name:         name,
		CategoryName: category_name,
		Price:        priceCents,
	}, nil
}

func parseImages(files []*multipart.FileHeader) ([]usecase.ProductImage, error) {
	const (
		maxImageCount = 10
		maxFileSize   = 15 << 20
	)

	if len(files) == 0 {
		return nil, e.ErrNoImages
	}

	if len(files) > maxImageCount {
		return nil, e.ErrTooManyImages
	}

	images := make([]usecase.ProductImage, 0, len(files))
	for _, fh := range files {
		data, mimeType, err := readFile(fh, maxFileSize)
		if err != nil {
			return nil, err
		}
		images = append(images, *usecase.NewProductImage(data, mimeType, int64(len(data)), fh.Filename))
	}

	return images, nil
}

func readFile(fh *multipart.FileHeader, maxSize int64) ([]byte, string, error) {
	src, err := fh.Open()
	if err != nil {
		return nil, "", e.ErrInternalServerError
	}
	defer src.Close()

	data, err := io.ReadAll(src)
	if err != nil {
		return nil, "", e.ErrInternalServerError
	}

	if int64(len(data)) > maxSize {
		return nil, "", e.Wrap(fh.Filename, e.ErrFileTooLarge)
	}

	if len(data) == 0 {
		return nil, "", e.Wrap(fh.Filename, e.ErrInvalidImage)
	}

	mimeType := http.DetectContentType(data[:min(len(data), 512)])
	if err := validateImageData(data, mimeType); err != nil {
		return nil, "", e.Wrap(fh.Filename, err)
	}

	return data, mimeType, nil
}

func validateImageData(data []byte, mimeType string) error {
	if !strings.HasPrefix(mimeType, "image/") {
		return e.ErrUnsupportedMediaType
	}

	cfg, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unknown format") {
			return e.ErrUnsupportedMediaType
		}
		return e.ErrInvalidImage
	}

	if format == "" || cfg.Width <= 0 || cfg.Height <= 0 {
		return e.ErrInvalidImage
	}

	return nil
}
