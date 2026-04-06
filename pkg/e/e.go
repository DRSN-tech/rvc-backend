package e

import (
	"fmt"
)

var (
	// 500 Internal Server Error
	ErrInternalServerError  = fmt.Errorf("internal server error")
	ErrIncorrectEnvVariable = fmt.Errorf("incorrect environment variable")

	// Транзакции
	ErrTransactionNotFound = fmt.Errorf("transaction not found")

	// Векторы
	ErrEmptyVectors         = fmt.Errorf("empty vectors")
	ErrVectorEmbeddingEmpty = fmt.Errorf("vector embedding is empty")
	ErrImageVectorMismatch  = fmt.Errorf("image vector mismatch")

	// 400 Bad Request
	ErrProductNameRequired  = fmt.Errorf("product name is required")
	ErrPriceMustBePositive  = fmt.Errorf("price must be positive")
	ErrNoImages             = fmt.Errorf("no images provided")
	ErrInvalidImage         = fmt.Errorf("invalid or corrupted image")
	ErrUnsupportedMediaType = fmt.Errorf("unsupported media type")
	ErrNoProducts           = fmt.Errorf("no products provided")
	ErrStatusBadRequest     = fmt.Errorf("status bad request")
	ErrExpectedMultipart    = fmt.Errorf("expected multipart/form-data")
	ErrMissingFields        = fmt.Errorf("missing fields")
	ErrInvalidPrice         = fmt.Errorf("invalid price")
	ErrPricePrecision       = fmt.Errorf("price must have at most 2 decimal places")
	ErrTooManyImages        = fmt.Errorf("too many images (max 10)")
	ErrFileTooLarge         = fmt.Errorf("file too large")
	ErrNoChanges            = fmt.Errorf("no changes")
)

// Wrap оборачивает ошибку
func Wrap(msg string, err error) error {
	return fmt.Errorf("%s: %w", msg, err)
}
