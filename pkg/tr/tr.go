package tr

import (
	"context"

	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/jackc/pgx/v5"
)

type contextKey string

const TxKey contextKey = "tx"

// TxFromCtx извлекает объект транзакции (pgx.Tx) из контекста
func TxFromCtx(ctx context.Context) (pgx.Tx, error) {
	txAny := ctx.Value("tx")
	tx, ok := txAny.(pgx.Tx)
	if !ok {
		return nil, e.ErrTransactionNotFound
	}
	return tx, nil
}
