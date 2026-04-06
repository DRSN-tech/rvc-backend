package pgdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/DRSN-tech/go-backend/internal/repository/pgdb/converter"
	"github.com/DRSN-tech/go-backend/internal/usecase"
	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/DRSN-tech/go-backend/pkg/logger"
	"github.com/DRSN-tech/go-backend/pkg/tr"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jimlawless/whereami"
)

type OutboxEventRepo struct {
	pool   *pgxpool.Pool
	conv   converter.OutboxEventConverter
	logger logger.Logger
}

func NewOutboxEventRepo(pool *pgxpool.Pool, conv converter.OutboxEventConverter, logger logger.Logger) *OutboxEventRepo {
	return &OutboxEventRepo{
		pool:   pool,
		conv:   conv,
		logger: logger,
	}
}

func (o *OutboxEventRepo) Create(ctx context.Context, event *usecase.OutboxEvent) (*usecase.OutboxEvent, error) {
	tx, err := tr.TxFromCtx(ctx)
	if err != nil {
		return nil, e.Wrap(whereami.WhereAmI(), err)
	}

	model := o.conv.ToModel(event)
	query := `
		INSERT INTO outbox_events (
			event_id,
			event_type,
			product_id,
			payload,
			status,
			created_at
		) VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, created_at;
	`

	if err := tx.QueryRow(ctx, query,
		model.EventID,
		model.EventType,
		model.ProductID,
		model.Payload,
		model.Status,
		model.CreatedAt,
	).Scan(&model.ID, &model.CreatedAt); err != nil {
		if postgresDuplicate(err) {
			return nil, fmt.Errorf("%s: event with id %s already exists", whereami.WhereAmI(), event.EventID)
		}

		return nil, fmt.Errorf("%s: failed to insert event: %w", whereami.WhereAmI(), err)
	}

	_, err = tx.Exec(ctx, "NOTIFY outbox_pending;")
	if err != nil {
		return nil, e.Wrap(whereami.WhereAmI(), err)
	}

	return o.conv.ToEntity(model), nil
}

func (o *OutboxEventRepo) GetAndMarkAsProcessing(ctx context.Context, limit int) ([]*usecase.OutboxEvent, error) {
	tx, err := o.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to begin transaction: %w", whereami.WhereAmI(), err)
	}
	defer func() {
		if err != nil {
			if err := tx.Rollback(ctx); err != nil && !errors.Is(err, sql.ErrTxDone) {
				o.logger.Errorf(err, "rollback failed")
			}
		}
	}()

	query := `
		UPDATE outbox_events
        SET status = $1, processing_started_at = now()
        WHERE id IN (
            SELECT id FROM outbox_events
            WHERE status = $2
            ORDER BY created_at
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, event_id, event_type, product_id, payload, status, created_at, processed_at
	`

	rows, err := tx.Query(ctx, query, usecase.Processing, usecase.Pending, limit)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to query pending events: %w", whereami.WhereAmI(), err)
	}
	defer rows.Close()

	var models []*converter.OutboxEventModel
	for rows.Next() {
		var model converter.OutboxEventModel
		var processedAt sql.NullTime

		err := rows.Scan(
			&model.ID,
			&model.EventID,
			&model.EventType,
			&model.ProductID,
			&model.Payload,
			&model.Status,
			&model.CreatedAt,
			&processedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to scan event: %w", whereami.WhereAmI(), err)
		}

		if processedAt.Valid {
			model.ProcessedAt = &processedAt.Time
		}

		models = append(models, &model)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: rows iterator error: %w", whereami.WhereAmI(), err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("%s: failed to commit transaction: %w", whereami.WhereAmI(), err)
	}

	return o.conv.ToArrEntity(models), nil
}

func (o *OutboxEventRepo) MarkAsProcessed(ctx context.Context, id int64) error {
	query := `
		UPDATE outbox_events
		SET status = $1, processed_at = NOW()
		WHERE id = $2 AND status = $3
	`

	result, err := o.pool.Exec(ctx, query, usecase.Processed, id, usecase.Processing)
	if err != nil {
		return fmt.Errorf("%s: failed to mark event %d as processed: %w", whereami.WhereAmI(), id, err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		// Событие уже было обработано другим worker'ом или не существует
		return nil
	}

	return nil
}
