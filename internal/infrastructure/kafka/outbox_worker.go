package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/DRSN-tech/go-backend/internal/usecase"
	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/DRSN-tech/go-backend/pkg/logger"
	"github.com/jackc/pgx/v5"
)

type OutboxWorker struct {
	repo      usecase.OutboxRepository
	logger    logger.Logger
	producer  usecase.MessageProducer
	stop      chan struct{}
	wg        sync.WaitGroup
	dbConnStr string
}

func NewOutboxWorker(
	repo usecase.OutboxRepository,
	logger logger.Logger,
	producer usecase.MessageProducer,
	dbConnStr string,
) *OutboxWorker {
	return &OutboxWorker{
		repo:      repo,
		logger:    logger,
		producer:  producer,
		stop:      make(chan struct{}),
		dbConnStr: dbConnStr,
	}
}

func (w *OutboxWorker) Start(ctx context.Context) {
	w.wg.Add(2)
	go func() {
		defer w.wg.Done()
		w.run(ctx)
	}()

	// Запускаем слушатель уведомлений
	go func() {
		defer w.wg.Done()
		w.listenOutboxNotifications(ctx)
	}()
}

func (w *OutboxWorker) Stop() {
	close(w.stop)
	w.wg.Wait()
}

func (w *OutboxWorker) run(ctx context.Context) {
	// Обрабатываем "остатки" при старте
	w.logger.Infof("Draining pending outbox events on startup...")
	for {
		hasMore, err := w.processBatch(ctx)
		if err != nil {
			w.logger.Warnf("startup batch failed: %v", err)
			return
		}
		if !hasMore {
			break
		}
	}

	<-ctx.Done()
	w.logger.Infof("Worker stopped by context cancellation")
}

func (w *OutboxWorker) listenOutboxNotifications(ctx context.Context) {
	var conn *pgx.Conn
	var err error

	connect := func() error {
		conn, err = pgx.Connect(ctx, w.dbConnStr)
		if err != nil {
			return e.Wrap("failed to connect for LISTEN", err)
		}

		_, err = conn.Exec(ctx, "LISTEN outbox_pending")
		if err != nil {
			if err := conn.Close(ctx); err != nil {
				w.logger.Warnf("Failed to close connection: %v", err)
			}

			return e.Wrap("failed to LISTEN", err)
		}

		w.logger.Infof("Subscribed to 'outbox_pending' channel")
		return nil
	}

	if err := connect(); err != nil {
		w.logger.Warnf("Initial connect failed: %v", err)
		return
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			w.logger.Warnf("Failed to close connection: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stop:
			return
		default:
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
			notif, err := conn.WaitForNotification(ctxWithTimeout)
			cancel()

			if err != nil {
				if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
					continue
				}
				w.logger.Warnf("Connection lost: %v. Reconnecting...", err)
				if err := conn.Close(ctx); err != nil {
					w.logger.Warnf("Failed to close connection: %v", err)
				}

				time.Sleep(2 * time.Second)
				if err := connect(); err != nil {
					w.logger.Warnf("Reconnect failed: %v", err)
					time.Sleep(5 * time.Second)
				}
				continue
			}

			if notif != nil && notif.Channel == "outbox_pending" {
				w.logger.Debugf("Received outbox notification, draining outbox events")
				for {
					hasMore, err := w.processBatch(ctx)
					if err != nil {
						w.logger.Warnf("Batch processing failed: %v", err)
						break
					}
					if !hasMore {
						break
					}
				}
			}
		}
	}
}

func (w *OutboxWorker) processBatch(ctx context.Context) (bool, error) {
	events, err := w.repo.GetAndMarkAsProcessing(ctx, 10)
	if err != nil {
		return false, err
	}

	if len(events) == 0 {
		return false, nil
	}

	for _, event := range events {
		if err := w.processEvent(ctx, event); err != nil {
			continue
		}
		if err := w.repo.MarkAsProcessed(ctx, event.ID); err != nil {
			w.logger.Warnf("mark processed failed: %v", err)
		}
	}

	return true, nil
}

func (w *OutboxWorker) processEvent(ctx context.Context, event *usecase.OutboxEvent) error {
	if err := w.SendBytes(ctx, event.ProductID, event.Payload); err != nil {
		// Добавляем retry логику для временных ошибок
		if isRetryableError(err) {
			return e.Wrap("Temporary Kafka failure, will retry", err)
		}
		return e.Wrap("Permanent Kafka failure", err)
	}
	return nil
}

func (w *OutboxWorker) SendBytes(ctx context.Context, productID int64, payload []byte) error {
	return w.producer.WriteRawMessage(ctx, usecase.NewWriteRawMessageReq(productID, payload))
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	retryablePhrases := []string{
		"connection refused",
		"i/o timeout",
		"network is unreachable",
		"broker not available",
		"connection reset",
		"broken pipe",
		"no such host",
	}
	for _, phrase := range retryablePhrases {
		if strings.Contains(errStr, phrase) {
			return true
		}
	}
	return false
}
