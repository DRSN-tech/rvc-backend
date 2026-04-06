package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/DRSN-tech/go-backend/internal/cfg"
	"github.com/DRSN-tech/go-backend/internal/domain"
	drsnProto "github.com/DRSN-tech/go-backend/internal/proto"
	"github.com/DRSN-tech/go-backend/internal/usecase"
	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/DRSN-tech/go-backend/pkg/logger"
	"github.com/google/uuid"
	"github.com/jimlawless/whereami"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type Producer struct {
	writer *kafka.Writer
	logger logger.Logger
	cfg    *cfg.KafkaCfg
}

func NewProducer(logger logger.Logger, cfg *cfg.KafkaCfg) (*Producer, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		BatchSize:    10,
		BatchTimeout: 500 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				logger.Warnf("Kafka producer error: %s", err.Error())
			}
		},
	}

	return &Producer{
		writer: writer,
		logger: logger,
		cfg:    cfg,
	}, nil
}

func (p *Producer) WriteMessage(ctx context.Context, req *usecase.WriteMessageReq) error {
	value, err := p.GetPayloadBytes(req)
	if err != nil {
		return e.Wrap(whereami.WhereAmI(), err)
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.FormatInt(req.ProductID, 10)),
		Value: value,
	})
}

func (p *Producer) WriteRawMessage(ctx context.Context, req *usecase.WriteRawMessageReq) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.FormatInt(req.ProductID, 10)),
		Value: req.Payload,
	})
}

func (p *Producer) EnsureTopic(timeout time.Duration) error {
	conn, err := kafka.Dial(p.cfg.NetworkMode, p.cfg.Brokers[0])
	if err != nil {
		return e.Wrap(whereami.WhereAmI(), err)
	}
	defer func() {
		_ = conn.Close()
	}()

	partitions, err := conn.ReadPartitions(p.cfg.Topic)
	if err == nil && len(partitions) > 0 {
		return nil
	}

	done := make(chan error, 1)
	go func() {
		err := conn.CreateTopics(kafka.TopicConfig{
			Topic:             p.cfg.Topic,
			NumPartitions:     p.cfg.Partitions,
			ReplicationFactor: p.cfg.ReplicationFactor,
		})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			return e.Wrap(whereami.WhereAmI(), fmt.Errorf("failed to create topic %s: %w", p.cfg.Topic, err))
		}
		return nil
	case <-time.After(timeout):
		_ = conn.Close()
		return e.Wrap(whereami.WhereAmI(), fmt.Errorf("timeout: %v, topic: %s", timeout, p.cfg.Topic))
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) GetPayloadBytes(req *usecase.WriteMessageReq) ([]byte, error) {
	protoEmbeddings, err := toArrProtoEmbeddings(req.Embeddings)
	if err != nil {
		return nil, e.Wrap(whereami.WhereAmI(), err)
	}

	event := &drsnProto.ProductChangeEvent{
		EventId:        uuid.NewString(),
		EventTimestamp: time.Now().UnixNano(),
		Operation: &drsnProto.ProductChangeEvent_Upsert{
			Upsert: &drsnProto.UpsertEvent{
				ProductId:  req.ProductID,
				Embeddings: protoEmbeddings,
			},
		},
	}

	return proto.Marshal(event)
}

func toProtoEmbeddings(embedding domain.Embedding) (*drsnProto.Embedding, error) {
	const op = "producer.toProtoEmbedding"

	productID, ok := embedding.Payload["product_id"].(int64)
	if !ok {
		return nil, e.Wrap(op, fmt.Errorf("missing or invalid product_id in payload"))
	}

	imagePath, ok := embedding.Payload["image_path"].(string)
	if !ok {
		return nil, e.Wrap(op, fmt.Errorf("missing or invalid image_path in payload"))
	}

	createdAt, ok := embedding.Payload["created_at"].(int64)
	if !ok {
		return nil, e.Wrap(op, fmt.Errorf("missing or invalid created_at in payload"))
	}

	modelVersion, ok := embedding.Payload["model_version"].(string)
	if !ok {
		return nil, e.Wrap(op, fmt.Errorf("missing or invalid model_version in payload"))
	}

	return &drsnProto.Embedding{
		EmbeddingId: embedding.ID,
		Vector:      embedding.Vector,
		Metadata: &drsnProto.EmbeddingMetadata{
			ProductId:    productID,
			ImagePath:    imagePath,
			CreatedAt:    createdAt,
			ModelVersion: modelVersion,
		},
	}, nil
}

func toArrProtoEmbeddings(embeddings []domain.Embedding) ([]*drsnProto.Embedding, error) {
	result := make([]*drsnProto.Embedding, 0, len(embeddings))
	for _, embedding := range embeddings {
		emb, err := toProtoEmbeddings(embedding)
		if err != nil {
			return nil, err
		}

		result = append(result, emb)
	}

	return result, nil
}
