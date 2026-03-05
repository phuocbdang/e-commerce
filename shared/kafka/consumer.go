package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Handler processes a single Kafka message. Return an error to route to DLQ.
type Handler func(ctx context.Context, key string, value []byte) error

// messageReader is the subset of kafka.Reader used by Consumer, allowing test mocks.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// messageWriter is the subset of kafka.Writer used for DLQ writes, allowing test mocks.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Consumer struct {
	reader    messageReader
	dlqWriter messageWriter
	groupID   string
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		groupID: groupID,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 10e3,
			MaxBytes: 10e6,
			MaxWait:  1 * time.Second,
		}),
	}
}

// WithDLQ configures a dead-letter queue topic for messages that fail processing.
// Call this before Consume. dlqTopic is typically kafka.DLQTopic(originalTopic).
func (c *Consumer) WithDLQ(brokers []string, dlqTopic string) *Consumer {
	c.dlqWriter = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        dlqTopic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireAll,
	}
	return c
}

// Consume blocks until ctx is cancelled or an unrecoverable error occurs.
// If a DLQ is configured, handler errors route the message there before committing.
// If no DLQ is configured, handler errors are logged and the message is committed
// (to avoid an infinite retry loop).
func (c *Consumer) Consume(ctx context.Context, handler Handler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // context cancelled — clean exit
			}
			return err
		}

		if err := handler(ctx, string(msg.Key), msg.Value); err != nil {
			if c.dlqWriter != nil {
				headers := make([]kafka.Header, len(msg.Headers), len(msg.Headers)+2)
				copy(headers, msg.Headers)
				headers = append(headers,
					kafka.Header{Key: "dlq-error", Value: []byte(err.Error())},
					kafka.Header{Key: "dlq-source-topic", Value: []byte(msg.Topic)},
					kafka.Header{Key: "dlq-consumer-group", Value: []byte(c.groupID)},
				)
				dlqMsg := kafka.Message{
					Key:     msg.Key,
					Value:   msg.Value,
					Headers: headers,
				}
				if dlqErr := c.dlqWriter.WriteMessages(ctx, dlqMsg); dlqErr != nil {
					// DLQ write failed — halt rather than silently lose the message.
					return fmt.Errorf("kafka: failed to write to DLQ: %w (original error: %v)", dlqErr, err)
				}
				log.Printf("kafka: handler error, message routed to DLQ: %v", err)
			} else {
				log.Printf("kafka: handler error, no DLQ configured (message committed): %v", err)
			}
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func (c *Consumer) Close() error {
	if c.dlqWriter != nil {
		if err := c.dlqWriter.Close(); err != nil {
			log.Printf("kafka: error closing DLQ writer: %v", err)
		}
	}
	return c.reader.Close()
}

// Unmarshal is a generic helper for deserializing message payloads.
func Unmarshal[T any](data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}
