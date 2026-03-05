package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

// --- DLQTopic ---

func TestDLQTopic(t *testing.T) {
	tests := []struct {
		topic string
		want  string
	}{
		{"order.created", "order.created.dlq"},
		{"payment.processed", "payment.processed.dlq"},
		{"", ".dlq"},
	}
	for _, tt := range tests {
		if got := DLQTopic(tt.topic); got != tt.want {
			t.Errorf("DLQTopic(%q) = %q, want %q", tt.topic, got, tt.want)
		}
	}
}

// --- Unmarshal ---

func TestUnmarshal_Success(t *testing.T) {
	type payload struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
	}

	data := []byte(`{"order_id":"abc-123","amount":99}`)
	got, err := Unmarshal[payload](data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.OrderID != "abc-123" || got.Amount != 99 {
		t.Errorf("unexpected result: %+v", got)
	}
}

func TestUnmarshal_InvalidJSON(t *testing.T) {
	_, err := Unmarshal[map[string]any]([]byte(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestUnmarshal_EmptyBytes(t *testing.T) {
	_, err := Unmarshal[map[string]any]([]byte(``))
	if err == nil {
		t.Fatal("expected error for empty input, got nil")
	}
}

// --- mock reader / writer ---

type mockReader struct {
	messages []kafka.Message
	fetchErr error
	cursor   int
	closed   bool
	committed []kafka.Message
}

func (m *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.fetchErr != nil {
		return kafka.Message{}, m.fetchErr
	}
	if m.cursor >= len(m.messages) {
		// block until context cancelled
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	msg := m.messages[m.cursor]
	m.cursor++
	return msg, nil
}

func (m *mockReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	m.committed = append(m.committed, msgs...)
	return nil
}

func (m *mockReader) Close() error {
	m.closed = true
	return nil
}

type mockWriter struct {
	written []kafka.Message
	writeErr error
	closed  bool
}

func (m *mockWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.written = append(m.written, msgs...)
	return nil
}

func (m *mockWriter) Close() error {
	m.closed = true
	return nil
}

// --- Consume ---

func TestConsume_HandlerSuccess(t *testing.T) {
	reader := &mockReader{
		messages: []kafka.Message{
			{Key: []byte("k1"), Value: []byte(`"hello"`)},
		},
	}
	c := &Consumer{reader: reader}

	var received []string
	ctx, cancel := context.WithCancel(context.Background())

	err := c.Consume(ctx, func(ctx context.Context, key string, value []byte) error {
		received = append(received, key)
		cancel() // stop after first message
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(received) != 1 || received[0] != "k1" {
		t.Errorf("expected [k1], got %v", received)
	}
	if len(reader.committed) != 1 {
		t.Errorf("expected 1 committed message, got %d", len(reader.committed))
	}
}

func TestConsume_HandlerError_NoDLQ_Commits(t *testing.T) {
	reader := &mockReader{
		messages: []kafka.Message{
			{Key: []byte("k1"), Value: []byte(`"data"`)},
		},
	}
	c := &Consumer{reader: reader}

	ctx, cancel := context.WithCancel(context.Background())
	err := c.Consume(ctx, func(ctx context.Context, key string, value []byte) error {
		cancel()
		return errors.New("processing failed")
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// message must still be committed to avoid infinite retry
	if len(reader.committed) != 1 {
		t.Errorf("expected message to be committed even on handler error, got %d commits", len(reader.committed))
	}
}

func TestConsume_HandlerError_WithDLQ_RoutesToDLQ(t *testing.T) {
	reader := &mockReader{
		messages: []kafka.Message{
			{Key: []byte("k1"), Value: []byte(`"data"`), Topic: "order.created"},
		},
	}
	dlq := &mockWriter{}
	c := &Consumer{reader: reader, dlqWriter: dlq, groupID: "payment-service"}

	ctx, cancel := context.WithCancel(context.Background())
	err := c.Consume(ctx, func(ctx context.Context, key string, value []byte) error {
		cancel()
		return errors.New("processing failed")
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(dlq.written) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(dlq.written))
	}

	// verify DLQ headers carry error info, source topic, and consumer group
	headers := map[string]string{}
	for _, h := range dlq.written[0].Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["dlq-error"] == "" {
		t.Error("DLQ message missing dlq-error header")
	}
	if headers["dlq-source-topic"] != "order.created" {
		t.Errorf("dlq-source-topic = %q, want %q", headers["dlq-source-topic"], "order.created")
	}
	if headers["dlq-consumer-group"] != "payment-service" {
		t.Errorf("dlq-consumer-group = %q, want %q", headers["dlq-consumer-group"], "payment-service")
	}

	// original message must be committed after DLQ write
	if len(reader.committed) != 1 {
		t.Errorf("expected message committed after DLQ write, got %d", len(reader.committed))
	}
}

func TestConsume_DLQWriteFailure_Halts(t *testing.T) {
	reader := &mockReader{
		messages: []kafka.Message{
			{Key: []byte("k1"), Value: []byte(`"data"`)},
		},
	}
	dlq := &mockWriter{writeErr: errors.New("broker unavailable")}
	c := &Consumer{reader: reader, dlqWriter: dlq}

	ctx := context.Background()
	err := c.Consume(ctx, func(_ context.Context, _ string, _ []byte) error {
		return errors.New("processing failed")
	})

	if err == nil {
		t.Fatal("expected error when DLQ write fails, got nil")
	}
	// message must NOT be committed when DLQ write fails
	if len(reader.committed) != 0 {
		t.Errorf("message must not be committed on DLQ write failure, got %d commits", len(reader.committed))
	}
}

func TestConsume_FetchError_NonContext_Propagates(t *testing.T) {
	fetchErr := errors.New("broker connection lost")
	reader := &mockReader{fetchErr: fetchErr}
	c := &Consumer{reader: reader}

	err := c.Consume(context.Background(), func(_ context.Context, _ string, _ []byte) error {
		return nil
	})

	if !errors.Is(err, fetchErr) {
		t.Errorf("expected fetch error to propagate, got %v", err)
	}
}

func TestConsume_ContextCancelled_ReturnsNil(t *testing.T) {
	reader := &mockReader{} // no messages — will block on FetchMessage
	c := &Consumer{reader: reader}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := c.Consume(ctx, func(_ context.Context, _ string, _ []byte) error {
		return nil
	})

	if err != nil {
		t.Errorf("expected nil on context cancellation, got %v", err)
	}
}

// --- DLQ header isolation (no mutation of original slice) ---

func TestConsume_DLQHeaders_DoNotMutateOriginal(t *testing.T) {
	original := []kafka.Header{{Key: "x-trace", Value: []byte("trace-id")}}
	reader := &mockReader{
		messages: []kafka.Message{
			{Key: []byte("k"), Value: []byte(`"v"`), Headers: original},
		},
	}
	dlq := &mockWriter{}
	c := &Consumer{reader: reader, dlqWriter: dlq}

	ctx, cancel := context.WithCancel(context.Background())
	c.Consume(ctx, func(_ context.Context, _ string, _ []byte) error { //nolint:errcheck
		cancel()
		return errors.New("fail")
	})

	if len(original) != 1 {
		t.Errorf("original headers slice was mutated: len=%d", len(original))
	}
}

// --- Close ---

func TestConsumer_Close_ClosesDLQWriter(t *testing.T) {
	reader := &mockReader{}
	dlq := &mockWriter{}
	c := &Consumer{reader: reader, dlqWriter: dlq}

	if err := c.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reader.closed {
		t.Error("reader was not closed")
	}
	if !dlq.closed {
		t.Error("DLQ writer was not closed")
	}
}

func TestConsumer_Close_NoDLQ(t *testing.T) {
	reader := &mockReader{}
	c := &Consumer{reader: reader}

	if err := c.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reader.closed {
		t.Error("reader was not closed")
	}
}
