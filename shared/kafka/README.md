# kafka

Thin wrapper around [`segmentio/kafka-go`](https://github.com/segmentio/kafka-go) providing a `Producer`, a `Consumer` with dead-letter queue (DLQ) support, and shared topic helpers.

## Usage

### Producer

```go
p := kafka.NewProducer([]string{"localhost:9092"}, "order.created")
defer p.Close()

type OrderCreated struct {
    OrderID string `json:"order_id"`
    UserID  string `json:"user_id"`
}

err := p.Publish(ctx, orderID, OrderCreated{
    OrderID: "abc-123",
    UserID:  "usr-456",
})
```

- One `Producer` per topic.
- `Publish` JSON-marshals the value before writing.
- Acks are required from **all** in-sync replicas (`RequireAll`).

### Consumer

```go
c := kafka.NewConsumer([]string{"localhost:9092"}, "order.created", "payment-service")
defer c.Close()

err := c.Consume(ctx, func(ctx context.Context, key string, value []byte) error {
    order, err := kafka.Unmarshal[OrderCreated](value)
    if err != nil {
        return err // routes to DLQ if configured
    }
    return processOrder(ctx, order)
})
```

`Consume` blocks until the context is cancelled or a non-recoverable error occurs. Returning `nil` from the context cancel is a clean exit.

### Dead-Letter Queue

```go
c := kafka.NewConsumer(brokers, "order.created", "payment-service").
    WithDLQ(brokers, kafka.DLQTopic("order.created")) // → "order.created.dlq"
defer c.Close()
```

| Scenario | Behaviour |
|---|---|
| Handler returns `nil` | Message committed |
| Handler returns error, DLQ configured | Message written to DLQ with error headers, then committed |
| Handler returns error, no DLQ | Error logged, message committed (avoids infinite retry) |
| DLQ write fails | Error returned, message **not** committed — consumption halts |

DLQ messages carry three extra headers:

- `dlq-error` — the handler error string
- `dlq-source-topic` — the originating topic
- `dlq-consumer-group` — the consumer group (service) that failed to process the message

### Topic Names

Define topic constants in each service. Use `DLQTopic` to derive the DLQ name:

```go
const TopicOrderCreated = "order.created"

dlq := kafka.DLQTopic(TopicOrderCreated) // "order.created.dlq"
```

### Unmarshal helper

```go
order, err := kafka.Unmarshal[OrderCreated](value)
```

Generic helper for deserializing message payloads inside a `Handler`.

## Configuration defaults

| Setting | Value |
|---|---|
| `MinBytes` | 10 KB |
| `MaxBytes` | 10 MB |
| `MaxWait` | 1 s |
| `WriteTimeout` | 10 s |
| `RequiredAcks` | `RequireAll` |

## Testing

```bash
go test ./kafka/...
```

All tests run without a live Kafka broker. Integration tests that require a broker should use build tags (e.g. `//go:build integration`) and a local broker via Docker Compose.
