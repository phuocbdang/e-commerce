package kafka

// DLQTopic returns the dead-letter queue topic name for a given topic.
// Convention: original topic name + ".dlq" suffix (e.g. "order.created.dlq").
func DLQTopic(topic string) string {
	return topic + ".dlq"
}
