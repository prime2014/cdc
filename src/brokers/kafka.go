package brokers

import (
	"context"
	"encoding/json"
	"fmt"
	"streams"

	"github.com/segmentio/kafka-go"
)

type BrokerType string

const (
	Kafka    BrokerType = "kafka"
	NATS     BrokerType = "nats"
	RabbitMQ BrokerType = "rabbitmq"
	Redpanda BrokerType = "redpanda"
	Pulsar   BrokerType = "pulsar"
	MQTT     BrokerType = "mqtt"
	Log      BrokerType = "log"
)

type KafkaBroker struct {
	writer *kafka.Writer
	topic  string
}

func NewKafkaBroker(brokers []string, topic string) *KafkaBroker {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	return &KafkaBroker{
		writer: writer,
		topic:  topic,
	}
}

func NewRedpandaBroker(brokers []string, topic string) *KafkaBroker {
	return NewKafkaBroker(brokers, topic)
}

func (k *KafkaBroker) Publish(ctx context.Context, event *streams.CDCEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Use table name as key so related changes go to the same partition
	key := []byte(event.Schema + "." + event.Table)

	return k.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: data,
	})
}

func (k *KafkaBroker) Close() error {
	return k.writer.Close()
}
