package brokers

import (
	"config"
	"context"
	"fmt"
	"os"
	"streams"
	"strings"
)

type Broker interface {
	Publish(ctx context.Context, event *streams.CDCEvent) error
	Close() error
}

func NewFromConfig(cfg config.BrokerConfig) (Broker, error) {
	t := BrokerType(strings.ToLower(cfg.Type))
	opts := cfg.Options

	switch t {
	case Kafka:
		brokers := splitAndTrim(opts["brokers"])
		topic := opts["topic"]
		if len(brokers) == 0 || topic == "" {
			return nil, fmt.Errorf("kafka: 'brokers' and 'topic' are required")
		}
		return NewKafkaBroker(brokers, topic), nil

	case Redpanda:
		brokers := splitAndTrim(opts["brokers"])
		topic := opts["topic"]
		if len(brokers) == 0 || topic == "" {
			return nil, fmt.Errorf("redpanda: 'brokers' and 'topic' are required")
		}
		return NewRedpandaBroker(brokers, topic), nil

	case NATS:
		url := opts["url"]
		subject := opts["subject"]
		if url == "" || subject == "" {
			return nil, fmt.Errorf("nats: 'url' and 'subject' are required")
		}
		return NewNATSBroker(url, subject)

	case RabbitMQ:
		url := opts["url"]
		exchange := opts["exchange"]
		if url == "" || exchange == "" {
			return nil, fmt.Errorf("rabbitmq: 'url' and 'exchange' are required")
		}
		return NewRabbitMQBroker(url, exchange)

	case MQTT:
		brokerURL := opts["url"]
		clientID := opts["client_id"]
		baseTopic := opts["topic"]
		if brokerURL == "" || clientID == "" || baseTopic == "" {
			return nil, fmt.Errorf("mqtt: 'url', 'client_id' and 'topic' are required")
		}
		return NewMQTTBroker(brokerURL, clientID, baseTopic)

	case Pulsar:
		url := opts["url"]
		topic := opts["topic"]
		if url == "" || topic == "" {
			return nil, fmt.Errorf("pulsar: 'url' and 'topic' are required")
		}
		return NewPulsarBroker(url, topic)

	case Log:
		return &LogBroker{}, nil

	default:
		return nil, fmt.Errorf("unknown broker type: %s", cfg.Type)
	}
}

// small helper
func splitAndTrim(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func NewFromEnv() (Broker, error) {
	t := BrokerType(strings.ToLower(os.Getenv("BROKER_TYPE")))

	fmt.Println(t)

	switch t {
	case Kafka, Redpanda:
		brokers := splitAndTrim(os.Getenv("KAFKA_BROKERS"))
		topic := os.Getenv("KAFKA_TOPIC")
		if len(brokers) == 0 || topic == "" {
			return nil, fmt.Errorf("%s: KAFKA_BROKERS and KAFKA_TOPIC required", t)
		}
		return NewKafkaBroker(brokers, topic), nil

	case NATS:
		return NewNATSBroker(os.Getenv("NATS_URL"), os.Getenv("NATS_SUBJECT"))

	case RabbitMQ:
		return NewRabbitMQBroker(os.Getenv("RABBITMQ_URL"), os.Getenv("RABBITMQ_EXCHANGE"))

	case MQTT:
		return NewMQTTBroker(
			os.Getenv("MQTT_URL"),
			os.Getenv("MQTT_CLIENT_ID"),
			os.Getenv("MQTT_TOPIC"),
		)

	case Pulsar:
		return NewPulsarBroker(os.Getenv("PULSAR_URL"), os.Getenv("PULSAR_TOPIC"))

	case Log:
		return &LogBroker{}, nil

	default:
		return nil, fmt.Errorf("unknown BROKER_TYPE: %s", t)
	}
}
