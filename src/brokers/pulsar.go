package brokers

import (
	"context"
	"encoding/json"
	"fmt"
	"streams"

	"github.com/apache/pulsar-client-go/pulsar"
)

type PulsarBroker struct {
	client   pulsar.Client
	producer pulsar.Producer
}

func NewPulsarBroker(url, topic string) (*PulsarBroker, error) {
	client, err := pulsar.NewClient(
		pulsar.ClientOptions{
			URL: url,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("pulsar client: %w", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})

	if err != nil {
		client.Close()
		return nil, fmt.Errorf("pulsar producer: %w", err)
	}

	return &PulsarBroker{
		client:   client,
		producer: producer,
	}, nil
}

func (p *PulsarBroker) Publish(ctx context.Context, event *streams.CDCEvent) error {
	data, err := json.Marshal(&event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	_, err = p.producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: data,
		Key:     event.Schema + "." + event.Table, // optional partitioning key
	})

	return err
}

func (p *PulsarBroker) Close() error {
	p.producer.Close()
	p.client.Close()
	return nil
}
