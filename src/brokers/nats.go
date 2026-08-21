package brokers

import (
	"context"
	"encoding/json"
	"fmt"
	"streams"

	"github.com/nats-io/nats.go"
)

type NATSBroker struct {
	nc      *nats.Conn
	subject string
}

func NewNATSBroker(url, subject string) (*NATSBroker, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("connect to nats: %w", err)
	}

	return &NATSBroker{
		nc:      nc,
		subject: subject,
	}, nil
}

func (n *NATSBroker) Publish(ctx context.Context, event *streams.CDCEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// You can make the subject dynamic if you want:
	subject := fmt.Sprintf("cdc.%s.%s", event.Schema, event.Table)

	if err := n.nc.Publish(subject, data); err != nil {
		return err
	}

	// Optional: wait until the server has received the message
	return n.nc.FlushWithContext(ctx)
}

func (n *NATSBroker) Close() error {
	n.nc.Close()
	return nil
}
