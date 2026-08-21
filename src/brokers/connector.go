package brokers

import (
	"context"

	"streams"
)

// Connector is a thin named wrapper around any Broker.
// It lets you add logging, metrics, or other cross-cutting behaviour
// without changing the concrete brokers.
type Connector struct {
	name   string
	broker Broker
}

// NewConnector creates a new Connector.
func NewConnector(name string, broker Broker) *Connector {
	return &Connector{
		name:   name,
		broker: broker,
	}
}

// Publish delegates to the underlying broker.
func (c *Connector) Publish(ctx context.Context, event *streams.CDCEvent) error {
	return c.broker.Publish(ctx, event)
}

// Close delegates to the underlying broker.
func (c *Connector) Close() error {
	return c.broker.Close()
}

// Name returns the friendly name of this connector.
func (c *Connector) Name() string {
	return c.name
}
