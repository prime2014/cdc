package brokers

import (
	"context"
	"encoding/json"
	"log"

	"streams"
)

// LogBroker just prints events to the standard logger.
// Useful for development and testing without a real message broker.
type LogBroker struct{}

func NewLogBroker() *LogBroker {
	return &LogBroker{}
}

func (l *LogBroker) Publish(ctx context.Context, event *streams.CDCEvent) error {
	data, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return err
	}

	log.Printf("[LogBroker] %s.%s %s\n%s",
		event.Schema,
		event.Table,
		event.Op,
		string(data),
	)

	return nil
}

func (l *LogBroker) Close() error {
	return nil
}
