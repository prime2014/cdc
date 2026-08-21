package brokers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"streams"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MQTTBroker struct {
	client mqtt.Client
	topic  string // base topic, e.g. "cdc"
	qos    byte
}

func NewMQTTBroker(brokerURL, clientID, baseTopic string) (*MQTTBroker, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL) // e.g. "tcp://localhost:1883"
	opts.SetClientID(clientID)
	opts.SetAutoReconnect(true)
	opts.SetConnectTimeout(5 * time.Second)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("mqtt connect: %w", token.Error())
	}

	return &MQTTBroker{
		client: client,
		topic:  baseTopic,
		qos:    1, // at-least-once
	}, nil
}

func (m *MQTTBroker) Publish(ctx context.Context, event *streams.CDCEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	topic := fmt.Sprintf("%s/%s/%s", m.topic, event.Schema, event.Table)

	token := m.client.Publish(topic, m.qos, false, data)

	// Honour the context
	done := make(chan struct{})
	go func() {
		token.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return token.Error()
	}
}

func (m *MQTTBroker) Close() error {
	m.client.Disconnect(250) // ms
	return nil
}
