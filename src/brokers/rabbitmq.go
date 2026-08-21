package brokers

import (
	"context"
	"encoding/json"
	"fmt"

	"streams"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQBroker struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	exchange string
}

func NewRabbitMQBroker(url, exchange string) (*RabbitMQBroker, error) {
	conn, err := amqp.Dial(url)

	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}

	// Declare a topic exchange (good for CDC routing)
	err = ch.ExchangeDeclare(
		exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	return &RabbitMQBroker{
		conn:     conn,
		channel:  ch,
		exchange: exchange,
	}, nil
}

func (r *RabbitMQBroker) Publish(ctx context.Context, event *streams.CDCEvent) error {
	data, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	routingKey := fmt.Sprintf("cdc.%s.%s", event.Schema, event.Table)

	return r.channel.PublishWithContext(
		ctx,
		r.exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         data,
		},
	)
}

func (r *RabbitMQBroker) Close() error {
	if r.channel != nil {
		r.channel.Close()
	}

	if r.conn != nil {
		return r.conn.Close()
	}

	return nil
}
