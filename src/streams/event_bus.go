package streams

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type EventBus struct {
	// 1. Interfaces & Function Pointers (16 bytes each)
	broker Broker             // 16 bytes (tab ptr + data ptr)
	ctx    context.Context    // 16 bytes (tab ptr + data ptr)
	cancel context.CancelFunc // 8 bytes (func ptr)

	// 2. Standard 64-bit Pointers & Channels (8 bytes each)
	events chan *CDCEvent // 8 bytes (pointer to hchan)

	// 3. 64-bit Primitive Integers (8 bytes each on 64-bit arch)
	workers int // 8 bytes

	// 4. Large Composite Structs (embed internal atomic/pointer fields)
	wg   sync.WaitGroup // 12 bytes + 4 bytes internal padding (16 bytes total)
	pool sync.Pool      // 56 bytes
	ring *Ring
}

func NewEventBus(broker Broker, workers, bufferSize int, ring *Ring) *EventBus {
	ctx, cancel := context.WithCancel(context.Background())

	b := &EventBus{
		events:  make(chan *CDCEvent, bufferSize),
		broker:  broker,
		ctx:     ctx,
		cancel:  cancel,
		workers: workers,
		pool: sync.Pool{
			New: func() any {
				return &CDCEvent{}
			},
		},
		ring: ring,
	}

	// Start workers
	for i := range workers {
		b.wg.Add(1)
		go b.worker(i)
	}

	return b
}

func (b *EventBus) QueueDepth() int {
	return len(b.events)
}

func (b *EventBus) QueueCapacity() int {
	return cap(b.events)
}

func (b *EventBus) QueueUtilization() float64 {
	c := cap(b.events)

	if c == 0 {
		return 0
	}

	return float64(len(b.events)) / float64(c)
}

func (b *EventBus) worker(id int) {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			return
		case event, ok := <-b.events:
			if !ok {
				return
			}

			// Publish to the broker
			if err := b.publishWithRetry(b.ctx, event); err != nil {
				log.Printf("worker %d: giving up: %v", id, err)
			}

			b.ring.Add(EventStat{
				Op:    string(event.Op),
				Table: event.Table,
				At:    time.Now(),
			})

			// Return the object to the pool
			b.putEvent(event)
		}
	}
}

func (b *EventBus) publishWithRetry(ctx context.Context, event *CDCEvent) error {
	const maxAttempts = 5
	backoff := 100 * time.Millisecond

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = b.broker.Publish(ctx, event)

		if lastErr == nil {
			return nil
		}

		log.Printf("publish attempt %d/%d failed: %v", attempt, maxAttempts, lastErr)

		if attempt == maxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2 // 100ms → 200ms → 400ms → 800ms ...
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
	}
	return fmt.Errorf("publish failed after %d attempts: %w", maxAttempts, lastErr)
}

func (b *EventBus) putEvent(e *CDCEvent) {
	e.ID = ""
	e.Table = ""
	e.Schema = ""
	e.Op = ""
	e.Timestamp = 0
	e.Before = nil
	e.After = nil

	b.pool.Put(e)
}

func (b *EventBus) getEvent() *CDCEvent {
	data := b.pool.Get().(*CDCEvent)
	return data
}

// blocking submit (used by the replication handler)
func (b *EventBus) Submit(ctx context.Context, event *CDCEvent) error {
	high := 0.80
	low := 0.50

	for cap(b.events) > 0 && float64(len(b.events))/float64(cap(b.events)) >= high {
		for float64(len(b.events))/float64(cap(b.events)) > low {
			select {
			case <-ctx.Done():
				b.putEvent(event)
				return ctx.Err()
			case <-b.ctx.Done():
				b.putEvent(event)
				return b.ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
		break
	}

	select {
	case b.events <- event:
		return nil
	case <-ctx.Done():
		b.putEvent(event)
		return ctx.Err()
	case <-b.ctx.Done():
		b.putEvent(event)
		return b.ctx.Err()
	}
}

func (b *EventBus) Close() {
	b.cancel()
	close(b.events)
	b.wg.Wait()
}
