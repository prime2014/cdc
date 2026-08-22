package streams

import (
	"context"
	"log"
	"sync"
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
}

func NewEventBus(broker Broker, workers, bufferSize int) *EventBus {
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
	}

	// Start workers
	for i := 0; i < workers; i++ {
		b.wg.Add(1)
		go b.worker(i)
	}

	return b
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
			if err := b.broker.Publish(b.ctx, event); err != nil {
				log.Printf("worker %d: publish error: %v", id, err)
			}

			// Return the object to the pool
			b.putEvent(event)
		}
	}
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

// Non-blocking submit (used by the replication handler)
func (b *EventBus) Submit(event *CDCEvent) {
	select {
	case b.events <- event:
		// queued successfully
	case <-b.ctx.Done():
		b.putEvent(event)
	default:
		// channel full -decide what to do
		log.Println("event bus full dropping  event")
	}
}

func (b *EventBus) Close() {
	b.cancel()
	close(b.events)
	b.wg.Wait()
}
