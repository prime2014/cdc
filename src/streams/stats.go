package streams

import (
	"sync"
	"time"
)

type EventStat struct {
	Op    string
	Table string
	At    time.Time
}

type Ring struct {
	buf  []EventStat
	pos  int
	size int
	mu   sync.Mutex
}

func NewRing(n int) *Ring {
	return &Ring{buf: make([]EventStat, n), size: n}
}

func (r *Ring) Add(s EventStat) {
	r.mu.Lock()
	r.buf[r.pos%r.size] = s
	r.pos++
	r.mu.Unlock()
}
