package frontend

// This file contains an implementation of a very simple FIFO pool.
//
// If there is a connection timeout configured, it will launch a pool cleaner goroutine
// to occasionally cycle through the available connections to help ensure that an
// infrequently used pool will not contain expired connections for long.

import (
	"log"
	"sync/atomic"
	"time"
)

type Pool struct {
	Name string

	active *int64
	queue  chan *FConn
}

func NewPool(name string, size int, connMaxLifetime *time.Duration) *Pool {
	pool := &Pool{
		Name:   name,
		active: new(int64),
		queue:  make(chan *FConn, size),
	}

	// the cleaner is only needed if connections have a max lifetime
	if connMaxLifetime != nil {
		go pool.cleaner()
	}

	return pool
}

func (p *Pool) cleaner() {
	for {
		activeConns := p.Active()
		for i := 0; i < activeConns; i++ {
			conn := p.Pop()
			if conn.Closed() {
				log.Printf("cleaned dead conn out of %s pool", p.Name)
				continue
			}
			p.Push(conn)
		}

		time.Sleep(1 * time.Second)
	}
}

// PushNew will push a connection onto the pool and increment active count
func (p *Pool) PushNew(v *FConn) {
	atomic.AddInt64(p.active, 1)
	p.queue <- v
}

func (p *Pool) Push(v *FConn) {
	p.queue <- v
}

func (p *Pool) Pop() *FConn {
	for {
		conn := <-p.queue
		if !conn.Closed() {
			return conn
		}
	}
}

// Expired is called when a member of the pool will not be returning
func (p *Pool) Expired() {
	atomic.AddInt64(p.active, -1)
}

func (p *Pool) Len() int {
	return len(p.queue)
}

func (p *Pool) Active() int {
	return int(atomic.LoadInt64(p.active))
}
