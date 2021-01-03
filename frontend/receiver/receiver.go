// Package receiver contains an actor that receives messages from Postgres
// and (usually) forwards those messages to the backend manager. This actor
// is probably the closest analogue to the backend manager on the frontend,
// but it is simpler than the backend manager, and not truly comparable.
package receiver

import (
	"log"
	"sync"
	"time"

	"github.com/jackc/pgproto3/v2"

	"github.com/coder543/roundabout/misc"
)

var unblockedChannel = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

type NewOutChans struct {
	Out       chan pgproto3.BackendMessage
	OutSync   *misc.Cond
	Detaching chan struct{}
}

type Receiver struct {
	NewOut chan<- NewOutChans

	newOut    <-chan NewOutChans
	outLock   *sync.RWMutex
	out       chan pgproto3.BackendMessage
	outSync   *misc.Cond
	detaching chan struct{}

	closed        chan struct{}
	closeFrontend func()
	receive       func() (pgproto3.BackendMessage, error)

	dropRFQ     bool
	dropRFQChan chan struct{}
	dropRFQLock *sync.Mutex
}

func Launch(
	closed chan struct{},
	closeFrontend func(),
	receive func() (pgproto3.BackendMessage, error),
) *Receiver {

	newOut := make(chan NewOutChans)

	r := &Receiver{
		NewOut:        newOut,
		newOut:        newOut,
		outLock:       &sync.RWMutex{},
		dropRFQChan:   unblockedChannel,
		dropRFQLock:   new(sync.Mutex),
		closeFrontend: closeFrontend,
		closed:        closed,
		receive:       receive,
	}

	go r.outManager()
	go r.receiver()

	return r
}

func (r *Receiver) outManager() {
	defer misc.Recover()
	defer r.closeFrontend()
	defer func() {
		r.outLock.Lock()
		defer r.outLock.Unlock()
		if r.out != nil {
			close(r.out)
		}
		r.outSync.SignalLocked()
		r.out = nil
		r.outSync = nil
		r.detaching = nil
	}()

	for {
		select {
		case newChan := <-r.newOut:
			r.outLock.Lock()
			if r.out != nil {
				close(r.out)
			}
			r.out = newChan.Out
			r.outSync = newChan.OutSync
			r.detaching = newChan.Detaching

			r.outSync.SignalLocked()
			r.outLock.Unlock()

		case <-r.detaching:
			r.outLock.Lock()
			if r.out != nil {
				close(r.out)
			}
			r.outSync.SignalLocked()
			r.out = nil
			r.outSync = nil
			r.detaching = nil
			r.outLock.Unlock()

		case <-r.closed:
			return
		}
	}
}

func (r *Receiver) receiver() {
	defer misc.Recover()
	defer r.closeFrontend()

	for {
		bmsg, err := r.receive()
		if err != nil {
			if misc.IsTemporary(err) {
				continue
			}
			log.Println("f-receive", err)
			return
		}

		// when proxying application_name, we will get an RFQ that the
		// backend doesn't need to know about
		if r.RFQSkip(bmsg) {
			continue
		}

		closed := r.Send(bmsg)
		if closed {
			return
		}
	}
}

func (r *Receiver) Send(bmsg pgproto3.BackendMessage) bool {
	r.outLock.RLock()
	defer r.outLock.RUnlock()

	if r.out == nil {
		log.Println("client detached unexpectedly. cannot send message to client!", misc.Marshal(bmsg))
		return true
	}

	// log.Println("f-msg-1", misc.Marshal(bmsg))
	r.outSync.L.Lock()
	select {
	case r.out <- bmsg:
		r.outSync.WaitAndUnlock()
	case <-r.closed:
		return true
	}

	return false
}

// when proxying application_name, we will get an RFQ that the
// backend doesn't need to know about
func (r *Receiver) RFQSkip(bmsg pgproto3.BackendMessage) bool {
	if !r.getDropRFQ() {
		return false
	}

	if bmsg, ok := bmsg.(*pgproto3.ParameterStatus); ok && bmsg.Name == "application_name" {
		return true
	}
	if bmsg, ok := bmsg.(*pgproto3.CommandComplete); ok && string(bmsg.CommandTag) == "SET" {
		return true
	}
	if _, ok := bmsg.(*pgproto3.ReadyForQuery); ok {
		r.SetDropRFQ(false)
		return true
	}

	return false
}

func (r *Receiver) SetDropRFQ(val bool) {
	r.dropRFQLock.Lock()
	defer r.dropRFQLock.Unlock()
	if r.dropRFQ == val {
		if !val {
			log.Println("Error: attempted to SetDropRFQ(false) when it was already false.")
			r.closeFrontend()
			return
		}

		dropRFQChan := r.dropRFQChan

		r.dropRFQLock.Unlock()
		maxWait := time.NewTimer(1 * time.Second)
		select {
		case <-dropRFQChan:
			maxWait.Stop()
		case <-maxWait.C:
			log.Println("Error: SetDropRFQ timeout expired")
		}
		r.dropRFQLock.Lock()

		if r.dropRFQ == val {
			log.Println("Error: tried to SetDropRFQ to the same value twice")
			r.closeFrontend()
			return
		}
	}

	r.dropRFQ = val
	if r.dropRFQ {
		r.dropRFQChan = make(chan struct{})
	} else {
		close(r.dropRFQChan)
		r.dropRFQChan = unblockedChannel
	}
}

func (r *Receiver) getDropRFQ() bool {
	r.dropRFQLock.Lock()
	defer r.dropRFQLock.Unlock()
	return r.dropRFQ
}

func (r *Receiver) AwaitDropRFQChan() {
	r.dropRFQLock.Lock()
	dropRFQChan := r.dropRFQChan
	r.dropRFQLock.Unlock()

	<-dropRFQChan
}
