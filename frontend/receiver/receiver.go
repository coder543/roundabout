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

	"github.com/zeropascals/roundabout/misc"
)

var unblockedChannel = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

type NewOutChans struct {
	Out       chan pgproto3.BackendMessage
	OutSync   *sync.Cond
	Detaching chan struct{}
}

type Receiver struct {
	NewOut chan<- NewOutChans

	newOut    <-chan NewOutChans
	out       chan pgproto3.BackendMessage
	outSync   *sync.Cond
	detaching chan struct{}

	closed        chan struct{}
	closeFrontend func()
	receive       func() (pgproto3.BackendMessage, error)
	forwarderChan chan forwarderMsg

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
		forwarderChan: make(chan forwarderMsg, 1),
		dropRFQChan:   unblockedChannel,
		dropRFQLock:   new(sync.Mutex),
		closeFrontend: closeFrontend,
		closed:        closed,
		receive:       receive,
	}

	go r.forwarder()
	go r.receiver()

	return r
}

type forwarderMsg struct {
	msg      pgproto3.BackendMessage
	syncCond *sync.Cond
}

func (r *Receiver) forwarder() {
	defer misc.Recover()
	defer r.closeFrontend()
	defer func() {
		if r.out != nil {
			close(r.out)
		}
	}()

	for {
		var outerSync *sync.Cond

		select {
		case wrapper, ok := <-r.forwarderChan:
			if !ok {
				return
			}

			outerSync = wrapper.syncCond

			if r.out == nil {
				log.Println("client detached unexpectedly. cannot send message to client!", misc.Marshal(wrapper.msg))
				if outerSync != nil {
					outerSync.Signal()
				}
				return
			}

			select {
			case r.out <- wrapper.msg:
				r.outSync.L.Lock()
				r.outSync.Wait()
				r.outSync.L.Unlock()
				if outerSync != nil {
					outerSync.Signal()
				}
			case <-r.detaching:
				goto detaching
			}

		case newChan := <-r.newOut:
			if r.out != nil {
				close(r.out)
			}
			r.out = newChan.Out
			r.outSync = newChan.OutSync
			r.detaching = newChan.Detaching

			r.outSync.L.Lock()
			r.outSync.Signal()
			r.outSync.L.Unlock()

		case <-r.detaching:
			goto detaching

		case <-r.closed:
			return
		}

		continue

	detaching:
		if outerSync != nil {
			outerSync.Signal()
		}
		if r.out != nil {
			close(r.out)
		}
		r.out = nil
		r.outSync = nil
		r.detaching = nil
	}
}

func (r *Receiver) receiver() {
	defer misc.Recover()
	defer r.closeFrontend()
	defer close(r.forwarderChan)

	syncCond := &sync.Cond{L: &sync.Mutex{}}

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

		// log.Println("f-msg-1", misc.Marshal(bmsg))
		select {
		case r.forwarderChan <- forwarderMsg{msg: bmsg, syncCond: syncCond}:
			syncCond.L.Lock()
			syncCond.Wait()
			syncCond.L.Unlock()
		case <-r.closed:
			return
		}
	}
}

func (r *Receiver) Send(bmsg pgproto3.BackendMessage, syncCond *sync.Cond) {
	r.forwarderChan <- forwarderMsg{
		msg:      bmsg,
		syncCond: syncCond,
	}
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
		if val {
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
