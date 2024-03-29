// Package frontend is responsible for handling connections to a postgres database.
package frontend

// This file contains functions that are used mostly by the backend manager to
// manage the state of the frontend, but there isn't a specific frontend manager actor
// for each frontend connection like there is for the backend.

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgproto3/v2"

	"github.com/coder543/roundabout/config"
	"github.com/coder543/roundabout/frontend/receiver"
	"github.com/coder543/roundabout/misc"
)

const defaultApplicationName = "roundabout"

var GlobalPreamble = []pgproto3.BackendMessage{}
var GlobalPreambleLock = &sync.RWMutex{}

var WritePool *Pool
var ReadPool *Pool

type FConn struct {
	conn           net.Conn
	expirationTime *time.Time
	pool           *Pool

	reqPreamble chan<- (chan<- struct{})
	receiver    *receiver.Receiver
	In          chan<- pgproto3.FrontendMessage

	closed     chan struct{}
	detaching  chan struct{}
	detachLock *sync.Mutex

	isClosed   bool
	closedLock *sync.Mutex

	backendTerminator   func()
	applicationName     string
	applicationNameLock *sync.Mutex
}

func (c *FConn) Closed() bool {
	if c.expirationTime != nil && time.Now().After(*c.expirationTime) {
		c.close()
		return true
	}
	c.closedLock.Lock()
	defer c.closedLock.Unlock()
	return c.isClosed
}

func (c *FConn) close() {
	c.closedLock.Lock()
	defer c.closedLock.Unlock()
	if c.isClosed {
		return
	}

	log.Printf("%s conn closing", c.pool.Name)

	c.isClosed = true
	c.pool.Expired()

	close(c.closed)
	c.conn.Close()

	c.detachLock.Lock()
	defer c.detachLock.Unlock()
	if c.backendTerminator != nil {
		log.Println("terminating backend connection")
		c.backendTerminator()
	}
}

func (c *FConn) ReqPreamble() {
	doneChan := make(chan struct{})
	c.reqPreamble <- doneChan
	<-doneChan
}

func (c *FConn) SetApplicationName(applicationName string) {
	c.applicationNameLock.Lock()
	defer c.applicationNameLock.Unlock()

	if c.applicationName == applicationName {
		return
	}

	c.applicationName = applicationName

	c.receiver.SetDropRFQ(true)
	c.In <- &pgproto3.Query{String: fmt.Sprintf("SET application_name = '%s'", applicationName)}
}

func (c *FConn) ClearApplicationName() {
	c.SetApplicationName(defaultApplicationName)
}

type AttachChannels struct {
	Out     <-chan pgproto3.BackendMessage
	OutSync *misc.Cond
}

func (c *FConn) AttachBackend(terminator func()) AttachChannels {
	c.detachLock.Lock()
	defer c.detachLock.Unlock()

	c.backendTerminator = terminator

	c.detaching = make(chan struct{})
	out := make(chan pgproto3.BackendMessage, 1)
	outSync := misc.NewCond()

	outSync.L.Lock()
	c.receiver.NewOut <- receiver.NewOutChans{
		Out:       out,
		OutSync:   outSync,
		Detaching: c.detaching,
	}
	outSync.WaitAndUnlock()

	return AttachChannels{
		Out:     out,
		OutSync: outSync,
	}
}

func (c *FConn) DetachBackend() {
	if config.Virtual.ClearApplicationNames {
		c.ClearApplicationName()
	}
	c.detachLock.Lock()
	defer c.detachLock.Unlock()
	c.receiver.AwaitDropRFQChan()
	if c.detaching != nil {
		close(c.detaching)
	}
	c.backendTerminator = nil
	c.pool.Push(c)
}

func (c *FConn) BackendTerminated() {
	c.detachLock.Lock()
	c.backendTerminator = nil
	c.detachLock.Unlock()

	c.close()
}
