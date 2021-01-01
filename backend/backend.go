// Package backend contains the backend manager, which is an actor that oversees the connection
// to a particular client application.
//
// This is called "backend" because from the client application's point of view, we *are* the
// database implementation.
package backend

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/zeropascals/roundabout/backend/sender"

	"github.com/zeropascals/roundabout/backend/receiver"

	"github.com/jackc/pgproto3/v2"

	"github.com/zeropascals/roundabout/config"
	"github.com/zeropascals/roundabout/frontend"
	"github.com/zeropascals/roundabout/misc"
)

type BConn struct {
	// read only
	applicationName string

	// synchronized by closedLock
	closedLock *sync.Mutex
	closed     chan struct{}
	conn       net.Conn

	// synchonized by dbLock
	dbLock                *sync.Mutex
	db                    *frontend.FConn
	txStatus              byte
	tempPreparedStatement bool
	pendingResponse       int

	// fields only accessed from backendRunner goroutine
	in    <-chan pgproto3.FrontendMessage
	out   chan<- pgproto3.BackendMessage
	newDB chan<- frontend.AttachChannels
}

func Launch(b net.Conn, backend *pgproto3.Backend) {
	newDB := make(chan frontend.AttachChannels, 1)

	conn := BConn{
		conn:       b,
		newDB:      newDB,
		closed:     make(chan struct{}),
		closedLock: new(sync.Mutex),
		dbLock:     new(sync.Mutex),
		txStatus:   'I',
	}

	go conn.backendRunner(backend, newDB)
}

func (c *BConn) close() {
	c.closedLock.Lock()
	defer c.closedLock.Unlock()
	if c.conn != nil {
		close(c.closed)
		c.conn.Close()
		c.conn = nil
	}

	// if client is closed while holding
	// onto a DB, make sure to rollback any
	// open transaction and put it back into
	// the db pool
	c.dbLock.Lock()
	defer c.dbLock.Unlock()
	if c.db != nil {

		c.pendingResponse--
		if c.pendingResponse > 0 {
			c.db.BackendTerminated()
			c.db = nil
			return
		}

		if c.txStatus != 'I' {
			c.db.In <- &pgproto3.Query{String: "rollback"}
		}

		if config.Virtual.ClearApplicationNames && c.applicationName != "" {
			c.db.ClearApplicationName()
		}

		c.db.DetachBackend()
		c.db = nil
	}
}

// senderDetachDB is used by the sender to cleanly return a DB connection to the pool
// when it is no longer needed
func (c *BConn) senderDetachDB() bool {
	c.dbLock.Lock()
	defer c.dbLock.Unlock()

	// we can only detach when we're not in a transaction,
	// and if there is a database attached
	if c.txStatus != 'I' || c.db == nil {
		return false
	}

	c.pendingResponse--
	if c.pendingResponse > 0 {
		return false
	}

	// we also can't detach during an anonymous prepared statement
	if c.tempPreparedStatement {
		c.tempPreparedStatement = false
		return false
	}

	// otherwise, we succesfully detached
	c.db.DetachBackend()
	c.db = nil
	return true
}

func (c *BConn) backendRunner(
	backend *pgproto3.Backend,
	newDB <-chan frontend.AttachChannels,
) {
	defer misc.Recover()
	defer c.close()

	startup, err := backend.ReceiveStartupMessage()
	switch startup.(type) {
	case *pgproto3.SSLRequest:
		_, err = c.conn.Write([]byte{'N'}) // tell the client we don't do SSL yet
		if err != nil {
			log.Panicln(err)
		}
		startup, err = backend.ReceiveStartupMessage()
		if err != nil {
			log.Println("startup error", err)
			return
		}
		if _, ok := startup.(*pgproto3.StartupMessage); !ok {
			log.Printf("expected startup message, received: %v", startup)
			return
		}
	case *pgproto3.CancelRequest:
		log.Printf("just ignored a cancelation request: %v", startup)
		return
	}

	if err != nil {
		if err.Error() == "can't handle ssl connection request" {
			_, err = c.conn.Write([]byte{'N'}) // tell the client we don't do SSL
			if err != nil {
				log.Panicln(err)
			}
			startup, err = backend.ReceiveStartupMessage()
			if err != nil {
				log.Println("startup error", err)
				return
			}
		} else if strings.HasPrefix(err.Error(), "Bad startup message version") {
			log.Println("probably ignored a cancelation request:", err)
			return
		} else {
			log.Panicln(err)
		}
	}

	c.in = receiver.Launch(c.close, backend.Receive)
	c.out = sender.Launch(
		c.close, c.senderDetachDB, backend.Send, c.closed, newDB,
	)

	var params map[string]string
	if startup, ok := startup.(*pgproto3.StartupMessage); ok {
		params = startup.Parameters
	} else {
		// should be unreachable
		log.Panicln("expected startup message, found something else", startup)
	}

	username := params["user"]
	if username != config.Virtual.Username {
		c.sendClientFatal("28P01", `password authentication failed for user "`+username+`"`)
		return
	}

	if config.Virtual.Password != "" {
		c.out <- &pgproto3.AuthenticationCleartextPassword{}

		// receive the message, but don't get stuck if the client leaves
		var passMsg pgproto3.FrontendMessage
		select {
		case <-c.closed:
			return
		case passMsg = <-c.in:
		}

		if passMsg, ok := passMsg.(*pgproto3.PasswordMessage); ok {
			if passMsg.Password != config.Virtual.Password {
				c.sendClientFatal("28P01", `password authentication failed for user "`+username+`"`)
				return
			}
		} else {
			log.Println("expected password")
			c.sendClientFatal("28P01", `password authentication failed for user "`+username+`"`)
			return
		}
	}

	c.out <- &pgproto3.AuthenticationOk{}

	c.applicationName = params["application_name"]

	clientEncoding := params["client_encoding"]
	if clientEncoding != "" && clientEncoding != "UTF8" && clientEncoding != "SQL_ASCII" {
		c.sendClientFatal("", fmt.Sprintf("unsupported client_encoding: %q", clientEncoding))
		return
	}

	frontend.GlobalPreambleLock.RLock()
	for _, msg := range frontend.GlobalPreamble {
		c.out <- msg
	}
	frontend.GlobalPreambleLock.RUnlock()

	// send out the local preamble from one connection
	// error: doing it this way causes a db sync against
	// a virtual rfq, which results in the sync blocking
	//
	// c.db = frontend.WritePool.Pop()
	// c.newDB <- c.db.AttachBackend(c.terminate)
	// c.db.ReqPreamble()

	c.rfq()

	for {
		if !c.waitForCmd() {
			return
		}
	}
}

func (c *BConn) waitForCmd() bool {
	var fmsg pgproto3.FrontendMessage

	select {
	case <-c.closed:
		return false
	case fmsg = <-c.in:
	}

	c.dbLock.Lock()
	defer c.dbLock.Unlock()

	switch fmsg := fmsg.(type) {
	case *pgproto3.Parse:
		var ret bool
		fmsg.Query, ret = c.handleQuery(fmsg.Query)
		if ret {
			return true
		}

		if fmsg.Name == "" {
			c.tempPreparedStatement = true
		}

	case *pgproto3.Query:
		var ret bool
		fmsg.String, ret = c.handleQuery(fmsg.String)
		if ret {
			return true
		}

	case *pgproto3.Terminate:
		return false

	case *pgproto3.Bind:
		// if we aren't still holding a specific database connection, then the unnamed
		// prepared statement is considered invalid, since it will be whatever random
		// query was last prepared on that frontend connection, and not necessarily what
		// the application is expecting to encounter.
		if fmsg.PreparedStatement == "" && c.db == nil {
			c.sendClientError("", "", "attempted to bind nonexistent unnamed prepared statement!")
			return true
		}
	case *pgproto3.Execute:
		// similarly, we don't allow executing an anonymous portal if no connection is held
		if fmsg.Portal == "" && c.db == nil {
			c.sendClientError("", "", "attempted to execute nonexistent unnamed portal!")
			return true
		}
	}

	// TODO: read replica support here
	c.db = frontend.WritePool.Pop()
	c.newDB <- c.db.AttachBackend(c.terminate)
	if config.Virtual.ProxyApplicationNames && c.applicationName != "" {
		c.db.SetApplicationName(c.applicationName)
	}

	c.pendingResponse++
	if c.pendingResponse > 1 {
		log.Printf("pending response: %d\n%s", c.pendingResponse, misc.Marshal(fmsg))
	}
	c.db.In <- fmsg

	return true
}

func (c *BConn) handleQuery(query string) (string, bool) {
	// handle the special case of an empty query
	noWhitespace := strings.TrimSpace(query)
	if noWhitespace == "" || noWhitespace == ";" {
		c.out <- &pgproto3.EmptyQueryResponse{}
		c.rfq()
		return "", true
	}

	// TODO: for future use
	if strings.HasPrefix(query, "roundabout ") {
		c.handleInternalCommand(query)
		return "", true
	}

	return query, false
}

// rfq sends the ready for query message
func (c *BConn) rfq() {
	c.out <- &pgproto3.ReadyForQuery{TxStatus: c.txStatus}
}

func (c *BConn) terminate() {
	go c.close()
}

func (c *BConn) waitForShutdown() {
	select {
	case <-c.closed:
	case <-time.After(1 * time.Second):
	}
}

func (c *BConn) sendClientFatal(code, err string) {
	c.out <- &pgproto3.ErrorResponse{Severity: "FATAL", Code: code, Message: err}
	c.waitForShutdown()
}

func (c *BConn) sendClientError(severity, code, err string) (string, bool) {
	c.out <- &pgproto3.ErrorResponse{Severity: severity, Code: code, Message: err}
	c.rfq()
	return "", true
}

func (c *BConn) handleInternalCommand(query string) {
	query = strings.TrimSuffix(strings.TrimPrefix(query, "roundabout "), ";")
	switch query {
	case "help":
		c.sendClientError("", "", "supported commands:\nhelp")
	default:
		c.sendClientError("", "", "unrecognized roundabout command")
	}
}
