// Package sender contains an actor that receives messages from both the backend manager
// and postgres, and it forwards those messages to the client application
package sender

import (
	"log"

	"github.com/jackc/pgproto3/v2"
	"github.com/zeropascals/roundabout/frontend"
	"github.com/zeropascals/roundabout/misc"
)

var silentChannel = make(<-chan pgproto3.BackendMessage, 1)

func Launch(
	closeBackend func(),
	detachDB func() bool,
	send func(msg pgproto3.BackendMessage) error,
	closed <-chan struct{},
	newDB <-chan frontend.AttachChannels,
) chan<- pgproto3.BackendMessage {
	out := make(chan pgproto3.BackendMessage, 1)

	go sender(
		closeBackend,
		detachDB,
		send,
		closed,
		newDB,
		out,
	)

	return out
}

func sender(
	closeBackend func(),
	detachDB func() bool,
	send func(msg pgproto3.BackendMessage) error,
	closed <-chan struct{},
	newDB <-chan frontend.AttachChannels,
	out <-chan pgproto3.BackendMessage,
) {
	defer misc.Recover()
	defer closeBackend()

	dbRec := silentChannel
	var dbSync *misc.Cond

	defer func() {
		if dbSync != nil {
			dbSync.SignalLocked()
		}
	}()

	for {
		var bmsg pgproto3.BackendMessage
		var ok bool
		select {
		case <-closed: // this backend is shutting down, we're either disconnecting the client or vice versa
			return
		case newRec := <-newDB: // switch postgres connections
			if dbSync != nil {
				dbSync.SignalLocked()
			}
			dbSync = newRec.OutSync
			dbRec = newRec.Out
			continue
		case bmsg, ok = <-dbRec: // the currently attached postgres is sending a message
			// if the dbRec channel is closed unexpectedly, we should kill this backend
			if !ok {
				return
			}
		case bmsg, ok = <-out: // roundabout is sending a message directly to the client
			// sometimes we need to send a message to the client first, in which
			// case the bmsg channel will be closed after the final message
			if !ok {
				return
			}
		}

		// if this is an RFQ, we want to detach from the database
		rfq, wantToDetach := bmsg.(*pgproto3.ReadyForQuery)
		if wantToDetach {
			// make a copy of the RFQ so that we can safely detach the database now
			rfqCopy := *rfq
			bmsg = &rfqCopy

			if dbSync != nil {
				// There may exist a pathological case where Postgres could send a message
				// immediately after a client being initialized receives the post-preamble RFQ.
				// In this specific case, the frontend.receiver would be waiting to send the
				// message through the channel, and dbSync.L would be locked. SignalLocked()
				// might create a deadlock if that happened, so instead we could spawn this
				// into a separate goroutine here.
				//
				// The code as written should prevent this deadlock, but if a deadlock appears...
				// this is the first place to check.
				dbSync.SignalLocked()
			}

			// the backend manager decides whether detaching is successful or not
			if detachDB() {
				dbRec = silentChannel
				dbSync = nil
			}
		}

		// log.Println("b-msg-1", misc.Marshal(bmsg))

		err := send(bmsg)
		if dbSync != nil {
			dbSync.SignalLocked()
		}
		if err != nil {
			if misc.IsTemporary(err) {
				continue
			}
			log.Println("b-send", err)
			return
		}
	}
}
