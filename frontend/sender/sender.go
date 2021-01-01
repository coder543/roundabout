// Package sender receives messages from the backend manager and forwards
// them directly to Postgres
package sender

import (
	"log"
	"time"

	"github.com/jackc/pgproto3/v2"

	"github.com/coder543/roundabout/misc"
)

func Launch(
	closed chan struct{},
	closeFrontend func(),
	send func(message pgproto3.FrontendMessage) error,
) chan<- pgproto3.FrontendMessage {

	in := make(chan pgproto3.FrontendMessage, 1)

	go sender(
		in,
		closed,
		closeFrontend,
		send,
	)

	return in
}

func sender(
	in <-chan pgproto3.FrontendMessage,
	closed chan struct{},
	closeFrontend func(),
	send func(message pgproto3.FrontendMessage) error,
) {
	defer misc.Recover()
	defer closeFrontend()

	for {
		var fmsg pgproto3.FrontendMessage
		select {
		case <-closed:
			return
		case fmsg = <-in:
		}
		// log.Println("f-msg-2", misc.Marshal(fmsg))
		err := send(fmsg)
		if err != nil {
			// retry loop on temporary failure
			for i := 0; i < 10 && err != nil && misc.IsTemporary(err); i++ {
				time.Sleep(25 * time.Millisecond)
				err = send(fmsg)
			}

			// did retry fail?
			if err != nil {
				log.Println("f-send", err)
				return
			}
		}
	}
}
