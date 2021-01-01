// Package receiver contains an actor that receives messages from a client application
// and forwards those messages to the backend manager
package receiver

import (
	"log"

	"github.com/coder543/roundabout/misc"
	"github.com/jackc/pgproto3/v2"
	"github.com/mitchellh/copystructure"
)

func Launch(
	closeBackend func(),
	receive func() (pgproto3.FrontendMessage, error),

) <-chan pgproto3.FrontendMessage {
	in := make(chan pgproto3.FrontendMessage, 1)

	go receiver(closeBackend, receive, in)

	return in
}

func receiver(
	closeBackend func(),
	receive func() (pgproto3.FrontendMessage, error),
	in chan<- pgproto3.FrontendMessage,
) {
	defer misc.Recover()
	defer closeBackend()

	for {
		bmsg, err := receive()
		if err != nil {
			if misc.IsTemporary(err) {
				continue
			}
			if err.Error() != "EOF" {
				log.Println("b-receive", err)
			}
			return
		}
		// log.Println("b-msg-2", misc.Marshal(bmsg))

		copy, err := copystructure.Copy(bmsg)
		if err != nil {
			log.Println("b-receive", err)
			return
		}
		in <- copy.(pgproto3.FrontendMessage)
	}
}
