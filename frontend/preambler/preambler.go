// Package preambler contains an actor whose job is to send the Postgres preamble
// to each client on demand as clients connect to this particular Postgres frontend
package preambler

import (
	"github.com/jackc/pgproto3/v2"

	"github.com/zeropascals/roundabout/misc"
)

func Launch(
	closed chan struct{},
	closeFrontend func(),
	sendToBackend func(message pgproto3.BackendMessage),
	preamble []pgproto3.BackendMessage,
) chan (chan<- struct{}) {

	reqPreamble := make(chan (chan<- struct{}))

	go preambler(
		reqPreamble,
		closed,
		closeFrontend,
		sendToBackend,
		preamble,
	)

	return reqPreamble
}

func preambler(
	reqPreamble chan (chan<- struct{}),
	closed chan struct{},
	closeFrontend func(),
	sendToBackend func(message pgproto3.BackendMessage),
	preamble []pgproto3.BackendMessage,
) {
	defer misc.Recover()
	defer closeFrontend()

	for {
		select {
		case <-closed:
			return
		case req := <-reqPreamble:
			for _, msg := range preamble {
				sendToBackend(msg)
			}
			close(req)
		}
	}
}
