package main

import (
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/jackc/pgproto3/v2"

	"github.com/zeropascals/roundabout/backend"
	"github.com/zeropascals/roundabout/config"
	"github.com/zeropascals/roundabout/frontend"
)

func main() {
	frontend.LaunchAll()

	// launch debug pprof server
	if config.PprofAddr != "" {
		go func() {
			err := http.ListenAndServe(config.PprofAddr, nil)
			if err != nil {
				log.Panicln("could not start debug server:", err)
			}
		}()
	}

	ln, err := net.Listen("tcp", config.Virtual.Listen)
	if err != nil {
		log.Panicln(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Panicln(err)
		}

		protoBackend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
		go backend.Launch(conn, protoBackend)
	}
}
