package main

import (
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	"github.com/jackc/pgproto3/v2"

	"github.com/coder543/roundabout/backend"
	"github.com/coder543/roundabout/config"
	"github.com/coder543/roundabout/frontend"
)

func main() {
	// Performance testing has shown almost a +70% performance boost
	// for roundabout just by limiting GOMAXPROCS to 1.
	//
	// It is possible that it will perform better with more procs in
	// certain environments, so we simply change the default here,
	// but still respect any override placed in the environment.
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(1)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)
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
