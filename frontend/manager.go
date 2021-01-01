package frontend

// This file contains the "frontend manager", which is nothing like the backend manager.
// The purpose of the frontend manager is to maintain a pool of connections to a particular
// postgres instance, including refilling that pool if necessary.

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgproto3/v2"

	"github.com/coder543/roundabout/config"
	"github.com/coder543/roundabout/frontend/preambler"
	"github.com/coder543/roundabout/frontend/receiver"
	"github.com/coder543/roundabout/frontend/sender"
	"github.com/coder543/roundabout/misc"
)

func LaunchAll() {

	numConns := int(config.Database.NumConns)
	WritePool = NewPool(config.Database.DatabaseName+" (primary)", numConns, config.Database.MaxLifetime)
	ReadPool = NewPool(config.Database.DatabaseName+" (read replicas)", numConns, config.Database.MaxLifetime)

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go launchFrontendManager(wg, numConns, WritePool, []string{config.Database.Address})

	if len(config.Database.ReadReplicas) > 0 {
		go launchFrontendManager(wg, numConns, ReadPool, config.Database.ReadReplicas)
	} else {
		ReadPool = WritePool
		wg.Done()
	}

	wg.Wait()

	log.Println("connections established")
}

func launchFrontendManager(wgPool *sync.WaitGroup, numConns int, pool *Pool, dbAddrs []string) {
	defer wgPool.Done()

	log.Printf("Establishing %d connections to %s\n", numConns, pool.Name)

	wgConn := &sync.WaitGroup{}
	wgConn.Add(numConns)

	addrSelector := 0
	for i := 0; i < numConns; i++ {
		go launchConn(wgConn, dbAddrs[addrSelector], pool)
		addrSelector = (addrSelector + 1) % len(dbAddrs)
	}

	wgConn.Wait()

	if pool.Active() != numConns {
		log.Fatalf("Could not establish required connections to database %s: %d < %d",
			pool.Name,
			pool.Active(),
			numConns,
		)
	}

	go frontendManager(numConns, pool, dbAddrs)
}

func frontendManager(numConns int, pool *Pool, dbAddrs []string) {
	backOff := 200 * time.Millisecond
	addrSelector := 0
	for {
		time.Sleep(backOff)

		connsToLaunch := numConns - pool.Active()
		if connsToLaunch <= 0 {
			continue
		}

		log.Printf("frontendManager: refilling %s pool by adding %d conns\n", pool.Name, connsToLaunch)

		wgConn := &sync.WaitGroup{}
		wgConn.Add(connsToLaunch)
		for i := 0; i < connsToLaunch; i++ {
			go launchConn(wgConn, dbAddrs[addrSelector], pool)
			addrSelector = (addrSelector + 1) % len(dbAddrs)
		}
		wgConn.Wait()

		if pool.Active() == numConns {
			log.Printf("%s pool size optimal again\n", pool.Name)
			backOff = 200 * time.Millisecond
			continue
		}

		if backOff < 4800*time.Millisecond {
			backOff += 800 * time.Millisecond
		}

		log.Printf("%s pool failed to reach optimal size, retrying in %s\n", pool.Name, backOff)
	}
}

// TODO: need to add a timeout to this function
func launchConn(wgConn *sync.WaitGroup, dbAddr string, pool *Pool) {
	defer wgConn.Done()

	conn, err := net.Dial("tcp", dbAddr)
	if err != nil {
		log.Println("f dial", err)
		return
	}

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)

	db := config.Database

	params := map[string]string{
		"client_encoding":  "UTF8",
		"datestyle":        "ISO, MDY",
		"application_name": defaultApplicationName,
		"user":             db.Username,
	}

	if db.Database != "" {
		params["database"] = db.Database
	}

	startup := &pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters:      params,
	}

	err = frontend.Send(startup)
	if err != nil {
		log.Println("f-send startup", err)
		return
	}

	GlobalPreambleLock.RLock()
	defer GlobalPreambleLock.RUnlock()
	globalPreambleNum := 0
	firstGlobalPreamble := len(GlobalPreamble) == 0
	if firstGlobalPreamble {
		GlobalPreambleLock.RUnlock()
		GlobalPreambleLock.Lock()
		// did someone race us here?
		if len(GlobalPreamble) != 0 {
			GlobalPreambleLock.Unlock()
			GlobalPreambleLock.RLock()
			firstGlobalPreamble = false
		} else {
			defer GlobalPreambleLock.RLock() // to match up with the defer RUnlock above
			defer GlobalPreambleLock.Unlock()
		}
	}
	localPreamble := []pgproto3.BackendMessage{}
	// drain the ParameterStatus and BackendKeyData messages, looking for RFQ
	for {
		fmsg, err := frontend.Receive()
		if err != nil {
			if misc.IsTemporary(err) {
				continue
			}
			log.Println("f-receive startup err", err)
			return
		}
		switch fmsg := fmsg.(type) {
		case *pgproto3.ErrorResponse:
			log.Println(fmsg)
			return
		case *pgproto3.AuthenticationOk:
			continue
		case *pgproto3.AuthenticationCleartextPassword:
			err := frontend.Send(&pgproto3.PasswordMessage{Password: db.Password})
			if err != nil {
				log.Println("send pass err", err)
				return
			}
		case *pgproto3.AuthenticationMD5Password:
			h1 := md5.New()
			_, _ = io.WriteString(h1, db.Password)
			_, _ = io.WriteString(h1, db.Username)
			h2 := md5.New()
			_, _ = io.WriteString(h2, fmt.Sprintf("%x", h1.Sum(nil)))
			_, _ = h2.Write(fmsg.Salt[:])
			passwordHash := fmt.Sprintf("md5%x", h2.Sum(nil))
			err := frontend.Send(&pgproto3.PasswordMessage{Password: passwordHash})
			if err != nil {
				log.Println("send pass err", err)
				return
			}
		case *pgproto3.ReadyForQuery:
			goto pushConn
		case *pgproto3.ParameterStatus:
			// we only want to save some server-specific
			// preambles that should be constant to the
			// global preamble. Each connection will retain
			// the other preambles.
			switch fmsg.Name {
			case "server_encoding":
			case "server_version":
			case "integer_datetimes":
			default:
				fmsgClone := *fmsg
				localPreamble = append(localPreamble, &fmsgClone)
				continue
			}
			if firstGlobalPreamble {
				fmsgClone := *fmsg
				GlobalPreamble = append(GlobalPreamble, &fmsgClone)
			} else {
				if *fmsg != *GlobalPreamble[globalPreambleNum].(*pgproto3.ParameterStatus) {
					log.Println("warning: servers are not homogeneous!")
				}
				globalPreambleNum++
			}
		}
	}

pushConn:

	fConn := &FConn{
		conn:                conn,
		closed:              make(chan struct{}),
		pool:                pool,
		applicationName:     defaultApplicationName,
		detachLock:          new(sync.Mutex),
		closedLock:          new(sync.Mutex),
		applicationNameLock: new(sync.Mutex),
	}

	fConn.In = sender.Launch(fConn.closed, fConn.close, frontend.Send)
	fConn.receiver = receiver.Launch(fConn.closed, fConn.close, frontend.Receive)
	fConn.reqPreamble = preambler.Launch(fConn.closed, fConn.close, fConn.receiver.Send, localPreamble)

	if db.MaxLifetime != nil {
		expirationTime := time.Now().Add(*db.MaxLifetime)
		fConn.expirationTime = &expirationTime
	}

	pool.PushNew(fConn)
}
