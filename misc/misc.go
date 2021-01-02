package misc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
)

func Marshal(i interface{}) string {
	b, err := json.Marshal(i)
	if err != nil {
		log.Panicln(err)
	}
	return fmt.Sprintf("%T %s", i, string(b))
}

// TODO: do something more useful here than just panic again
func Recover() {
	err := recover()
	if err != nil {
		log.Panicf("recovered unexpected panic: %v", err)
	}
}

func IsTemporary(err error) bool {
	if err, ok := err.(net.Error); ok {
		return err.Temporary()
	}
	return false
}

func SocketClosedError(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed)
}
