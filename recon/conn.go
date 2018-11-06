package recon

import (
	"io"
	"net"
)

type Conn interface {
	io.Reader
	io.Writer
	io.Closer
	RemoteAddr() net.Addr
}
