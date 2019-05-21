package port

import "github.com/jllopis/arcadia/connector"

const (
	INPUT = iota
	OUTPUT
	INOUT
)

var (
	Direction = []string{"INPUT", "OUTPUT", "INOUT"}
)

type Port interface {
	GetID() string
	SetConnector(connector.Connector) error
	UpdateConnector(connector.Connector) error
	// GetIoChan() chan []byte
	GetName() string
	// Implements io.Reader interface
	Read(p []byte) (n int, err error)
	// Implements io.Writer interface
	Write(p []byte) (n int, err error)
	Run() error
	Close()
}

type PortArray struct {
	Ports []Port
	Type  int
}
