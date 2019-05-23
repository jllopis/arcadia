package stdout

import (
	"fmt"

	"github.com/jllopis/arcadia/connectors"
)

var _ connectors.Connector = (*StdoutConnector)(nil)

type StdoutConnector struct {
	ConnID string
}

func New() *StdoutConnector {
	return &StdoutConnector{ConnID: connectors.GenID()}
}

func (c *StdoutConnector) Name() string {
	return c.ConnID
}

func (c *StdoutConnector) ID() string {
	return c.ConnID
}

func (c *StdoutConnector) Dial() error {
	fmt.Println("StdoutConnector.Dial called")
	return nil
}

func (c *StdoutConnector) Close() error {
	return nil
}

func (c *StdoutConnector) Put(opts *connectors.PublishOptions, msg []byte) error {
	fmt.Printf("%s\n", msg)
	return nil
}

func (c *StdoutConnector) Stream(opts *connectors.PublishOptions, ch chan []byte) error {
	fmt.Println("StdoutConnector.Stream called")
	go func() {
		for {
			select {
			case msg := <-ch:
				c.Put(opts, msg)
			}
		}
	}()
	return nil
}

func (c *StdoutConnector) Get(opts *connectors.SubscribeOptions) []byte {
	return nil
}

func (c *StdoutConnector) On(opts *connectors.SubscribeOptions, f func(s []byte)) error {
	return nil
}

func (c *StdoutConnector) Listen(opts *connectors.SubscribeOptions, ch chan []byte) error {
	return nil
}

func (c *StdoutConnector) String() string {
	return fmt.Sprint("Type: Stdout")
}

func (m *StdoutConnector) GetPublishOptions() *connectors.PublishOptions {
	return nil
}

func (m *StdoutConnector) GetSubscribeOptions() *connectors.SubscribeOptions {
	return nil
}
