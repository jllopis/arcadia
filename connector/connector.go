package connector

import "time"

type Connector interface {
	// Name returns the label for the service
	Name() string
	// ID returns the client ID
	ID() string
	// Dial connects to the server
	Dial() error
	// Close terminates the service and close the remote connection
	Close() error
	// Put send a message to the server
	Put(opts *PublishOptions, msg []byte) error
	// Stream opens a channel to push messages to the server as they come
	Stream(opts *PublishOptions, ch chan []byte) error
	// Get gets a single message from the server
	Get(opts *SubscribeOptions) []byte
	// On subscribes to a topic and calls the handler function when a message is received
	On(opts *SubscribeOptions, f func(s []byte)) error
	// Listen subscribe on a topic and push the messages on a channel when received
	Listen(opts *SubscribeOptions, ch chan []byte) error
	// GetPublishOptions retrieve the defined Publish options for the connector
	GetPublishOptions() *PublishOptions
	// GetSubscribeOptions retrieve the defined Subscription options for the connector
	GetSubscribeOptions() *SubscribeOptions
	// String returns a string representation of the service.
	String() string
}

type Table map[string]interface{}

// Publishing struct comes from https://github.com/streadway/amqp/blob/master/types.go#L150
// and keeps the data needed to publish through amqp. It can be useful in other backends too.
type Publishing struct {
	Headers         Table
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
	Priority        uint8     // 0 to 9
	CorrelationId   string    // correlation identifier
	ReplyTo         string    // address to to reply to (ex: RPC)
	Expiration      string    // message expiration spec
	MessageId       string    // message identifier
	Timestamp       time.Time // message timestamp
	Type            string    // message type name
	UserId          string    // creating user id - ex: "guest"
	AppId           string    // creating application id

	// The application specific payload of the message
	Body []byte
}

type PublishOptions struct {
	Topic      string // this will be RoutingKey in AMQP
	Qos        byte
	Retained   bool
	Exchange   string
	Mandatory  bool
	Immediate  bool
	Publishing Publishing
}

type SubscribeOptions struct {
	Queue     string
	Consumer  string
	Topic     string
	Qos       byte
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      Table
	Fn        func(msg []byte)
	Ch        chan []byte
}

type ConnectorSubscription struct {
	Options *SubscribeOptions
	Fn      func(msg []byte)
	Ch      chan []byte
	Class   string // specify if subscribing either with "Listen" or "On" methods
}
