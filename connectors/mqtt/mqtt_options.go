package mqtt

import (
	"crypto/tls"
	"net/http"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/jllopis/arcadia/connectors"
)

type mqttOption func(*MQTT.ClientOptions)

func (c *Connector) SetDefaultPublishOptions(opts *connectors.PublishOptions) *Connector {
	c.defaultPutOpts = opts
	return c
}

func (c *Connector) SetDefaultSubscribeOptions(opts *connectors.SubscribeOptions) *Connector {
	c.defaultConsumeOpts = opts
	return c
}

// AddBroker adds a broker URI to the list of brokers to be used. The format should be scheme://host:port Where "scheme" is one of "tcp", "ssl", or "ws", "host" is the ip-address (or hostname) and "port" is the port on which the broker is accepting connections.
//
// Default values for hostname is "127.0.0.1", for schema is "tcp://".
//
// An example broker URI would look like: tcp://foobar.com:1883
func (c *Connector) AddBroker(server string) *Connector {
	c.clientOptions.AddBroker(server)
	return c
}

// SetAutoReconnect sets whether the automatic reconnection logic should be used when the connection is lost, even if disabled the ConnectionLostHandler is still called
func (c *Connector) SetAutoReconnect(a bool) *Connector {
	c.clientOptions.SetAutoReconnect(a)
	return c
}

// SetBinaryWill accepts a []byte will message to be set. When the client connects, it will give this will message to the broker, which will then publish the provided payload (the will) to any clients that are subscribed to the provided topic.
func (c *Connector) SetBinaryWill(topic string, payload []byte, qos byte, retained bool) *Connector {
	c.clientOptions.SetBinaryWill(topic, payload, qos, retained)
	return c
}

// SetCleanSession will set the "clean session" flag in the connect message when this client connects to an MQTT broker. By setting this flag, you are indicating that no messages saved by the broker for this client should be delivered. Any messages that were going to be sent by this client before diconnecting previously but didn't will not be sent upon connecting to the broker.
//
// Should be false if reconnect is enabled. Otherwise all subscriptions will be lost
func (c *Connector) SetCleanSession(clean bool) *Connector {
	c.clientOptions.SetCleanSession(clean)
	return c
}

// SetClientID will set the client id to be used by this client when connecting to the MQTT broker. According to the MQTT v3.1 specification, a client id mus be no longer than 23 characters.
func (c *Connector) SetClientID(id string) *Connector {
	c.clientOptions.SetClientID(id)
	return c
}

// SetConnectTimeout limits how long the client will wait when trying to open a connection to an MQTT server before timeing out and erroring the attempt. A duration of 0 never times out. Default 30 seconds. Currently only operational on TCP/TLS connections.
func (c *Connector) SetConnectTimeout(t time.Duration) *Connector {
	c.clientOptions.SetConnectTimeout(t)
	return c
}

// SetConnectionLostHandler will set the OnConnectionLost callback to be executed in the case where the client unexpectedly loses connection with the MQTT broker.
func (c *Connector) SetConnectionLostHandler(onLost MQTT.ConnectionLostHandler) *Connector {
	c.clientOptions.SetConnectionLostHandler(onLost)
	return c
}

// SetCredentialsProvider will set a method to be called by this client when connecting to the MQTT broker that provide the current username and password. Note: without the use of SSL/TLS, this information will be sent in plaintext accross the wire.
// func (c *Connector) SetCredentialsProvider(p CredentialsProvider) {}

// SetDefaultPublishHandler sets the MessageHandler that will be called when a message is received that does not match any known subscriptions.
func (c *Connector) SetDefaultPublishHandler(defaultHandler MQTT.MessageHandler) *Connector {
	c.clientOptions.SetDefaultPublishHandler(defaultHandler)
	return c
}

// SetHTTPHeaders sets the additional HTTP headers that will be sent in the WebSocket opening handshake.
func (c *Connector) SetHTTPHeaders(h http.Header) *Connector {
	c.clientOptions.SetHTTPHeaders(h)
	return c
}

// SetKeepAlive will set the amount of time (in seconds) that the client should wait before sending a PING request to the broker. This will allow the client to know that a connection has not been lost with the server.
func (c *Connector) SetKeepAlive(k time.Duration) *Connector {
	c.clientOptions.SetKeepAlive(k)
	return c
}

// SetMaxReconnectInterval sets the maximum time that will be waited between reconnection attempts when connection is lost
func (c *Connector) SetMaxReconnectInterval(t time.Duration) *Connector {
	c.clientOptions.SetMaxReconnectInterval(t)
	return c
}

// SetMessageChannelDepth sets the size of the internal queue that holds messages while the client is temporairily offline, allowing the application to publish when the client is reconnecting. This setting is only valid if AutoReconnect is set to true, it is otherwise ignored.
func (c *Connector) SetMessageChannelDepth(s uint) *Connector {
	c.clientOptions.SetMessageChannelDepth(s)
	return c
}

// SetOnConnectHandler sets the function to be called when the client is connected. Both at initial connection time and upon automatic reconnect.
func (c *Connector) SetOnConnectHandler(onConn MQTT.OnConnectHandler) *Connector {
	c.clientOptions.SetOnConnectHandler(onConn)
	return c
}

// SetOrderMatters will set the message routing to guarantee order within each QoS level. By default, this value is true. If set to false, this flag indicates that messages can be delivered asynchronously from the client to the application and possibly arrive out of order.
func (c *Connector) SetOrderMatters(order bool) *Connector {
	c.clientOptions.SetOrderMatters(order)
	return c
}

// SetPassword will set the password to be used by this client when connecting to the MQTT broker. Note: without the use of SSL/TLS, this information will be sent in plaintext accross the wire.
func (c *Connector) SetPassword(p string) *Connector {
	c.clientOptions.SetPassword(p)
	return c
}

// SetPingTimeout will set the amount of time (in seconds) that the client will wait after sending a PING request to the broker, before deciding that the connection has been lost. Default is 10 seconds.
func (c *Connector) SetPingTimeout(k time.Duration) *Connector {
	c.clientOptions.SetPingTimeout(k)
	return c
}

// SetProtocolVersion sets the MQTT version to be used to connect to the broker. Legitimate values are currently 3 - MQTT 3.1 or 4 - MQTT 3.1.1
func (c *Connector) SetProtocolVersion(pv uint) *Connector {
	c.clientOptions.SetProtocolVersion(pv)
	return c
}

// SetResumeSubs will enable resuming of stored (un)subscribe messages when connecting but not reconnecting if CleanSession is false. Otherwise these messages are discarded.
func (c *Connector) SetResumeSubs(resume bool) *Connector {
	c.clientOptions.SetResumeSubs(resume)
	return c
}

// SetStore will set the implementation of the Store interface used to provide message persistence in cases where QoS levels QoS_ONE or QoS_TWO are used. If no store is provided, then the client will use MemoryStore by default.
func (c *Connector) SetStore(s MQTT.Store) *Connector {
	c.clientOptions.SetStore(s)
	return c
}

// SetTLSConfig will set an SSL/TLS configuration to be used when connecting to an MQTT broker. Please read the official Go documentation for more information.
func (c *Connector) SetTLSConfig(t *tls.Config) *Connector {
	c.clientOptions.SetTLSConfig(t)
	return c
}

// SetUsername will set the username to be used by this client when connecting to the MQTT broker. Note: without the use of SSL/TLS, this information will be sent in plaintext accross the wire.
func (c *Connector) SetUsername(u string) *Connector {
	c.clientOptions.SetUsername(u)
	return c
}

// SetWill accepts a string will message to be set. When the client connects, it will give this will message to the broker, which will then publish the provided payload (the will) to any clients that are subscribed to the provided topic.
func (c *Connector) SetWill(topic string, payload string, qos byte, retained bool) *Connector {
	c.clientOptions.SetWill(topic, payload, qos, retained)
	return c
}

// SetWriteTimeout puts a limit on how long a mqtt publish should block until it unblocks with a timeout error. A duration of 0 never times out. Default 30 seconds
func (c *Connector) SetWriteTimeout(t time.Duration) *Connector {
	c.clientOptions.SetWriteTimeout(t)
	return c
}

// UnsetWill will cause any set will message to be disregarded.
func (c *Connector) UnsetWill() *Connector {
	c.clientOptions.UnsetWill()
	return c
}
