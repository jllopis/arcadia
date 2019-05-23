package mqtt

import (
	"errors"
	"fmt"
	"log"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/jllopis/arcadia/connectors"
	"github.com/jllopis/retry"
)

// var _ connectors.Connector = (*Connector)(nil)

const (
	// QoS_ZERO specifies "at most once"
	QoS_ZERO = byte(0)
	// QoS_ONE specifies "at least once"
	QoS_ONE = byte(1)
	// QoS_TWO specifies "exactly once"
	QoS_TWO = byte(2)
)

type Connector struct {
	name               string
	C                  MQTT.Client
	subscriptions      map[string]*connectors.ConnectorSubscription
	defaultPutOpts     *connectors.PublishOptions
	defaultConsumeOpts *connectors.SubscribeOptions
	clientOptions      *MQTT.ClientOptions
}

func New(host string) *Connector {
	c := &Connector{
		name: "MQTT Broker",
		clientOptions: MQTT.NewClientOptions().
			AddBroker(host).
			SetAutoReconnect(true).
			SetClientID(connectors.GenID()),
		subscriptions: map[string]*connectors.ConnectorSubscription{},
	}
	c.SetOnConnectHandler(MQTT.OnConnectHandler(c.mqttOnConnect))
	c.SetConnectionLostHandler(MQTT.ConnectionLostHandler(c.mqttLostConnection))

	return c
}

// Connect returns true if connection to mqtt is established
func (c *Connector) Connect() error {
	return c.Dial()
}

// Dial stablish a connection with the broker and make it usable
func (c *Connector) Dial() error {
	if len(c.clientOptions.Servers) < 1 {
		log.Print("[MQTTConnector] No Server provided. Defaulting to tcp://localhost:1883")
		c.clientOptions.AddBroker("tcp://localhost:1883")
	}
	// Close connection if exists to start clean
	// c.Disconnect()
	retryStrategy := &retry.All{
		&retry.ExponentialBackoffStrategy{
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     20 * time.Second,
		},
		&retry.CountStrategy{
			Tries: 100,
		},
	}
	res, err := c.dial(retryStrategy)
	if err != nil {
		log.Printf("[MQTTConnector] f=Dial e=%v", err)
		return err
	}
	if !res {
		log.Print("[MQTTConnector] f=Dial e=Cannot connect to MQTT broker")
		return errors.New("cannot connect to MQTT broker")
	}

	return nil
}

func (c *Connector) dial(strategy retry.Strategy) (success bool, err error) {
	client := MQTT.NewClient(c.clientOptions)
	// TODO (jllopis): add an error package like github.com/hashicorp/go-multierror to be able to return all errors
	for strategy.Next() {
		if success = func() bool {
			log.Printf("[MQTTConnector] f=Dial brokers=%+v s=Trying connection to broker", c.clientOptions.Servers)
			if token := client.Connect(); token.Wait() && token.Error() == nil {
				c.C = client
				log.Printf("[MQTTConnector] f=Dial s=connected broker=%+v", c.clientOptions.Servers)
				return true
			} else {
				err = token.Error()
				log.Printf("[MQTTConnector] f=Dial s=not connected e=%v", err)
				return false
			}
		}(); success {
			return true, nil
		}
	}
	return
}

// Disconnect returns true if connection to mqtt is closed
func (c *Connector) Disconnect() {
	if c.C != nil {
		c.C.Disconnect(500)
	}
	return
}

func (c *Connector) Close() error {
	c.Disconnect()
	return nil
}

func (c *Connector) String() string {
	return fmt.Sprintf("Type: MQTT, Brokers: %+v", c.clientOptions.Servers)
}
