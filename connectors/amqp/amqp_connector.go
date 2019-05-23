package connectors

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jllopis/arcadia/connectors"

	"github.com/dgrijalva/retry"
	AMQP "github.com/streadway/amqp"
)

var _ connectors.Connector = (*Connector)(nil)

type Connector struct {
	URI string
	C          *AMQP.Connection
	Channel       *AMQP.Channel
	errChan       chan *AMQP.Error
	cancelMon     chan bool
	subscriptions []*connectors.ConnectorSubscription
	closed        chan error
	closedChannel chan error
	connected     bool
}

func NewConnector(uri string) *Connector {
	if uri == "" {
		log.Print("AMQP host cannot be empty")
		return nil
	}
	conn := &Connector{
		URI: uri,
		cancelMon:        make(chan bool),
		subscriptions:    make([]*connectors.ConnectorSubscription, 0),
		closed:           make(chan error),
		closedChannel:    make(chan error),
		connected:        false,
	}
	if len(opts.ClientID) == 0 {
		conn.ConnectorOptions.ClientID = GenID()
	}

	return conn
}

func (c *Connector) Name() string {
	return c.ClientID
}

func (c *Connector) ID() string {
	return c.ClientID
}

func (c *Connector) Dial() error {
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
		log.Printf("[AMQPConnector] f=Dial e=%s", err.Error())
		return err
	}
	if !res {
		err := errors.New("cannot connect to AMQP broker")
		log.Printf("[AMQPConnector] f=Dial e=%s", err.Error())
		return err
	}
	c.connected = true
	c.Monitor()

	return nil
}

func (c *Connector) dial(strategy retry.Strategy) (bool, error) {
	var err error

	for strategy.Next() {
		if success := func() bool {
			a.C, err = AMQP.Dial(c.uri)

			if err != nil {
				log.Printf("[AMQPConnector] f=dial s=not connected e=%s", err.Error())
				return false
			}

			log.Printf("[AMQPConnector] f=dial s=connected broker=%s", c.uri)
			c.Channel, err = c.C.Channel()
			if err != nil {
				log.Print("[AMQPConnector] f=dial s=not connected e=%s", err.Error())
				return false
			}

			go func() {
				aerr := <-c.C.NotifyClose(make(chan *AMQP.Error))
				c.connected = false

				if aerr == nil {
					log.Print("[AMQPConnector] f=dial s=connection gracefully closed")
				}
				if aerr != nil {
					log.Printf("[AMQPConnector] f=dial s=connection closed e=%s", aerr.Error())
				}

				c.closed <- aerr
			}()
			go func() {
				chanerr := <-c.Channel.NotifyClose(make(chan *AMQP.Error))
				c.connected = false

				if chanerr == nil {
					log.Print("[AMQPConnector] f=dial s=channel gracefully closed")
				}
				if chanerr != nil {
					log.Printf("[AMQPConnector] f=dial s=channel closed e=%s", chanerr.Error())
				}

				c.closedChannel <- chanerr
			}()

			log.Print("[AMQPConnector] f=dial s=channel opened")
			return true

		}(); success {
			return true, nil
		}
	}
	return false, err
}

// Monitor reset the status of the topology after disconnection from server
// Influenced by https://github.com/streadway/amqp/issues/133 per using a channel to
// be notified instead of using NotifyClose directly
func (c *Connector) Monitor() {
	go func() {
		select {
		case ee := <-c.closed:
			e := ee.(*AMQP.Error)
			log.Info("[AMQPConnector] f=Monitor s=connection closed e=%s", e.Error())
			err := c.Dial()
			if err != nil {
				log.Print("[AMQPConnector] f=Monitor s=cannot redial e=%s", err.Error())
			}
			c.resubscribe()
		case ee := <-c.closedChannel:
			e := ee.(*AMQP.Error)
			log.Printf("[AMQPConnector] f=Monitor s=channel closed e=%s", e.Error())
			var err error
			c.Channel, err = c.C.Channel()
			if err != nil {
				log.Printf("[AMQPConnector] f=Monitor s=cannot create channel e=%s", err.Error())
				err := c.Dial()
				if err != nil {
					log.Printf("[AMQPConnector] f=Monitor s=cannot redial e=%s", err.Error())
				}
				c.resubscribe()
			}
			c.resubscribe()
			go func() {
				chanerr := <-c.Channel.NotifyClose(make(chan *AMQP.Error))
				c.connected = false

				if chanerr == nil {
					log.Print("[AMQPConnector] f=Monitor s=channel gracefully closed")
				}
				if chanerr != nil {
					log.Printf("[AMQPConnector] f=Monitor s=channel closed e=%s", chanerr.Error())
				}
				c.closedChannel <- chanerr
			}()
		case <-c.cancelMon:
			log.Print("[AMQPConnector] f=Monitor s=quitting monitor on signal")
			break
		}
	}()
}

func (c *Connector) CancelMonitor() {
	c.cancelMon <- true
}

func (c *Connector) Close() error {
	c.CancelMonitor()
	if c.C == nil {
		return nil
	}

	if c.Channel != nil {
		err := c.Channel.Close()
		if err != nil {
			log.Printf("[AMQPConnector] f=Close s=error closing channel e=%s", err.Error())
		}
	}

	err := c.C.Close()
	if err != nil {
		log.Printf("[AMQPConnector] f=Close s=error closing connection e=%s", err.Error())
		return err
	}

	return nil
}

func (c *Connector) Put(opts *connectors.PublishOptions, msg []byte) error {
	if !c.connected {
		return errors.New("AMQP Service disconnected")
	}
	if opts == nil {
		return errors.New("No options specified")
	}

	// Must convert to AMQP.Table in order to use it as AMQP.Publishing and add RoutingKey
	p := opts.Publishing
	if p.Headers == nil {
		p.Headers = connectors.Table{}
	}
	AMQPpublishing := AMQP.Publishing{
		Headers:         AMQP.Table(p.Headers),
		ContentType:     p.ContentType,
		ContentEncoding: p.ContentEncoding,
		DeliveryMode:    p.DeliveryMode,
		Priority:        p.Priority,
		CorrelationId:   p.CorrelationId,
		ReplyTo:         p.ReplyTo,
		Expiration:      p.Expiration,
		MessageId:       p.MessageId,
		Timestamp:       p.Timestamp,
		Type:            p.Type,
		UserId:          p.UserId,
		AppId:           p.AppId,
	}

	AMQPpublishing.Body = msg

	if err := c.Channel.Publish(opts.Exchange, opts.Topic, opts.Mandatory, opts.Immediate, AMQPpublishing); err != nil {
		log.Printf("[AMQPConnector] f=Put s=error publishing message e=%s", err.Error())
		return err
	}
	return nil
}

func (c *Connector) Stream(opts *connectors.PublishOptions, ch chan []byte) error {
	if !c.connected {
		return errors.New("AMQP Service disconnected")
	}
	if opts == nil {
		return errors.New("No options specified")
	}
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

func (c *Connector) Get(opts *connectors.SubscribeOptions) []byte {
	if !c.connected {
		log.Print("[AMQPConnector] f=Get s=error consuming message e=AMQP Service disconnected")
		return nil
	}
	if opts == nil {
		log.Print("[AMQPConnector] f=Get s=error consuming message e=No options specified")
		return nil
	}
	msg, ok, err := c.Channel.Get(opts.Queue, opts.AutoAck)
	if !ok {
		if err != nil {
			log.Printf("[AMQPConnector] f=Get s=error consuming message e=%s", err.Error())
			return nil
		}
		log.Print("[AMQPConnector] f=Get s=error consuming message e=No messages in queue")
		return nil
	}
	if !opts.AutoAck {
		msg.Ack(false)
	}

	log.Printf("[AMQPConnector] f=Get s=got message from broker with id %d", msg.MessageId)
	log.Printf("[AMQPConnector] f=Get s=%d messages left on queue", msg.MessageCount)

	return msg.Body
}

func (c *Connector) On(opts *connectors.SubscribeOptions, f func(s []byte)) error {
	if opts == nil {
		return errors.New("Opts cannot be nil")
	}

	err := c.consumeFunc(opts, f)
	if err != nil {
		return err
	}

	c.subscriptions = append(c.subscriptions, &connectors.ConnectorSubscription{Options: opts, Fn: f, Ch: nil, Class: "On"})
	log.Printf("[AMQPConnector] f=On s=new subscription, %d subscriptions total", len(a.subscriptions))

	return nil
}

func (c *Connector) consumeFunc(opts *connectors.SubscribeOptions, f func(s []byte)) error {
	deliveries, err := c.Channel.Consume(
		opts.Queue,            // name
		opts.Consumer,         // consumerTag,
		opts.AutoAck,          // noAck
		opts.Exclusive,        // exclusive
		opts.NoLocal,          // noLocal
		opts.NoWait,           // noWait
		AMQP.Table(opts.Args), // arguments
	)
	if err != nil {
		return err
	}

	// TODO (jllopis): use context to finish goroutines
	go c.handleFunc(deliveries, opts.AutoAck, f)

	return nil
}

func (c *Connector) handleFunc(deliveries <-chan AMQP.Delivery, autoAck bool, f func(s []byte)) {
	for d := range deliveries {
		f(d.Body)
		if !autoAck {
			err := d.Ack(true)
			if err != nil {
				log.Print("[AMQPConnector] f=handleFunc s=cannot ack message with id %d e=%s", d.MessageId, err.Error())
			}
		}
	}
}

func (c *Connector) Listen(opts *connectors.SubscribeOptions, outch chan []byte) error {
	if opts == nil {
		return errors.New("No options specified")
	}

	err := c.consumeChan(opts, outch)
	if err != nil {
		return err
	}

	c.subscriptions = append(c.subscriptions, &connectors.ConnectorSubscription{Options: opts, Fn: nil, Ch: outch, Class: "Listen"})
	log.Printf("[AMQPConnector] f=Listen s=new subscription, %d subscriptions total", len(c.subscriptions))

	return nil
}

func (c *Connector) consumeChan(opts *connectors.SubscribeOptions, outch chan []byte) error {
	deliveries, err := a.Channel.Consume(
		opts.Queue,            // name
		opts.Consumer,         // consumerTag,
		opts.AutoAck,          // noAck
		opts.Exclusive,        // exclusive
		opts.NoLocal,          // noLocal
		opts.NoWait,           // noWait
		AMQP.Table(opts.Args), // arguments
	)
	if err != nil {
		return err
	}

	// TODO (jllopis): use context to finish goroutines
	go c.handleChan(deliveries, opts.AutoAck, outch)

	return nil
}

func (c *Connector) handleChan(deliveries <-chan AMQP.Delivery, autoAck bool, ch chan []byte) {
	for d := range deliveries {
		ch <- d.Body
		if !autoAck {
			err := d.Ack(true)
			if err != nil {
				log.Printf("[AMQPConnector] f=handleChan s=cannot ack message with id %d e=%s", d.MessageId, err.Error())
			}
		}
	}
}

func (c *Connector) resubscribe() error {
	log.Info("[AMQPConnector] f=resubscribe s=%d subscriptions", len(c.subscriptions))
	// Check registered queues already exist before trying to reconnect
	ln := len(c.subscriptions)
	for i, v := range c.subscriptions {
		if c.C == nil {
			err := errors.New("Connection is nil")
			log.Printf("[AMQPConnector] f=resubscribe s=cannot resubscribe e=%s", err)
			return err
		}
		if c.Channel == nil {
			err := errors.New("Channel is nil")
			log.Printf("[AMQPConnector] f=resubscribe s=cannot resubscribe e=%s", err)
			return err
		}

		if v == nil {
			log.Print("[AMQPConnector] f=resubscribe s=deleting nil subscription")

			if i+1 >= ln {
				c.subscriptions = append(c.subscriptions[:i])
				continue
			}
			c.subscriptions = append(c.subscriptions[:i], c.subscriptions[i+1:]...)
			continue
		}
		_, err := c.Channel.QueueInspect(v.Options.Queue)
		if err != nil {
			log.Printf("[AMQPConnector] f=resubscribe s=queue %s does not exists in broker e=%s", v.Options.Queue, err.Error())
			if i+1 >= ln {
				if len(c.subscriptions) > 1 {
					c.subscriptions = append(c.subscriptions[:i])
				} else {
					c.subscriptions[0] = nil
				}
				continue
			}

			c.subscriptions = append(c.subscriptions[:i], c.subscriptions[i+1:]...)
		}
	}
	for _, v := range c.subscriptions {
		if v == nil {
			continue
		}
		log.Printf("[AMQPConnector] f=resubscribe s=resubscribe %+v", v)
		switch v.Class {
		case "On":
			err := c.consumeFunc(v.Options, v.Fn)
			if err != nil {
				return err
			}
		case "Listen":
			err := c.consumeChan(v.Options, v.Ch)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Connector) String() string {
	return fmt.Sprintf("Type: AMQP, Server: %v", c.uri)
}

func (m *Connector) GetPublishOptions() *connectors.PublishOptions {
	return nil
}

func (m *Connector) GetSubscribeOptions() *connectors.SubscribeOptions {
	return nil
}
