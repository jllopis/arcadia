package mqtt

import (
	"errors"
	"fmt"
	"log"
	"strings"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/jllopis/arcadia/connectors"
)

func (c *Connector) Name() string {
	return c.name
}

// ID returns the client ID
func (c *Connector) ID() string {
	return c.clientOptions.ClientID
}

func (c *Connector) mqttOnConnect(client MQTT.Client) {
	log.Print("[Connector] f=mqttOnConnect status=Connected")
	for _, v := range c.subscriptions {
		if v.Fn == nil && v.Ch == nil {
			continue
		}

		var fn func(MQTT.Client, MQTT.Message)

		if v.Fn != nil {
			fn = func(c MQTT.Client, msg MQTT.Message) { v.Fn(msg.Payload()) }
		}
		if v.Ch != nil {
			fn = c.onReceive
		}

		if token := c.C.Subscribe(v.Options.Topic, v.Options.Qos, fn); token.Wait() && token.Error() != nil {
			log.Printf("[Connector] f=mqttOnConnect status=resubscribe e=%s", token.Error().Error())
		}
		log.Printf("[Connector] f=mqttOnConnect status=resubscribe restarted subscription topic=%s", v.Options.Topic)
	}
}

func (c *Connector) mqttLostConnection(client MQTT.Client, err error) {
	log.Printf("[Connector] f=mqttLostConnection status=Lost connection broker e=%s", err.Error())
}

func (c *Connector) Put(opts *connectors.PublishOptions, msg []byte) error {
	if opts == nil || c.defaultPutOpts != nil {
		opts = c.defaultPutOpts
	} else {
		return errors.New("publish options cannot be nil")
	}

	var token MQTT.Token
	if token = c.C.Publish(opts.Topic, opts.Qos, opts.Retained, msg); token.Wait() && token.Error() != nil {
		log.Printf("[MQTTConnector] f=Put error=%s", token.Error().Error())
		return token.Error()
	}
	return nil
}

func (c *Connector) Stream(opts *connectors.PublishOptions, ch chan []byte) error {
	if opts == nil || c.defaultPutOpts != nil {
		opts = c.defaultPutOpts
	} else {
		return errors.New("publish options cannot be nil")
	}
	go func() {
		for {
			select {
			case msg := <-ch:
				fmt.Println("[MQTTConnector] got stream message")
				if err := c.Put(opts, msg); err != nil {
					log.Printf("[MQTTConnector] f=Stream error: %s", err.Error())
				}
			}
		}
	}()
	return nil
}

//func (c *Connector) On(opts interface{}, f func(c *MQTT.Client, msg MQTT.Message)) error {
func (c *Connector) On(opts *connectors.SubscribeOptions, f func([]byte)) error {
	if opts == nil || c.defaultConsumeOpts != nil {
		opts = c.defaultConsumeOpts
	} else {
		return errors.New("publish options cannot be nil")
	}
	fn := func(c MQTT.Client, msg MQTT.Message) { f(msg.Payload()) }
	if token := c.C.Subscribe(opts.Topic, opts.Qos, fn); token.Wait() && token.Error() != nil {
		log.Printf("[MQTTConnector] f=On error=%s", token.Error().Error())
		return token.Error()
	}
	log.Printf("[MQTTConnector] f=On status=started subscription topic=%s", opts.Topic)
	c.subscriptions[opts.Topic] = &connectors.ConnectorSubscription{Options: opts, Fn: f, Class: "On"}
	return nil
}

func (c *Connector) Listen(opts *connectors.SubscribeOptions, ch chan []byte) error {
	if opts == nil || c.defaultConsumeOpts != nil {
		opts = c.defaultConsumeOpts
	} else {
		return errors.New("subscription options cannot be nil")
	}

	// Add subscription first so onReceive doesn't panic if there is a retained message
	c.subscriptions[opts.Topic] = &connectors.ConnectorSubscription{
		Options: opts,
		Ch:      ch,
		Class:   "Listen",
	}

	if token := c.C.Subscribe(opts.Topic, opts.Qos, c.onReceive); token.Wait() && token.Error() != nil {
		//	logger.Info("Listen", "error", token.Error().Error())
		log.Printf("[Connector] f=Listen e=%s", token.Error().Error())

		delete(c.subscriptions, opts.Topic)

		return token.Error()
	}
	return nil
}

func (c *Connector) onReceive(client MQTT.Client, msg MQTT.Message) {
	for topic, _ := range c.subscriptions {
		if !topicMatches(topic, msg.Topic()) {
			continue
		}

		if c.subscriptions[topic].Ch != nil {
			c.subscriptions[topic].Ch <- msg.Payload()
		}
	}

}

func topicMatches(pattern, topic string) bool {
	patternSegments := strings.Split(pattern, "/")
	topicSegments := strings.Split(topic, "/")

	patternLength := len(patternSegments)
	topicLength := len(topicSegments)
	lastIndex := patternLength - 1

	for i := 0; i < patternLength; i++ {
		currentPattern := patternSegments[i]
		if topicLength <= i && currentPattern != "#" {
			return false
		}

		currentTopic := ""
		if i < topicLength {
			currentTopic = topicSegments[i]
		}

		patternChar := ""
		if len(currentPattern) > 0 {
			patternChar = string(currentPattern[0])
		}
		// Only allow # at end
		if patternChar == "#" {
			return i == lastIndex
		}
		if patternChar != "+" && currentPattern != currentTopic {
			return false
		}
	}

	return patternLength == topicLength
}
