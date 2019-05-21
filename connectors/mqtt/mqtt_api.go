package mqtt

import (
	"errors"
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
	log.Print("[MQTTConnector] f=mqttOnConnect status=Connected")
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
			log.Printf("[MQTTConnector] f=mqttOnConnect status=resubscribe e=%s", token.Error().Error())
		}
		log.Printf("[MQTTConnector] f=mqttOnConnect status=resubscribe restarted subscription topic=%s", v.Options.Topic)
	}
}

func (c *Connector) Listen(opts *connectors.SubscribeOptions, ch chan []byte) error {
	if opts == nil {
		if c.DefaultConsumeOpts != nil {
			opts = c.DefaultConsumeOpts
		} else {
			return errors.New("opts cannot be nil")
		}
	}

	// Add subscription first so onReceive doesn't panic if there is a retained message
	c.subscriptions[opts.Topic] = &connectors.ConnectorSubscription{
		Options: opts,
		Ch:      ch,
		Class:   "Listen",
	}

	if token := c.C.Subscribe(opts.Topic, opts.Qos, c.onReceive); token.Wait() && token.Error() != nil {
		//	logger.Info("Listen", "error", token.Error().Error())
		log.Printf("[MQTTConnector] f=Listen e=%s", token.Error().Error())

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

func (c *Connector) mqttLostConnection(client MQTT.Client, err error) {
	log.Printf("[MQTTConnector] f=mqttLostConnection status=Lost connection broker e=%s", err.Error())
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
