package main

import (
	"fmt"
	"log"
	"time"

	"github.com/jllopis/arcadia/connectors/mqtt"
)

var mf = func(msg []byte) []byte {
	return []byte(fmt.Sprintf("[SUBS] %s", msg))
}

func main() {
	// MQTT Connector
	// mqttSubOpts := &connectors.SubscribeOptions{Topic: "test", Qos: byte(0)}
	// mqttPubOpts := &connectors.PublishOptions{Topic: "test", Qos: byte(0), Immediate: true}

	mqttConnector := mqtt.New("tcp://localhost:1883").
		SetCleanSession(true).
		SetClientID("ARCADIA-TEST-MQTT-1")
	if err := mqttConnector.Connect(); err != nil {
		log.Panicf("Can't connect to broker: %v\n", err)
	}

	time.Sleep(20 * time.Second)

	mqttConnector.Disconnect()
}
