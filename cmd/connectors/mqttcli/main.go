package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jllopis/arcadia/connectors"
	"github.com/jllopis/arcadia/connectors/mqtt"
)

var mf = func(msg []byte) []byte {
	return []byte(fmt.Sprintf("[SUBS] %s", msg))
}

func main() {
	setupSignals()

	// MQTT Connector
	subscriptionCh := make(chan []byte)
	mqttSubOpts := &connectors.SubscribeOptions{
		Topic: "test",
		Qos:   mqtt.QoS_ZERO,
	}
	// mqttPubOpts := &connectors.PublishOptions{Topic: "test", Qos: mqtt.QoS_ZERO, Immediate: true}
	mqttConnector := mqtt.New("tcp://localhost:1883").
		SetCleanSession(true).
		SetClientID("ARCADIA-TEST-MQTT-1").
		SetMaxReconnectInterval(30 * time.Second)

	if err := mqttConnector.Connect(); err != nil {
		log.Panicf("Can't connect to broker: %v\n", err)
	}

	mqttConnector.Listen(mqttSubOpts, subscriptionCh)

	for msg := range subscriptionCh {
		fmt.Println(string(msg))
	}

	mqttConnector.Disconnect()
}

// setupSignals configura la captura de señales de sistema y actúa basándose en ellas
func setupSignals() {
	sc := make(chan os.Signal, 1)
	go func() {
		for sig := range sc {
			log.Printf("[mqtttest.signal_notify] captured signal %v", sig)
			os.Exit(1)
		}
	}()
}
