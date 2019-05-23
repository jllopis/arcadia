package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jllopis/arcadia/connectors"
	"github.com/jllopis/arcadia/connectors/mqtt"
	"github.com/jllopis/arcadia/connectors/stdout"
)

const (
	TOPIC  = "test"
	BROKER = "tcp://localhost:1883"
)

var mf = func(msg []byte) []byte {
	return []byte(fmt.Sprintf("[SUBS] %s", msg))
}

func main() {
	setupSignals()
	out := stdout.New()

	// MQTT Connector
	listenChannel := make(chan []byte)
	mqttSubOpts := &connectors.SubscribeOptions{
		Topic: TOPIC,
		Qos:   mqtt.QoS_ZERO,
	}
	mqttConnector := mqtt.New(BROKER).
		SetCleanSession(true).
		SetClientID("ARCADIA-TEST-MQTT-1").
		SetMaxReconnectInterval(30 * time.Second).
		SetDefaultSubscribeOptions(mqttSubOpts)

	if err := mqttConnector.Connect(); err != nil {
		log.Panicf("Can't connect to broker: %v\n", err)
	}
	defer mqttConnector.Disconnect()

	mqttConnector.Listen(nil, listenChannel)

	go func() {
		for msg := range listenChannel {
			out.Put(nil, msg)
		}
	}()

	runMQTTPublisher()
	time.Sleep(1 * time.Second)
}

func runMQTTPublisher() {
	mqttPubOpts := &connectors.PublishOptions{Topic: TOPIC, Qos: mqtt.QoS_ZERO, Immediate: true}
	mqttPublisher := mqtt.New(BROKER).
		SetCleanSession(true).
		SetClientID("ARCADIA-TEST-MQTT-PUBLISHER-1").
		SetMaxReconnectInterval(30 * time.Second).
		SetDefaultPublishOptions(mqttPubOpts)

	if err := mqttPublisher.Connect(); err != nil {
		log.Panicf("Can't connect to broker: %v\n", err)
	}
	defer mqttPublisher.Disconnect()

	for i := 0; i < 10; i++ {
		mqttPublisher.Put(nil, []byte(fmt.Sprintf("MSG %d", i)))
	}
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

func recvFunc(msg []byte) {
	fmt.Printf("Applying func on received message. Payload: %s\n", string(msg))
}
