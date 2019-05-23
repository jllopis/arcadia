package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jllopis/arcadia/connectors"
	"github.com/jllopis/arcadia/connectors/mqtt"
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

	// MQTT Connector
	listenChannel := make(chan []byte)
	mqttSubOpts := &connectors.SubscribeOptions{
		Topic: TOPIC,
		Qos:   mqtt.QoS_ZERO,
	}
	mqttPubOpts := &connectors.PublishOptions{Topic: TOPIC, Qos: mqtt.QoS_ZERO, Immediate: true}
	mqttConnector := mqtt.New(BROKER).
		SetCleanSession(true).
		SetClientID("ARCADIA-TEST-MQTT-1").
		SetMaxReconnectInterval(30 * time.Second).
		SetDefaultSubscribeOptions(mqttSubOpts).
		SetDefaultPublishOptions(mqttPubOpts)

	if err := mqttConnector.Connect(); err != nil {
		log.Panicf("Can't connect to broker: %v\n", err)
	}
	defer mqttConnector.Disconnect()

	mqttConnector.Listen(nil, listenChannel)

	go func() {
		for msg := range listenChannel {
			fmt.Printf("Listen got: %s\n", string(msg))
		}
	}()

	runMQTTOnReceiver()

	go runMQTTPublisher()

	go runMQTTStreamer()
	time.Sleep(3 * time.Second)
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

func runMQTTStreamer() {
	mqttPubOpts := &connectors.PublishOptions{Topic: TOPIC, Qos: mqtt.QoS_ZERO, Immediate: true}
	mqttStreamer := mqtt.New(BROKER).
		SetCleanSession(true).
		SetClientID("ARCADIA-TEST-MQTT-PUBLISHER-2").
		SetMaxReconnectInterval(30 * time.Second).
		SetDefaultPublishOptions(mqttPubOpts)

	if err := mqttStreamer.Connect(); err != nil {
		log.Panicf("Can't connect to broker: %v\n", err)
	}
	defer mqttStreamer.Disconnect()

	strm := make(chan []byte)
	err := mqttStreamer.Stream(nil, strm)
	if err != nil {
		log.Panicf("Can't setup a streamer. error: &s\n", err.Error())
	}
	for i := 0; i < 10; i++ {
		strm <- []byte(fmt.Sprintf("STREAM MSG %d", i))
	}
}

func runMQTTOnReceiver() {
	mqttSubOpts := &connectors.SubscribeOptions{
		Topic: TOPIC,
		Qos:   mqtt.QoS_ZERO,
	}
	mqttConnector := mqtt.New(BROKER).
		SetCleanSession(true).
		SetClientID("ARCADIA-TEST-MQTT-SUBSCRIBER-2").
		SetMaxReconnectInterval(30 * time.Second).
		SetDefaultSubscribeOptions(mqttSubOpts)

	if err := mqttConnector.Connect(); err != nil {
		log.Panicf("Can't connect to broker: %v\n", err)
	}
	// defer mqttConnector.Disconnect()

	mqttConnector.On(nil, recvFunc)
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
