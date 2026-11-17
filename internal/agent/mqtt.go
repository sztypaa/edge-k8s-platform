package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"time"

	"k8s-mqtt-agent/pkg/protocol"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	agentClientIDFmt = "cp-agent-%s"
)

// newMQTTClientOptios returns pointer to preconfigured [mqtt.ClientOptions] for given MQTT broker
func (a *Agent) newMQTTClientOptions(broker string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	configureBrokerAddress(opts, broker)

	opts.SetClientID(fmt.Sprintf(agentClientIDFmt, a.clusterID))
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(protocol.MQTTRetryInterval)
	opts.SetKeepAlive(protocol.MQTTKeepAlive)
	// Handlers publish and wait for PUBACK, with ordered delivery this deadlocks when many messages arrive at once
	opts.SetOrderMatters(false)
	opts.SetOnConnectHandler(a.onConnect)
	opts.SetConnectionLostHandler(a.onConnectionLost)

	return opts
}

// onConnect subscribes client to [protocol.TopicCFP] with [Agent.handleCFP] callback 
// and cluster specific topic for task execution defined by [Agent.execTopic] with [Agent.handleExecute] callback
func (a *Agent) onConnect(client mqtt.Client) {
	log.Printf("[INFO] Agent %s connected to MQTT broker, subscribing topics...", a.clusterID)
	subscribeTopic(client, protocol.TopicCFP, a.handleCFP)
	subscribeTopic(client, a.execTopic(), a.handleExecute)
}

// onConnectionLost logs error loss of connection to MQTT broker by client
func (a *Agent) onConnectionLost(_ mqtt.Client, err error) {
	log.Printf("[WARN] Connection to MQTT broker lost: %v. Reconnecting...", err)
}

// configureBrokerAddress adds broker to [mqtt.ClientOptions]
func configureBrokerAddress(opts *mqtt.ClientOptions, broker string) {
	brokerURL, err := url.Parse(broker)
	if err == nil && brokerURL.User != nil {
		opts.AddBroker(fmt.Sprintf("%s://%s", brokerURL.Scheme, brokerURL.Host))
		opts.SetUsername(brokerURL.User.Username())
		password, _ := brokerURL.User.Password()
		opts.SetPassword(password)
		return
	}
	opts.AddBroker(broker)
}

// connectWithRetry will repeatedly try to connect MQTT client to broker with [protocol.MQTTRetryInterval]
// This function is blocking.
func connectWithRetry(client mqtt.Client) {
	for {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Printf("[WARN] Initial MQTT connect failed: %v, retrying in %v...", token.Error(), protocol.MQTTRetryInterval)
			time.Sleep(protocol.MQTTRetryInterval)
			continue
		}
		return
	}
}

// subscribeTopic subscribes MQTT client to MQTT topic with handler callback to execute when message is published.
// This function is blocking.
func subscribeTopic(client mqtt.Client, topic string, handler mqtt.MessageHandler) {
	if token := client.Subscribe(topic, protocol.MQTTQoS, handler); token.Wait() && token.Error() != nil {
		log.Printf("[ERROR] Failed to subscribe to %s: %v", topic, token.Error())
	}
}

// publishJSON publishes payload to MQTT topic from given client.
// This function is blocking.
func publishJSON(client mqtt.Client, topic string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[ERROR] Failed to marshal payload for %s: %v", topic, err)
		return
	}
	if token := client.Publish(topic, protocol.MQTTQoS, false, data); token.Wait() && token.Error() != nil {
		log.Printf("[ERROR] Failed to publish message to %s: %v", topic, token.Error())
	}
}
