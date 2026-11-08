package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"time"

	"k8s-mqtt-agent/internal/config"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func (a *Agent) newMQTTClientOptions(broker string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	configureBrokerAddress(opts, broker)

	opts.SetClientID(fmt.Sprintf("cp-agent-%s", a.clusterID))
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(config.MQTTRetryInterval)
	opts.SetKeepAlive(config.MQTTKeepAlive)
	opts.SetOnConnectHandler(a.onConnect)
	opts.SetConnectionLostHandler(a.onConnectionLost)

	return opts
}

func (a *Agent) onConnect(client mqtt.Client) {
	log.Printf("[INFO] Agent %s connected to MQTT broker, subscribing topics...", a.clusterID)
	subscribeTopic(client, config.TopicCFP, a.handleCFP)
	subscribeTopic(client, a.execTopic(), a.handleExecute)
}

func (a *Agent) onConnectionLost(_ mqtt.Client, err error) {
	log.Printf("[WARN] Connection to MQTT broker lost: %v. Reconnecting...", err)
}

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

func connectWithRetry(client mqtt.Client) {
	for {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Printf("[WARN] Initial MQTT connect failed: %v, retrying in 5s...", token.Error())
			time.Sleep(config.MQTTRetryInterval)
			continue
		}
		return
	}
}

func subscribeTopic(client mqtt.Client, topic string, handler mqtt.MessageHandler) {
	if token := client.Subscribe(topic, config.MQTTQoS, handler); token.Wait() && token.Error() != nil {
		log.Printf("[ERROR] Failed to subscribe to %s: %v", topic, token.Error())
	}
}

func publishJSON(client mqtt.Client, topic string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[ERROR] Failed to marshal payload for %s: %v", topic, err)
		return
	}
	client.Publish(topic, config.MQTTQoS, false, data)
}
