module dispatcher

go 1.22.5

require (
	github.com/eclipse/paho.mqtt.golang v1.4.3
	k8s-mqtt-agent v0.0.0
)

require (
	github.com/gorilla/websocket v1.5.0 // indirect
	golang.org/x/net v0.19.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
)

replace k8s-mqtt-agent => ../
