# K8s Control Plane MQTT Agent

Agent monitorujący zdarzenia z API Kubernetes i wysyłający je w formacie JSON do zewnętrznego brokera MQTT.
Zoptymalizowany pod kątem działania na architekturze ARM64 (K3s na Raspberry Pi).

## Wymagania
- Środowisko uruchomieniowe: Kubernetes / K3s
- Broker: RabbitMQ (z wtyczką MQTT), Mosquitto lub EMQX

## Konfiguracja (Zmienne środowiskowe)
- `MQTT_BROKER` - adres serwera (np. `tcp://100.92.5.77:1883`)
- `CLUSTER_ID` - identyfikator węzła dodawany do tematu MQTT
- `KUBECONFIG` - ścieżka do autoryzacji (domyślnie InClusterConfig lub `~/.kube/config`)

## Wdrożenie fizyczne (ARM64)
Repozytorium zawiera gotową paczkę `rpi-agent-deploy.tar.gz` (binarka + usługa systemd).

1. Pobierz i rozpakuj archiwum na Raspberry Pi.
2. Nadaj uprawnienia do wykonywania: `chmod +x agent`.
3. Dostosuj zmienne środowiskowe w pliku `k8s-mqtt-agent.service`.
4. Skopiuj plik usługi do `/etc/systemd/system/`.
5. Uruchom i włącz autostart: `sudo systemctl enable --now k8s-mqtt-agent`.