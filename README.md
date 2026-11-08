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

## Architektura projektu

```text
edge-k8s-platform/
├── main.go                    #Inicjalizacja konfiguracji, klienta K8s i start agenta
├── internal/
│   ├── config/
│   │   └── config.go          # Zmienne środowiskowe, tematy MQTT, timeouty i wagi algorytmu licytacji
│   ├── model/
│   │   └── model.go           # Modele domenowe (TaskCFP, BidPayload, TaskResult, ClusterResources) oraz ReservationTracker
│   ├── k8s/
│   │   ├── client.go          # Inicjalizacja połączenia z API Kubernetes (InClusterConfig / kubeconfig)
│   │   ├── metrics.go         # Pobieranie dostępnych zasobów węzłów (CPU/RAM) z Metrics API i sprawdzanie cache'u obrazów
│   │   └── executor.go        # Tworzenie podów z zadaniami, monitorowanie ich statusu oraz czyszczenie po zakończeniu
│   └── agent/
│       ├── agent.go           # Logika biznesowa agenta: obsługa aukcji (CFP) oraz wykonywania zadań (Execute)
│       └── mqtt.go            # Konfiguracja klienta MQTT, automatyczne wznawianie połączenia, subskrypcje i publikacja JSON
└── dispatcher/
    └── dispatcher.go         
```

### Przepływ działania agenta
1. **Start (`main.go`)**: Wczytuje konfigurację (`internal/config`), tworzy klienta Kubernetes (`internal/k8s`) oraz uruchamia instancję `agent.Agent`.
2. **Obsługa licytacji (`(mqtt) clusterless/auction/cfp`)**:
   - Po otrzymaniu zapytania `TaskCFP` agent odpytuje API Kubernetes (`k8s.GetClusterResources`) o wolne zasoby CPU/RAM pomniejszone o aktualnie zarezerwowane zadania (`model.ReservationTracker`) oraz weryfikuje, czy wymagany obraz kontenera znajduje się już w pamięci podręcznej węzła.
   - Jeśli klaster spełnia minimalne wymagania zadania, wylicza wynik oferty (`Score` z premią `CachedImageBonus` za lokalny obraz) i publikuje `BidPayload` na `(mqtt) clusterless/auction/bids`.
3. **Wykonywanie zadania (`(mqtt) clusterless/execute/<CLUSTER_ID>`)**:
   - Zwycięski klaster rezerwuje zasoby w `ReservationTracker`, tworzy jednorazowy Pod (`k8s.ExecuteFunctionPod`), oczekuje na jego zakończenie, publikuje wynik (`TaskResult`) na `(mqtt) clusterless/results`, a następnie usuwa Pod i zwalnia rezerwację.

## Wdrożenie fizyczne (ARM64)
Repozytorium zawiera gotową paczkę `rpi-agent-deploy.tar.gz` (binarka + usługa systemd).

1. Pobierz i rozpakuj archiwum na Raspberry Pi.
2. Nadaj uprawnienia do wykonywania: `chmod +x agent`.
3. Dostosuj zmienne środowiskowe w pliku `k8s-mqtt-agent.service`.
4. Skopiuj plik usługi do `/etc/systemd/system/`.
5. Uruchom i włącz autostart: `sudo systemctl enable --now k8s-mqtt-agent`.