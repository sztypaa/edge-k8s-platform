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
├── main.go                    # Inicjalizacja konfiguracji, klienta K8s i start agenta (moduł k8s-mqtt-agent)
├── pkg/
│   └── protocol/              # Wspólny, czysty protokół komunikacyjny (TaskCFP, BidPayload, AckPayload, TaskResult, tematy MQTT)
├── internal/                  # Wewnętrzne komponenty agenta K8s (zależne od k8s.io)
│   ├── config/                # Konfiguracja środowiskowa agenta (CLUSTER_ID, KUBECONFIG itp.)
│   ├── model/                 # Modele wewnętrzne agenta (ClusterResources, ReservationTracker)
│   ├── k8s/                   # Integracja z Kubernetes API (client, metrics-server, pod executor)
│   └── agent/                 # Pętla zdarzeń agenta, weryfikacja zasobów i obsługa ACK/RES
├── dispatcher/                # Niezależny moduł licytatora (własny go.mod, zero zależności od K8s)
│   ├── go.mod                 # Zależności wyłącznie od MQTT i pkg/protocol
│   ├── dispatcher.go          # Centralny serwer HTTP/MQTT rozstrzygający licytacje
│   └── strategy/              # Wymienne strategie licytatora (fallback ranking, re-auction, roundrobin/random do benchmarków)
├── bench/                     # Obraz testowy edge-task, generator obciążenia loadgen i wykresy (opis w bench/README.md)
└── tests/                     # Integracyjny zestaw testowy
```

### Przepływ działania systemu i protokół dwufazowy (Two-Phase Dispatch)
1. **Zgłoszenie zadania**: Klient wysyła `TaskCFP` przez HTTP `/submit` lub MQTT `clusterless/requests/submit`.
2. **Zapytanie ofertowe (`clusterless/auction/cfp`)**:
   - Dispatcher ogłasza aukcję. Agenci na klastrach sprawdzają stan wolnych zasobów i cache'u obrazu, publikując oferty `BidPayload` na `clusterless/auction/bids`.
3. **Wybór kandydata i protokół ACK / RES (`clusterless/execute/<CLUSTER_ID>` oraz `clusterless/auction/ack`)**:
   - Licytator typuje zwycięzcę według `Score` (opis niżej) i przesyła mu zadanie.
   - Wytypowany agent w sekcji krytycznej ponownie weryfikuje dostępność zasobów (`tryAcceptAndReserve`):
     - **Akceptacja (`ACK`)**: Klaster rezerwuje zasoby w `ReservationTracker`, wysyła `AckPayload{Status: "ACK"}` i uruchamia jednorazowy Pod.
     - **Rezygnacja (`RES`)**: W przypadku wygrania kilku aukcji równolegle i braku miejsca na kolejne zadanie, klaster natychmiast odsyła `AckPayload{Status: "RES", Reason: "..."}` bez blokowania zasobów.
4. **Wymienne strategie licytatora (cel badawczy)**:
   - **`fallback` (`RankingFallbackStrategy`, domyślna)**: Licytator generuje ranking ofert. W razie rezygnacji klastra natychmiast przekazuje zadanie kolejnemu kandydatowi z posortowanej listy.
   - **`reauction` (`ReauctionStrategy`)**: W razie rezygnacji licytator restartuje licytację (nowy CFP), dzięki czemu pobiera świeże oferty uwzględniające już nowe obciążenie klastrów.
   - **`roundrobin` / `random`**: Bez licytacji, klastry z listy `CLUSTERS` (flaga `-clusters`) po kolei albo losowo. Służą tylko jako punkt odniesienia w benchmarkach.
   - Strategia jest wybierana przy uruchomieniu aplikacji i pozostaje stała w czasie jej działania. Konfiguracja odbywa się przez flagę startową CLI `-strategy` (np. `go run ./dispatcher -strategy=reauction`) lub zmienną środowiskową `DISPATCHER_STRATEGY=fallback|reauction|roundrobin|random`.
5. **Wynik zadania (`clusterless/results`)**: Po zakończeniu Poda agent zwalnia rezerwację i publikuje `TaskResult` (z polem `image_cached`).

### Funkcja celu (`Score`)

Każda składowa jest ułamkiem od 0 do 1, więc wynik nie zależy od wielkości klastra:

```text
cpu    = (wolne CPU - CPU zadania) / CPU klastra          (obcięte do 0..1)
mem    = (wolna pamięć - pamięć zadania) / pamięć klastra (obcięte do 0..1)
cached = 1, jeśli obraz jest już na węźle, inaczej 0

Score = 1000 * (0.4 * cpu + 0.2 * mem + 0.4 * cached)
```

Przykłady (zadanie 500m CPU, 256 MiB, klaster 4 CPU, 4 GiB):

| Klaster | Score |
| --- | --- |
| pusty, bez obrazu | 537 |
| w połowie zajęty, z obrazem | 637 |
| prawie pełny, z obrazem | 412 |

Obraz na węźle oznacza szybszy start (bez pobierania), dlatego ma dużą wagę, ale nie wygrywa z klastrem, który jest prawie pełny. Wagi są w `internal/model/model.go`. Oferta zawiera też `total_cpu_milli` i `total_mem_mib`, żeby dało się sprawdzić wynik.

## Wdrożenie fizyczne (ARM64)
Repozytorium zawiera gotową paczkę `rpi-agent-deploy.tar.gz` (binarka + usługa systemd).

1. Pobierz i rozpakuj archiwum na Raspberry Pi.
2. Nadaj uprawnienia do wykonywania: `chmod +x agent`.
3. Dostosuj zmienne środowiskowe w pliku `k8s-mqtt-agent.service`.
4. Skopiuj plik usługi do `/etc/systemd/system/`.
5. Uruchom i włącz autostart: `sudo systemctl enable --now k8s-mqtt-agent`.