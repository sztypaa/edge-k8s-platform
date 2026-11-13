# Edge K8s Platform – plan działań

## Cel

System rozdziela zadania między klastry Kubernetes na Raspberry Pi. Zadanie to uruchomienie kontenera z wybranego obrazu. Klastry licytują się o każde zadanie, a licytator przy brokerze MQTT na maszynie wirtualnej przydziela je najlepszej ofercie.

Rozwiązanie opiera się na dwóch pracach:

- ClusterLess (Farahani et al.) – przenoszenie zadań między klastrami brzegowymi,
- Caravela – przydział zasobów przez licytację.

## Środowisko

| Maszyna | Adres | Rola |
| --- | --- | --- |
| rpi1 (raspberry) | 192.168.1.100 | klaster K3s |
| rpi2 (raspberrypi) | 192.168.1.110 | klaster K3s |
| vm (main-kubernetes) | 192.168.1.120 | broker RabbitMQ (MQTT 1883, panel 15672) i licytator – główny węzeł komunikacji |

Klastry komunikują się wyłącznie przez broker na VM. Wszystko działa w sieci lokalnej.

## Z czego składa się system

```
            zadanie (obraz + CPU/RAM)
                      │
                      ▼
        ┌───────────────────────────┐
        │ VM: broker MQTT           │
        │ + licytator (rozstrzyga)  │
        └───────────────────────────┘
          ▲   │               ▲   │
   oferta │   │ przydział     │   │
   status │   ▼               │   ▼
   ┌──────────────┐     ┌──────────────┐
   │ rpi1 (K3s)   │     │ rpi2 (K3s)   │
   │ agent        │     │ agent        │
   │ scheduler    │     │ scheduler    │
   │ uruchamianie │     │ uruchamianie │
   │ podów        │     │ podów        │
   └──────────────┘     └──────────────┘
```

| Element | Gdzie działa | Co robi |
| --- | --- | --- |
| Broker MQTT | VM | przekazuje wszystkie wiadomości między klastrami |
| Licytator | VM, obok brokera | osobny program, który zbiera oferty od klastrów i wybiera zwycięzcę |
| Agent | każdy klaster | wysyła zdarzenia z Kubernetesa na `k8s/<klaster>/control-plane/events` – już działa |
| Obsługa zadań | każdy klaster | odpowiada na licytacje, uruchamia pod z obrazem zadania, po zakończeniu go usuwa |
| Scheduler | każdy klaster | umieszcza pod na węźle klastra |

## Jak przebiega licytacja

1. Ktoś zgłasza zadanie na `tasks/new`: nazwa obrazu, ile potrzebuje CPU i RAM.
2. Licytator wysyła do wszystkich klastrów zapytanie `auction/request`.
3. Każdy klaster sprawdza w Kubernetesie, ile ma wolnych zasobów i czy obraz jest już na węźle. Jeśli ma wystarczająco zasobów, odpowiada ofertą na `auction/offer`.
4. Licytator zbiera oferty przez krótką chwilę (np. 500 ms) i wybiera zwycięzcę:
   - najpierw klaster, który ma już obraz,
   - potem ten, który ma najwięcej wolnych zasobów.
5. Licytator wysyła zadanie do zwycięzcy na `tasks/<klaster>`.
6. Klaster uruchamia pod. Po zakończeniu wysyła wynik (status i logi) na `tasks/result` i usuwa pod.
7. Gdy nie ma żadnej oferty, licytator po chwili powtarza licytację.

**Tematy MQTT:**

| Temat | Kto wysyła | Co zawiera |
| --- | --- | --- |
| `tasks/new` | zgłaszający zadanie | id zadania, obraz, wymagane CPU i RAM |
| `auction/request` | licytator | id zadania, obraz, wymagane CPU i RAM |
| `auction/offer` | klastry | id zadania, klaster, wolne CPU i RAM, czy obraz jest na węźle |
| `tasks/<klaster>` | licytator | zadanie dla zwycięskiego klastra |
| `tasks/result` | klaster | id zadania, status (sukces / błąd), logi |
| `clusters/<klaster>/status` | każdy klaster co kilka sekund | czy działa, wolne zasoby |

Licytator na podstawie statusów wie, które klastry działają. Klaster, od którego dłużej nie przyszedł status, jest pomijany w licytacjach.

## Uruchamianie zadań

- Zadanie to dowolny obraz kontenera.
- Klaster uruchamia go jako pod (np. Kubernetes Job) i usuwa po zakończeniu.
- Pobrane obrazy zostają w pamięci podręcznej na Raspberry, więc kolejne uruchomienie tego samego obrazu jest szybsze.

Przy uruchomieniu są dwie sytuacje:

| Sytuacja | Co się dzieje |
| --- | --- |
| obraz jest już na węźle | od razu start poda |
| obrazu nie ma | najpierw pobranie obrazu, potem start poda |

**Skąd klaster bierze dane do oferty:**

| Co | Skąd |
| --- | --- |
| wolne CPU i RAM | Kubernetes (K3s ma wbudowany metrics-server, to samo co pokazuje `kubectl top node`) |
| czy obraz jest na węźle | status węzła w Kubernetesie |
| czy pod się uruchomił i zakończył | status poda w Kubernetesie / zdarzenia z agenta |

## Do ustalenia

- Czy licytację prowadzi licytator przy brokerze, czy klastry między sobą?
- Jak dokładnie ma wyglądać zadanie (funkcja): sam obraz, czy też dane wejściowe i wynik?
- Jaką rolę ma scheduler, skoro każde Raspberry to klaster z jednym węzłem?
- Czy i jak wykorzystujemy Knative?
- Co pokazujemy jako wynik pracy (np. zadania trafiają tam, gdzie jest obraz lub więcej zasobów; po wyłączeniu jednego klastra zadania idą na drugi)?
- Jak często klastry wysyłają status?

## Przydatne komendy

Pod testowy na Raspberry:
```bash
KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl run test-pod --image=nginx
KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl delete pod test-pod
```

Pod testowy z VM (w `~/.kube/config-rpi2` musi być adres rpi2, nie `127.0.0.1`):
```bash
KUBECONFIG=~/.kube/config-rpi2 kubectl run test-pod --image=nginx
KUBECONFIG=~/.kube/config-rpi2 kubectl delete pod test-pod
```

Podgląd wiadomości MQTT na VM:
```bash
read -p "Użytkownik: " MQTT_USER
read -s -p "Hasło: " MQTT_PASS
mosquitto_sub -h 127.0.0.1 -p 1883 -u "$MQTT_USER" -P "$MQTT_PASS" -t "k8s/#" -v
mosquitto_sub -h 127.0.0.1 -p 1883 -u "$MQTT_USER" -P "$MQTT_PASS" -t "#" -v
```

Restart agenta (usługa: `/etc/systemd/system/k8s-mqtt-agent.service`):
```bash
sudo systemctl daemon-reload
sudo systemctl restart k8s-mqtt-agent.service
journalctl -u k8s-mqtt-agent -f
```