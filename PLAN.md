# Edge K8s Platform – plan działań

## O co chodzi

Mamy kilka klastrów edge (K3s na Raspberry Pi i maszynie wirtualnej), które komunikują się ze sobą przez MQTT. Na nich implementujemy ClusterLess (Farahani et al.): uruchamianie workflowów z funkcji serverless tak, żeby zdążyć przed deadline'em.

Naszą zmianą względem oryginału jest licytacja zamiast super-mastera. W ClusterLessie o tym, gdzie przenieść zadanie, decyduje jeden wybrany klaster. U nas klastry same licytują się o zadanie (pomysł z Caraveli).

## Środowisko

| Maszyna | Adres | Rola |
| --- | --- | --- |
| rpi1 (raspberry) | 192.168.1.100 | klaster K3s + master ClusterLessa |
| rpi2 (raspberrypi) | 192.168.1.110 | klaster K3s + master ClusterLessa |
| vm (main-kubernetes) | 192.168.1.120 | broker RabbitMQ (MQTT 1883, panel 15672), później może też klaster i registry obrazów |

Każdy klaster ma swojego mastera, a klastry rozmawiają ze sobą wyłącznie przez broker na VM. Wszystko działa w sieci lokalnej. Hasła trzymamy poza repozytorium.


## Podział pracy

| Część | Zakres |
| --- | --- |
| Agent | wysyła zdarzenia z Kubernetesa na MQTT (`k8s/<klaster>/control-plane/events`) |
| Scheduler | własny scheduler na węźle, współpracuje z masterem |
| FaaS | uruchamianie funkcji w podach, porównanie z Knative |
| Broker | RabbitMQ z MQTT, klient do podglądu komunikatów, registry obrazów |

---

## 1. Licytacja

Jeśli klaster nie zdąży wykonać kroku sam, pyta pozostałe klastry, które z nich go wezmą. Wygrywa ten, kto ma już uruchomioną funkcję (warm). Jeśli takich jest kilku, wygrywa ten, kto skończy najszybciej, a przy remisie ten z największą liczbą wolnych zasobów.

**Jak to przebiega:**

1. Master klastra A sprawdza, czy zdąży wykonać krok u siebie. Jeśli nie, ogłasza licytację na `clusterless/v1/bid/request`.
2. Pozostałe klastry liczą, kiedy mogłyby skończyć (czas przesłania danych + czas wykonania). Odpowiadają tylko wtedy, gdy zdążą przed deadline'em i będą szybsze niż A.
3. Oferty trafiają do A na `clusterless/v1/bid/offer/<A>`: czas zakończenia, tryb (warm / warm scaling / cold) i liczba wolnych slotów.
4. A zbiera oferty przez 200 ms i wybiera zwycięzcę.
5. A wysyła zadanie na `clusterless/v1/dispatch/<zwycięzca>`, a wynik wraca na `clusterless/v1/dispatch/result/<A>`.
6. Jeśli nikt się nie zgłosi, A wykonuje krok u siebie, nawet ze spóźnieniem, i zapisuje to w wynikach.

**Tematy MQTT:**

| Temat | Kto wysyła | Co zawiera |
| --- | --- | --- |
| `bid/request` | klaster, który szuka wykonawcy | funkcja, deadline, kiedy będą gotowe dane, rozmiar danych |
| `bid/offer/<A>` | klastry, które mogą wziąć zadanie | czas zakończenia, tryb, wolne sloty |
| `dispatch/<cel>` | klaster zlecający | krok workflowu i dane wejściowe |
| `dispatch/result/<A>` | wykonawca | wynik i czasy wykonania |
| `status/<klaster>` | każdy klaster co sekundę | czy działa, obciążenie, dostępne funkcje |

Wszystko idzie z QoS 1. Jeśli klaster padnie, broker sam ogłosi, że jest niedostępny (Last Will).

**Czego świadomie nie robimy:** przegrani nie dostają informacji, że przegrali, okno licytacji ma zawsze 200 ms i nie ma globalnej kolejki w centrali. W pracy opiszemy to jako ograniczenia.

## 2. FaaS

Funkcje uruchamia nasz master, a nie gotowa platforma. Knative, OpenFaaS i podobne same decydują, gdzie i jak startuje funkcja, a w ClusterLessie właśnie o tym ma decydować nasz algorytm. Knative używamy tylko do porównania wyników.

| Rozwiązanie | Czy wybieramy węzeł i tryb | Workflow (DAG) | Na RPi | U nas |
| --- | --- | --- | --- | --- |
| Nasz master | tak | tak | lekki | główne rozwiązanie |
| Knative | nie | nie | ciężki | porównanie |
| OpenFaaS / faasd | nie | nie | lekki | tylko w przeglądzie |
| Fission | częściowo | nie | średni | tylko w przeglądzie |

**Jak wygląda funkcja:**
- To obraz kontenera z serwerem HTTP na porcie 8080: `POST /invoke` wykonuje funkcję, a `GET /healthz` mówi, czy pod jest gotowy.
- Jeden pod obsługuje jedno wywołanie naraz, więc master zawsze wie, które pody są zajęte.
- Dla każdej funkcji na każdym węźle jest osobny Deployment. Dodanie poda to zwiększenie `replicas`.
- Lista funkcji jest w `deploy/functions.json`.
- Do testów służy funkcja `fn-sim`, która obciąża CPU i ma ustawiany czas startu. Dzięki temu łatwo zobaczyć różnicę między startem na zimno a na ciepło.

**Obrazy:** stawiamy własne registry na VM, żeby Raspberry pobierały obrazy przez sieć lokalną. Pobieranie jest wtedy szybsze, nie ma limitów Docker Hub, a czasy da się powtarzalnie mierzyć.

```bash
# na VM
docker run -d -p 5000:5000 --restart always registry:2

# budowanie obrazu na ARM i x86
docker buildx build --platform linux/arm64,linux/amd64 -t 192.168.1.120:5000/fn-sim:1 --push functions/fn-sim
```

Na każdym Raspberry trzeba wpisać to registry w `/etc/rancher/k3s/registries.yaml` i zrestartować K3s.

**Porównanie z Knative:** ten sam obraz `fn-sim`, jedno wywołanie na pod, to samo obciążenie. Porównujemy, ile workflowów zdążyło przed deadline'em i ile trwa start na zimno.

**Opcjonalnie:** porównać start na zimno kontenera i WebAssembly. Najpierw trzeba sprawdzić, czy nasza wersja K3s to obsługuje.

## 3. Tryby uruchamiania

Dla każdego kroku master wybiera najszybszą opcję, która zdąży przed deadline'em:

| Tryb | Kiedy | Co się dzieje | Ile trwa |
| --- | --- | --- | --- |
| Warm execution | jest wolny, uruchomiony pod funkcji | od razu wywołanie funkcji | tylko wykonanie |
| Warm scaling | nie ma wolnego poda, ale obraz jest na węźle | dodanie poda | start kontenera, ok. 2 s (do zmierzenia) |
| Cold scaling | obrazu nie ma na węźle | pobranie obrazu i dodanie poda | pobranie + start, ok. 20 s (do zmierzenia) |
| Offload | żaden tryb lokalny nie zdąży | licytacja (punkt 1) | przesłanie danych + wykonanie u innego klastra |

Nieużywany pod żyje jeszcze 120 s, potem jest usuwany. Następne wywołanie tej funkcji to wtedy warm scaling albo cold.

**Skąd master wie, co się dzieje na klastrze:**

| Co | Skąd |
| --- | --- |
| czy obraz jest na węźle | status węzła w Kubernetesie |
| wolne CPU i RAM | zasoby węzła minus to, co zajmują pody |
| które pody są gotowe | status podów w Kubernetesie |
| które pody są zajęte | master sam wysyła wywołania, więc to wie |
| szybkość węzła | etykieta na węźle (`clusterless.io/speed`) |
| ile naprawdę trwają tryby | średnia z poprzednich wywołań, aktualizowana na bieżąco |

**Do poprawy:**
- Przy usuwaniu nieużywanych podów Kubernetes może skasować akurat zajęty pod. Rozwiązanie: oznaczyć wolny pod adnotacją `controller.kubernetes.io/pod-deletion-cost: "-100"`.
- Zdarzenia z agenta (pobieranie obrazu, start kontenera) mogą poprawić szacowanie czasu startu na zimno.

---

## Eksperymenty

- Porównujemy strategie przenoszenia zadań: brak, po kolei (round-robin), losowo, super-master i licytacja.
- Mierzymy, ile workflowów zdążyło przed deadline'em, jak długo trwa decyzja i ile wiadomości MQTT idzie na jedno przeniesienie zadania.
- Test awarii: wyłączamy klaster, który jest super-masterem. Przy super-masterze przenoszenie zadań stoi do wyboru nowego (do 5 s), przy licytacji działa dalej.

## Do ustalenia

- Kto wybiera węzeł dla poda: master czy nasz scheduler? Propozycja: master wybiera, scheduler sprawdza i przypisuje.
- Czy VM ma być trzecim klastrem? Tak, jeśli postawimy na niej K3s. Jeśli nie, zostaje tylko broker i registry.

## Do zrobienia

- [ ] Zmienić hasła do RabbitMQ i maszyn, usunąć je z plików w repo
- [ ] Wrzucić kod mastera ClusterLessa do repo
- [ ] Postawić registry na VM i skonfigurować je na Raspberry
- [ ] Uruchomić mastera na rpi1 i rpi2
- [ ] Ustalić współpracę mastera ze schedulerem
- [ ] Przygotować porównanie z Knative
- [ ] Zmierzyć czasy trybów warm / warm scaling / cold na RPi i VM
- [ ] Przeprowadzić eksperymenty

## Przydatne komendy

Pod testowy na Raspberry:
```bash
KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl run test-pod --image=nginx
kubectl delete pod test-pod
```

Pod testowy z VM (w `~/.kube/config-rpi2` musi być adres rpi2, nie `127.0.0.1`):
```bash
KUBECONFIG=~/.kube/config-rpi2 kubectl run test-pod --image=nginx
```

Podgląd wiadomości MQTT na VM:
```bash
read -s MQTT_PASS
mosquitto_sub -h 127.0.0.1 -p 1883 -u "$MQTT_USER" -P "$MQTT_PASS" -t "k8s/#" -v
mosquitto_sub -h 127.0.0.1 -p 1883 -u "$MQTT_USER" -P "$MQTT_PASS" -t "clusterless/v1/#" -v
```

Restart agenta (usługa: `/etc/systemd/system/k8s-mqtt-agent.service`):
```bash
sudo systemctl daemon-reload
sudo systemctl restart k8s-mqtt-agent.service
journalctl -u k8s-mqtt-agent -f
```