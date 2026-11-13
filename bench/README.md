# Benchmarki

Narzędzia do pomiarów w pracy:

- `edge-task/` – obraz testowy z kontrolowanym obciążeniem CPU i pamięci (wersja `light` i `heavy`),
- `loadgen/` – generator zleceń do dispatchera, zapisuje wyniki do CSV,
- `plot.py` – tabela i wykresy z plików CSV.

Dispatcher ma oprócz strategii licytacji (`fallback`, `reauction`) dwie proste strategie bez licytacji (`roundrobin`, `random`), żeby było z czym porównać.

## 1. Obraz `edge-task`

Program wykonuje to, co dostanie w argumentach:

| Argument | Działanie |
| --- | --- |
| `--cpu 2s` | obciąża CPU przez 2 s |
| `--threads 2` | liczba wątków obciążających CPU (domyślnie 1) |
| `--mem 128` | zajmuje 128 MiB pamięci |
| `--sleep 5s` | czeka 5 s |
| `--exit 1` | kończy się kodem 1 |
| `--out tekst` | wypisuje tekst |

Wersja `heavy` ma dodatkowo ok. 250 MB danych w obrazie, żeby pobieranie obrazu (zimny start) trwało wyraźnie dłużej.

Budowanie (Docker Desktop albo Docker z buildx, obraz na ARM64 i AMD64):

```bash
cd bench/edge-task
docker login
docker buildx build --platform linux/arm64,linux/amd64 --target light -t <login>/edge-task:light --push .
docker buildx build --platform linux/arm64,linux/amd64 --target heavy -t <login>/edge-task:heavy --push .
```

W zleceniu komenda musi zaczynać się od `/edge-task`, bo pole `command` zastępuje domyślną komendę obrazu:

```json
{"image":"<login>/edge-task:light","command":["/edge-task","--cpu","2s","--mem","64"],"min_cpu_milli":1000,"min_mem_mib":128}
```

## 2. Strategie w dispatcherze

| `DISPATCHER_STRATEGY` | Działanie |
| --- | --- |
| `fallback` (domyślnie) | licytacja, po rezygnacji (`RES`) następny klaster z rankingu |
| `reauction` | licytacja, po rezygnacji nowa licytacja |
| `roundrobin` | bez licytacji, klastry z `CLUSTERS` po kolei |
| `random` | bez licytacji, klastry z `CLUSTERS` w losowej kolejności |

Przy `roundrobin` i `random` klaster też może odesłać `RES`, wtedy zadanie idzie do następnego z listy. Różnica względem licytacji jest więc tylko w tym, że kolejność klastrów nie zależy od `score`.

```bash
DISPATCHER_STRATEGY=roundrobin CLUSTERS=rpi-1,rpi-2 MQTT_BROKER=tcp://127.0.0.1:1883 ./dispatcher
```

Wynik zadania zawiera dodatkowo `image_cached` (czy obraz był na węźle w chwili przyjęcia zadania).

## 3. Generator `loadgen`

```bash
go build -o loadgen ./bench/loadgen
./loadgen -dispatcher http://192.168.1.120:8080 -tasks 30 -rate 0.5 \
  -images "<login>/edge-task:light,<login>/edge-task:heavy,busybox:latest" \
  -cmd "/edge-task --cpu 3s" -cpu 1500 -mem 128 -seed 7 -label fallback -out b2.csv
```

| Flaga | Znaczenie |
| --- | --- |
| `-tasks` | liczba zleceń |
| `-rate` | średnio ile zleceń na sekundę (odstępy losowane jak w procesie Poissona) |
| `-burst` | wszystkie zlecenia naraz |
| `-images` | lista obrazów, pierwszy jest najpopularniejszy (rozkład Zipfa) |
| `-zipf` | wykładnik rozkładu Zipfa (domyślnie 0.75, jak w ClusterLess) |
| `-cmd` | komenda w kontenerze, dzielona po spacjach (bez cudzysłowów w środku) |
| `-cpu`, `-mem` | wymagania zadania (`min_cpu_milli`, `min_mem_mib`) |
| `-seed` | ziarno losowania – ten sam seed daje tę samą sekwencję zadań |
| `-label` | etykieta w CSV, np. nazwa strategii |
| `-out` | plik CSV, wiersze są dopisywane |

Kolumny CSV: `label, task_id, image, submitted_at, total_ms, dispatch_ms, pod_ms, http_status, cluster, status, image_cached, error`.

`pod_ms` to czas od utworzenia poda do jego zakończenia (z agenta), a `dispatch_ms = total_ms - pod_ms`, czyli licytacja, potwierdzenie i przesył wiadomości.

## 4. Benchmarki

Każdy pomiar powtórzyć 3–5 razy (różne `-seed`), wyniki dopisują się do tego samego pliku.

### B1 – zimny i ciepły start

Przed każdym zimnym startem usunąć obraz z obu Raspberry:

```bash
sudo k3s crictl rmi docker.io/<login>/edge-task:heavy
```

Potem na VM:

```bash
./loadgen -tasks 1 -images "<login>/edge-task:heavy" -cmd "/edge-task --cpu 2s" -cpu 500 -label cold -out b1.csv
./loadgen -tasks 1 -images "<login>/edge-task:heavy" -cmd "/edge-task --cpu 2s" -cpu 500 -label warm -out b1.csv
```

### B2 – licytacja a proste strategie

Ten sam `-seed` dla każdej strategii. Dla każdej strategii uruchomić dispatcher z odpowiednim `DISPATCHER_STRATEGY`:

```bash
./loadgen -tasks 30 -rate 0.5 -images "<login>/edge-task:light,<login>/edge-task:heavy,busybox:latest" \
  -cmd "/edge-task --cpu 3s" -cpu 1500 -mem 128 -seed 7 -label fallback -out b2.csv
```

i powtórzyć z `-label reauction`, `-label roundrobin` oraz `-label random`.

### B3 – równoległe licytacje

Wszystkie zadania naraz, każde potrzebuje ok. 60% CPU jednego Raspberry:

```bash
./loadgen -tasks 6 -burst -images "<login>/edge-task:light" -cmd "/edge-task --sleep 20s" -cpu 2400 -label fallback -out b3.csv
```

i to samo dla `reauction` i `roundrobin`. Tu widać, ile zadań zostało odrzuconych i czy żaden klaster nie dostał więcej, niż mieści (rezygnacje `RES` są w logach dispatchera).

## 5. Wykresy

```bash
pip install pandas matplotlib
python3 bench/plot.py b2.csv --outdir wyniki-b2
```

Powstaje tabela (średnia, odchylenie, mediana, p95, liczba odrzuconych zadań, zimne starty) w `podsumowanie.csv` oraz wykresy `czas_wykonania.png` i `rozklad_klastry.png`.

Czasy liczone są tylko dla zadań zakończonych sukcesem, dlatego zawsze trzeba patrzeć też na kolumnę `rejected_or_failed`. Zadanie jest odrzucane (HTTP 503), gdy żaden klaster nie ma na nie miejsca.
