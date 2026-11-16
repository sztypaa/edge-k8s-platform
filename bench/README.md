# Benchmarki

| Plik | Co robi |
| --- | --- |
| `loadgen/` | wysyła zlecenia do dispatchera przez HTTP i podsłuchuje MQTT (oferty, wysłania zadania, ACK/RES), wynik każdego zadania zapisuje do CSV |
| `run_b1.sh` | B1 – zimny i ciepły start |
| `run_b2.sh` | B2 – czy funkcja celu wysyła zadania tam, gdzie jest obraz |
| `run_b3.sh` | B3 – wiele zadań naraz, `fallback` kontra `reauction` |
| `common.sh` | wspólne ustawienia skryptów |
| `plot.py` | tabele i wykresy z plików CSV |

Skrypty uruchamia się na VM. Obrazy testowe są na Docker Hubie (`sztypaa/rpi-task`, arm64):

| Obraz | Rozmiar | Test |
| --- | --- | --- |
| `light` | 80 KB | B3 |
| `size-50`, `size-100`, `size-250`, `size-500` | 52, 105, 262, 525 MB | B1 |
| `fn-a`, `fn-b`, `fn-c`, `fn-d` | po 105 MB, różne dane | B2 |

## Przygotowanie

1. Na swoim komputerze, z folderu nad `edge-k8s-platform`, wgraj kod na sprzęt:
   ```
   .\deploy_and_test.ps1 <hasło>
   ```
   Skrypt podmienia agenta na obu Raspberry, uruchamia dispatchera i puszcza testy. Wszystkie testy muszą przejść.
2. Zaloguj się na VM i przejdź do wgranej kopii:
   ```
   ssh main-kubernetes@192.168.1.120
   cd ~/deploy/edge-k8s-platform
   ```
3. Ustaw zmienne:
   ```
   export TASK_CMD="<krótka komenda programu z obrazu, ok. 1–2 s>"
   export B3_CMD="<ta sama komenda, ale działająca ok. 20 s>"
   export SSH_PASS="<hasło użytkownika pi>"
   ```
   `TASK_CMD` jest wymagane, bo przy pustej komendzie dispatcher wstawia `sh -c ...`, a w obrazach `rpi-task` nie ma `sh`. `SSH_PASS` jest potrzebne w B1 i B2 do usuwania obrazów z Raspberry.

Wyniki zapisują się w `~/bench-results` na VM (poza `~/deploy`, więc kolejne wgranie kodu ich nie kasuje). Wiersze są dopisywane, więc kolejne uruchomienia trafiają do tego samego pliku.

## B1 – zimny i ciepły start

```
bash bench/run_b1.sh
```

Dla każdego obrazu z B1 (i `light` jako punktu odniesienia) skrypt usuwa obraz z obu Raspberry, uruchamia zadanie (zimny start, obraz musi się pobrać), a potem to samo zadanie drugi raz (ciepły start). Domyślnie 5 powtórzeń (`REPEAT=5`).

Przy ciepłym starcie licytacja powinna wybrać klaster, który ma już obraz. Kolumna `image_cached` to pokazuje.

Limit wykonania zadania w systemie to 2 min (`DefaultExecutionTimeout`). Jeśli pobieranie `size-500` potrwa dłużej, zadanie skończy się statusem `Timeout` i w tabeli trafi do kolumny `failed`.

## B2 – funkcja celu

Przed każdą serią skrypt usuwa obrazy `fn-*` z obu Raspberry, pobiera `fn-a` i `fn-b` na pierwsze, a `fn-c` i `fn-d` na drugie, i czeka minutę, aż Kubernetes zgłosi je w statusie węzłów. Potem wysyła tę samą serię zadań (30 zadań średnio co 5 s, popularniejsze obrazy częściej). Kolumna `image_cached` pokazuje, czy zadanie trafiło na klaster z obrazem. Domyślnie 3 serie.

Nowa funkcja celu (obecny kod):
```
bash bench/run_b2.sh new
```

Stara funkcja celu (wolne CPU + 500 za obraz). Na swoim komputerze przywróć jej pliki, wgraj kod i uruchom B2:
```
git restore --source fc60c06 -- internal/model/model.go internal/k8s/metrics.go internal/agent/agent.go dispatcher/dispatcher.go tests/harness_test.go
.\deploy_and_test.ps1 <hasło>
```
na VM:
```
bash bench/run_b2.sh old
```
i z powrotem na swoim komputerze:
```
git restore --source HEAD -- internal/model/model.go internal/k8s/metrics.go internal/agent/agent.go dispatcher/dispatcher.go tests/harness_test.go
```

## B3 – wiele zadań naraz

```
bash bench/run_b3.sh
```

6 zadań wysłanych jednocześnie, każde potrzebuje 2400m CPU (60% jednego Raspberry), więc na klaster mieści się jedno. Skrypt uruchamia dispatchera ze strategią `fallback`, robi 3 serie, potem to samo dla `reauction`, a na koniec zostawia dispatchera z `fallback`.

Jeśli Raspberry mają inną liczbę rdzeni niż 4, zmień `CPU` (np. `CPU=1200 bash bench/run_b3.sh` dla 2 rdzeni).

## Wykresy

Skopiuj wyniki z VM na swój komputer:
```
scp main-kubernetes@192.168.1.120:~/bench-results/*.csv .
```
i uruchom:
```
pip install pandas matplotlib tabulate
python bench/plot.py b1 b1.csv --outdir wyniki-b1
python bench/plot.py b2 b2.csv --outdir wyniki-b2
python bench/plot.py b3 b3.csv --outdir wyniki-b3
```

| Test | Tabela | Wykres |
| --- | --- | --- |
| B1 | średni czas działania poda przy zimnym i ciepłym starcie dla każdego obrazu, różnica = czas pobrania | `b1_start.png` |
| B2 | średni czas, mediana, p95, odrzucone zadania, % ciepłych startów | `b2_funkcja_celu.png` |
| B3 | wykonane i odrzucone zadania, rezygnacje (RES), wysłania na zadanie, czas do ACK, czas do odrzucenia zadania, najwięcej zadań naraz na jednym klastrze | `b3_strategie.png` |

W B3 kolumna `max_tasks_on_one_cluster` musi wynosić 1. Większa wartość oznacza, że klaster przyjął więcej, niż mieści.

## Kolumny CSV

| Kolumna | Znaczenie |
| --- | --- |
| `label` | etykieta serii (`cold`/`warm`, `new`/`old`, nazwa strategii) |
| `total_ms` | czas od wysłania zlecenia do odpowiedzi dispatchera |
| `ack_ms` | czas od wysłania zlecenia do ACK klastra, który wykonał zadanie |
| `pod_ms` | czas działania poda według agenta, razem z pobraniem obrazu |
| `status` | `Succeeded`, `Failed`, `Timeout`, `FailedToCreate`; puste, gdy dispatcher odrzucił zadanie |
| `cluster` | klaster, który wykonał zadanie |
| `image_cached` | czy ten klaster miał obraz w chwili składania oferty |
| `bids` | liczba ofert (przy `reauction` ze wszystkich licytacji tego zadania) |
| `dispatches` | ile razy zadanie zostało wysłane do klastra |
| `resignations` | ile razy klaster odpowiedział RES |
| `error` | treść błędu, gdy zadanie zostało odrzucone |
