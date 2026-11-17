# Benchmarki

| Plik | Co robi |
| --- | --- |
| `loadgen/` | wysyła zlecenia do dispatchera przez HTTP i podsłuchuje MQTT (oferty, wysłania zadania, ACK/RES), wynik każdego zadania zapisuje do CSV |
| `run_b1.sh` | B1 – zimny i ciepły start |
| `run_b2.sh` | B2 – czy funkcja celu wysyła zadania tam, gdzie jest obraz |
| `run_b3.sh` | B3 – wiele zadań naraz, `fallback` kontra `reauction` |
| `common.sh` | wspólne ustawienia skryptów |

## Po co te testy

| Test | Co sprawdza | Wynik (2× Raspberry Pi) |
| --- | --- | --- |
| B1 | ile kosztuje pobranie obrazu, czyli dlaczego opłaca się wysyłać zadanie tam, gdzie obraz już jest | zimny start rośnie z rozmiarem obrazu (od 7,5 s do 106 s dla 525 MB), ciepły trwa ok. 5 s |
| B2 | czy funkcja celu wysyła zadanie na węzeł, który ma już obraz | 86% wykonanych zadań trafiło na węzeł z obrazem (losowy wybór dałby ok. 50%), wykonane 80 z 90 zadań |
| B3 | czy przy przeciążeniu żaden klaster nie przyjmie więcej zadań, niż mieści, i która strategia szybciej przydziela lub odrzuca zadanie | maks. 1 zadanie naraz na klaster, `fallback` szybszy od `reauction` (ACK 1,2 s wobec 1,6 s, odrzucenie 1,9 s wobec 3,9 s) |

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

```
bash bench/run_b2.sh
```

Przed każdą serią skrypt usuwa obrazy `fn-*` z obu Raspberry, pobiera `fn-a` i `fn-b` na pierwsze, a `fn-c` i `fn-d` na drugie, i czeka minutę, aż Kubernetes zgłosi je w statusie węzłów. Potem wysyła 30 zadań (średnio co 5 s, popularniejsze obrazy częściej). Domyślnie 3 serie.

Każdy obraz jest tylko na jednym z dwóch węzłów, więc przy losowym wyborze węzła ok. 50% zadań trafiłoby na węzeł z obrazem. Wynik powyżej 50% oznacza, że funkcja celu bierze obraz pod uwagę. Gdy węzeł z obrazem jest zajęty, zadanie idzie na drugi i wtedy obraz musi się pobrać.

## B3 – wiele zadań naraz

```
bash bench/run_b3.sh
```

6 zadań wysłanych jednocześnie, każde potrzebuje 2400m CPU (60% jednego Raspberry), więc na klaster mieści się jedno. Skrypt uruchamia dispatchera ze strategią `fallback`, robi 3 serie, potem to samo dla `reauction`, a na koniec zostawia dispatchera z `fallback`.

Jeśli Raspberry mają inną liczbę rdzeni niż 4, zmień `CPU` (np. `CPU=1200 bash bench/run_b3.sh` dla 2 rdzeni).

## Wyniki

Skopiuj wyniki z VM na swój komputer:
```
scp main-kubernetes@192.168.1.120:~/bench-results/*.csv .
```

W B3 na jednym klastrze nie może działać naraz więcej niż jedno zadanie (kolumny `cluster`, `ack_at`, `done_at`). Inaczej klaster przyjął więcej, niż mieści.

## Kolumny CSV

| Kolumna | Znaczenie |
| --- | --- |
| `label` | etykieta serii (`cold`/`warm`, `b2`, nazwa strategii) |
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
