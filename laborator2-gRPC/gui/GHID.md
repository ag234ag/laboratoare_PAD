# Ghid: interfețele grafice pentru brokerul gRPC (Laborator 2)

Pentru: Adrian și colegii lui de la PAD (cine rulează, prezintă sau extinde laboratorul 2).

## 1. Ce s-a făcut

Brokerul, publisher-ul și subscriber-ul din laboratorul 2 (făcut de Anastasia) se foloseau doar din consolă. Am adăugat **trei aplicații grafice separate**, câte una pentru fiecare rol, care vorbesc cu brokerul **doar prin gRPC**, exact ca clienții din consolă:

| Aplicație | Pornire (din `laborator2-gRPC`) | Ce face |
|---|---|---|
| Broker | `python gui/broker_app.py` | Pornește și oprește brokerul C#, arată consola lui și tabelele Messages, Deliveries, DeadLetters, Subscriptions, abonații online (se reîmprospătează la 1 s prin `GetState`) |
| Publisher | `python gui/publisher_app.py --host IP --port 50051` | Publică mesaje, testează DLQ (fără topic / fără conținut) și idempotența (retrimite același `message_id`), istoric cu răspunsul brokerului |
| Receiver | `python gui/receiver_app.py --host IP --port 50051` | Se conectează cu un Subscriber ID, face Subscribe/Unsubscribe, ACK automat sau manual; vezi reîncercările 1, 2, 3 și apoi DLQ |

**Nu am modificat nimic** în `broker/`, `publisher/`, `subscriber/`, `proto/` și `tests/e2e/`. Interfețele sunt un strat nou, în folderul `gui/`. Comenzile detaliate și scenariul de demo sunt în `gui/README.md`.

## 2. Cum lucrăm cu aplicația

### 2.1 O singură dată, pe fiecare calculator
```powershell
cd laborator2-gRPC
python -m venv .venv
.\.venv\Scripts\python -m pip install -r publisher/requirements.txt
```
Pentru fereastra Broker este nevoie și de .NET SDK 9. Pentru calculatoarele care rulează doar Publisher sau Receiver nu este nevoie de .NET.

### 2.2 Pe un singur calculator (demo rapid)
1. `python gui/broker_app.py`, apoi **Start**. Așteaptă statusul `Running` (prima dată compilează, ~10-60 s).
2. `python gui/receiver_app.py`, apoi **Connect**, topic `sport`, **Subscribe**.
3. `python gui/publisher_app.py`, apoi **Connect**, topic `sport`, mesaj, **Publish**.
4. Mesajul apare în Receiver, iar în fereastra Broker apare ca `DELIVERED`.

### 2.3 Pe mai multe calculatoare (cum cere profesorul)
1. Toate în aceeași rețea (hotspot de telefon dacă rețeaua facultății izolează clienții).
2. Pe calculatorul cu brokerul: pornește fereastra Broker. În consolă apare adresa de forma `clients on other computers can connect to 192.168.x.x:50051`. Notează-o.
3. Pe același calculator, permite portul în firewall (PowerShell ca administrator): `New-NetFirewallRule -DisplayName "PAD gRPC Broker" -Direction Inbound -Protocol TCP -LocalPort 50051 -Action Allow`. Detalii: `laborator2-gRPC/README.md`, secțiunea „Rulare pe mai multe calculatoare”.
4. Pe celelalte calculatoare: `--host 192.168.x.x` la Publisher și Receiver.
5. **Fiecare Receiver are un ID unic.** Doi receiveri cu același ID se înlocuiesc: primul primește `SESSION_REPLACED`.

### 2.4 Scenariul de demo (în ordinea în care îl arăți profesorului)
1. Publish pe `sport` cu Receiver abonat: mesajul ajunge, `DELIVERED`.
2. Publish pe un topic fără abonat: mesajul stă `PENDING`. Apoi Subscribe: livrarea apare (backlog).
3. Debifează **ACK automat** și publică: încercările 1/3, 2/3, 3/3, apoi mesajul trece în `DeadLetters` (~15-18 s).
4. Butoanele **Fără topic** și **Fără conținut**: rând nou în DeadLetters, motiv `TOPIC_REQUIRED` / `CONTENT_REQUIRED`.
5. **Retrimite ultimul**: rezultat `DUPLICATE` (idempotență, nu se dublează livrarea).
6. Două Receiver cu ID-uri diferite pe același topic: fan-out (fiecare primește).
7. Două Receiver cu același ID: primul primește `SESSION_REPLACED`.
8. Brokerul pe `persistent`: oprește-l din fereastra Broker, pornește-l din nou: mesajele `PENDING` rămân (SQLite).

### 2.5 Testele
Din `laborator2-gRPC/gui`:
```powershell
..\.venv\Scripts\python -m unittest discover -s tests -t . -v      # 171 teste, ~45 s
..\.venv\Scripts\python -m unittest discover -s e2e -t . -v        # pornește brokerul REAL, ~40 s, doar Windows
```
Testul E2E compară ce arată interfața cu `broker.db`, citit direct din SQLite (prin `tests/e2e/oracle.py`), nu prin codul interfeței. Așa se verifică faptul că tabelele arată realitatea, nu doar ce crede aplicația.

### 2.6 Probleme frecvente
| Simptom | Cauză probabilă | Ce faci |
|---|---|---|
| „Broker indisponibil…” | broker oprit, host/port greșit sau firewall | `Test-NetConnection IP -Port 50051` |
| „Brokerul nu a raspuns in 5 secunde…” | pachetele sunt aruncate (firewall) sau IP greșit | regula de firewall din 2.3 |
| „Portul 50051 este ocupat” | alt broker rulează deja | oprește-l sau alege alt port în fereastra Broker |
| Receiver: `SESSION_REPLACED` | alt Receiver cu același ID | ID-uri diferite |
| Prima pornire lentă | compilează brokerul (.NET) | așteaptă `Running` |

## 3. Cum este construită (ca să o poți explica și extinde)

```
gui/
  broker_app.py  publisher_app.py  receiver_app.py     (3 ferestre, 3 puncte de intrare)
  core/        logica, fără Tk
    stubs.py            generează codul gRPC din proto/broker.proto (python)
    gateway.py          un canal gRPC + apelurile (Publish, Subscribe, Ack, Ping, GetState, Listen)
    listener.py         citește stream-ul Listen într-un thread
    state_poller.py     cheamă GetState la 1 s
    broker_process.py   pornește/oprește brokerul C# (tot arborele de procese)
    events.py           coadă de evenimente între thread-uri și fereastră
    ...                 modele mici: opțiuni broker, rânduri de stare, mesaje primite, reguli
  ui/widgets.py  widgeturi Tk (log, tabel, bara de conexiune), fără gRPC
  tests/  e2e/  README.md  GHID.md
```

Ideea centrală: **Tk nu se atinge din alte thread-uri**. Apelurile de rețea rulează în thread-uri, rezultatele se pun într-o coadă (`EventBus`), iar fereastra le citește la fiecare 100 ms (`TkPump`). Fiecare conexiune are propria identitate (gateway-ul), iar rezultatele vechi sunt ignorate (altfel un „Connect” lent de dinainte de un „Disconnect” ar corupe starea).

Principii: o clasă, o responsabilitate; dependențele se dau prin constructor (testăm cu un broker fals în proces și cu brokerul real); fără comentarii în cod.

## 4. Ce probleme au apărut (și cum au fost rezolvate)

Acestea sunt răspunsul sincer la „ce dificultăți au apărut”:
1. **Oprirea brokerului din interfață.** `dotnet run` pornește un proces copil (`Broker.exe`); dacă omori doar părintele, portul rămâne ocupat. Soluție: `taskkill /T /F` pe tot arborele, plus verificarea codului de ieșire.
2. **Starea „exited” nu trebuia să depindă de închiderea pipe-ului.** Un proces nepot poate ține pipe-ul deschis. Soluție: un thread separat așteaptă procesul.
3. **Curse între thread-uri.** Un stream `Listen` vechi putea posta un eveniment după `stop()`, sau își putea șterge succesorul. Soluție: lock-uri și etichetarea evenimentelor cu conexiunea de care aparțin.
4. **Teste care nu verificau nimic.** Mai multe teste erau „verzi” fără să fie legate de ce promiteau; au fost înlocuite cu teste care pică dacă scoți protecția respectivă (mutation check).
5. **Eroare reală la trunchierea logului** (păstra cu o linie mai puțin) și un handle de pipe nedeschis la fiecare pornire a brokerului.
6. **Diferența dintre „arată corect” și „este corect”:** verificarea finală compară interfața cu SQLite, citit direct.

## 5. Cum lucrăm în echipă (git)
- Lucrul e pe branch-ul `gui-grpc-lab2`. Fă `git fetch` și `git switch gui-grpc-lab2`. Nu lucra direct pe `main`.
- Cod nou: branch propriu din `gui-grpc-lab2`, apoi pull request.
- Nu modifica `proto/broker.proto` fără acordul tuturor: îl folosesc broker (C#), publisher (Python), subscriber (Java) și GUI. Dacă se schimbă, regenerează stub-urile (se face automat la următoarea pornire).
- Roluri la demo: o persoană Broker, una Publisher, una Receiver (sau mai mulți Receiveri cu ID-uri diferite).
