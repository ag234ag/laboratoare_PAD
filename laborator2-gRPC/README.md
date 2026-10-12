# Laborator 2 — Agent de mesagerie pe gRPC

Broker de mesaje (C#), publisher (Python) și subscriber (Java), care comunică prin gRPC
(HTTP/2 peste TCP). Contractul comun este [proto/broker.proto](proto/broker.proto).
Designul complet: [docs/specs/2026-10-12-grpc-broker-design.md](docs/specs/2026-10-12-grpc-broker-design.md).

## Cerințe

| Componentă | Ce trebuie instalat |
|---|---|
| Broker | .NET SDK 9 |
| Publisher | Python 3.10+ |
| Subscriber | JDK 11+ (Maven se descarcă automat prin `mvnw`) |

Fiecare componentă poate rula pe alt calculator. Pe calculatorul unde rulează doar un client
nu e nevoie de .NET.

## Pornire

Toate comenzile se rulează din folderul `laborator2-gRPC`.

### 1. Broker

```powershell
dotnet run --project broker -c Release -- --port 50051 --storage persistent --db broker.db --workers 4
```

- `--storage persistent`: mesajele se salvează în SQLite și supraviețuiesc repornirii.
- `--storage transient`: totul se ține în memorie (colecții concurente) și se pierde la oprire.

La pornire, brokerul afișează adresele la care se pot conecta celelalte calculatoare, de exemplu
`clients on other computers can connect to 192.168.1.15:50051  (Wi-Fi, default gateway)`.

### 2. Publisher (Python)

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r publisher/requirements.txt
.\.venv\Scripts\python publisher/publisher.py --host 192.168.1.15 --port 50051
```

### 3. Subscriber (Java)

```powershell
Push-Location subscriber
.\mvnw.cmd package
chcp 65001
java -jar target/subscriber.jar 192.168.1.15 50051
Pop-Location
```

`chcp 65001` comută consola Windows pe UTF-8. Subscriber-ul scrie mereu UTF-8, deci fără această comandă diacriticele apar ca caractere ilizibile (mesajul nu este alterat, doar afișarea).

La pornire, subscriber-ul cere ID-ul (Enter = `java-<numele calculatorului>`). Doi subscriberi pe **același**
calculator trebuie să introducă ID-uri diferite, altfel al doilea îl înlocuiește pe primul.

Pe Linux/macOS se folosește `./mvnw package`. După clonare poate fi nevoie de `chmod +x subscriber/mvnw`.

## Livrare și reîncercări

- Brokerul trimite fiecare mesaj subscriber-ilor abonați la topic și așteaptă ACK.
- Timeout ACK: 5 secunde. Scheduler-ul de reîncercări rulează la fiecare 1 secundă.
- Maximum 3 încercări de livrare. Dacă nu vine ACK după a treia, mesajul ajunge în Dead Letter Queue
  (aproximativ 15–18 secunde de la prima trimitere).
- Mesajele publicate fără topic sau fără conținut ajung direct în DLQ.
- Octeții corupți pe rețea sunt respinși de framework-ul gRPC înainte să ajungă la logica brokerului:
  clientul primește o eroare, brokerul continuă să ruleze și nu se salvează nimic. Mesajele invalide, dar
  bine formate, ajung în DLQ.

## Inspectarea stării

Până există GUI, DLQ-ul și starea nu se văd din clienții CLI. Variante:

- Liniile `[DLQ]` din consola brokerului.
- Metoda `GetState` (reflection este activat), de exemplu cu `grpcurl` (unealtă opțională):
  `grpcurl -plaintext <ip>:50051 pad.broker.v1.MessageBroker/GetState`
- În modul persistent, fișierul SQLite `broker.db` se poate deschide direct.

## Rulare pe mai multe calculatoare

1. Toate calculatoarele trebuie să fie în aceeași rețea (același Wi-Fi sau LAN).
2. Pe calculatorul cu brokerul, permite conexiunile venite pe port. PowerShell **ca administrator**:
   ```powershell
   New-NetFirewallRule -DisplayName "PAD gRPC Broker" -Direction Inbound -Protocol TCP -LocalPort 50051 -Action Allow
   ```
   Pentru ștergerea regulii: `Remove-NetFirewallRule -DisplayName "PAD gRPC Broker"`.
   Dacă la prima pornire s-a apăsat „Cancel" în fereastra Windows Firewall, Windows creează o regulă
   **Block** pentru `Broker.exe` (pornit de `dotnet run`) sau `dotnet.exe`, care are prioritate față de regula Allow.
   Verificare (Windows Defender Firewall > Inbound Rules, sau PowerShell):
   ```powershell
   Get-NetFirewallApplicationFilter | Where-Object { $_.Program -like '*Broker.exe' -or $_.Program -like '*dotnet.exe' } | Get-NetFirewallRule | Where-Object Action -eq Block
   ```
   Ștergere (PowerShell ca administrator): aceeași comandă urmată de `| Remove-NetFirewallRule`.
3. Pornește brokerul și notează adresa afișată (`192.168.x.x:50051`). Pe Windows, bannerul marchează
   adaptorul rețelei reale cu `default gateway` și îl listează primul. Adaptoarele virtuale
   (Hyper-V, VirtualBox, VMware) se ignoră.
4. Pe celelalte calculatoare pornește publisher-ul și subscriber-ul cu acea adresă.
5. Fiecare subscriber are nevoie de un ID unic. Implicit, ID-ul este `java-<numele calculatorului>`.

Wi-Fi-urile de campus (de exemplu eduroam) izolează adesea clienții între ei. În acest caz folosește
hotspot de pe telefon sau aceeași rețea LAN.

Diagnostic de pe un client: `Test-NetConnection <ip-broker> -Port 50051` (`TcpTestSucceeded : True` înseamnă că portul e accesibil).

Verificare rapidă de pe alt calculator: opțiunea `5 - Ping Broker` din publisher.

## Teste

```powershell
dotnet test broker.Tests
Push-Location subscriber; .\mvnw.cmd test; Pop-Location
.\.venv\Scripts\python -m pip install -r tests/e2e/requirements.txt
.\.venv\Scripts\python -m pytest tests/e2e -v
```

Testele E2E pornesc brokerul real pe porturi și baze de date temporare. Verificarea finală citește
SQLite direct. Testul de rețea se conectează prin IP-ul LAN al mașinii, nu prin `127.0.0.1`.
Testul de interoperabilitate rulează publisher-ul Python și subscriber-ul Java reale prin brokerul C#.
