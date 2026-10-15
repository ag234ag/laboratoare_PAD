# Interfețe grafice pentru brokerul gRPC (Laborator 2)

Trei aplicații Tkinter separate: **Broker**, **Publisher**, **Receiver**. Cer același mediu ca publisher-ul (Python 3.10+, `grpcio`, `grpcio-tools`) și, pentru fereastra Broker, .NET SDK 9.

Din `laborator2-gRPC`:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r publisher/requirements.txt
.\.venv\Scripts\python gui/broker_app.py
.\.venv\Scripts\python gui/publisher_app.py --host 192.168.1.15 --port 50051
.\.venv\Scripts\python gui/receiver_app.py --host 192.168.1.15 --port 50051
```

`--host` implicit `127.0.0.1`, `--port` implicit `50051`. Fiecare aplicație poate rula pe alt calculator; stub-urile gRPC se generează automat în `gui/generated` din `proto/broker.proto`.

Pentru conectarea de pe alt calculator, folosește adresele LAN afișate de broker la pornire („clients on other computers can connect to ...") și regula de firewall din secțiunea „Rulare pe mai multe calculatoare" din `laborator2-gRPC/README.md`.

## Broker
Câmpuri port, storage, db, workers. **Start** rulează `dotnet run --project broker -c Release -- ...` (prima pornire compilează). Consola brokerului apare în fereastră; după `Ping` statusul devine `Running` și tabelele Messages, Deliveries, DeadLetters, Subscriptions se reîmprospătează la 1 s prin `GetState`. **Stop** (sau închiderea ferestrei) oprește tot arborele de procese.

## Publisher
Conectare, Topic + Message, **Publish**. Butoanele DLQ trimit mesaj fără topic sau fără conținut. **Retrimite ultimul** reutilizează messageId-ul și arată `DUPLICATE`. Istoricul arată răspunsul brokerului pentru fiecare trimitere.

## Receiver
Conectare cu un Subscriber ID unic (deschide stream-ul `Listen`), apoi Subscribe/Unsubscribe pe topicuri. **ACK automat** bifat confirmă fiecare mesaj; debifat, **Trimite ACK** confirmă mesajul selectat. Fără ACK, brokerul retrimite (încercarea 1, 2, 3) și apoi mută mesajul în DLQ. Retransmisiile actualizează același rând.

## Demo
1. Broker: Start. 2. Receiver: Connect, Subscribe `sport`. 3. Publisher: Connect, Publish pe `sport`: rândul apare în Receiver, tabelul Deliveries arată `DELIVERED`. 4. Publish pe un topic fără abonat, apoi Subscribe: livrarea apare. 5. Debifează ACK automat și publică: încercările 1, 2, 3, apoi DeadLetters. 6. Butoanele DLQ ale Publisher-ului: rând în DeadLetters. 7. Două Receiver cu ID-uri diferite pe același topic: fan-out. 8. Doi Receiver cu același ID: primul primește `SESSION_REPLACED`.

## Teste
Din `gui/`:
```powershell
..\.venv\Scripts\python -m unittest discover -s tests -t . -v
..\.venv\Scripts\python -m unittest discover -s e2e -t . -v
```
Testele E2E pornesc brokerul real și compară tabelele afișate cu `broker.db`, citit direct prin `tests/e2e/oracle.py`. Verifică și ciclul Start, Stop, Start pe același port și oprirea brokerului din afara aplicației (arborele de procese și portul se eliberează). Testul E2E (`gui/e2e`) rulează doar pe Windows, fiindcă folosește `taskkill` și tabela de procese Win32; pe alte sisteme este sărit.
