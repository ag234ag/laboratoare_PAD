# Laborator 2 — Agent de mesagerie pe gRPC (design)

Data: 2026-10-12
Status: aprobat și implementat (corecturi din execuție: `RetryInterval=1s`, §4)

## 1. Scop

Partea 2 a lucrării „Agent de mesagerie. Invocarea la distanță”: același sistem ca în
`laborator1/` (Partea 1, socket-uri TCP + JSON), reimplementat cu invocări la distanță (gRPC) și
date reprezentate în Protocol Buffers. Arhitectura rămâne aceeași; se schimbă implementarea
comunicației. Interfața grafică NU face parte din acest pas, dar backend-ul expune starea printr-un
RPC (§3) ca GUI-ul ulterior să nu depindă de fișierul bazei de date.

### 1.1 Trasabilitatea cerințelor

| # | Cerință (enunț) | Unde e acoperită |
|---|---|---|
| P1.1.1 | Formatul mesajelor (P2: proto în loc de XML/JSON) | §3 |
| P1.1.2 | Numărul de canale unidirecționale, argumentat | §2.2 |
| P1.1.3 | Structura comunicației (unul-la-unu) | §2.3 |
| P1.1.4 | Politici de livrare (mesaje invalide, căderea agentului etc.) | §2.4 |
| P1.1 | Rutare după tipul mesajului; excepții tratate fără căderea agentului | §4.3, §4.6 |
| P1.2.1 | Protocolul de transport, argumentat (TCP/UDP) | §2.1 |
| P1.2.2 | Tratare concurentă a cererilor; set de work joburi care livrează din storage | §4.4 |
| P1.3.1 | Stocare transientă în colecții concurente | §4.2 |
| P1.3.2 | Stocare persistentă cu procesare asincronă | §4.2 |
| P1.3.3 | Nivel abstract de rutare | §4.3 |
| P1.PS | Șablonul Publisher/Subscriber, rutare pe subiect (topic) | §4.3 |
| P2.1 | RPC în loc de socket-uri | gRPC, tot documentul |
| P2.2 | Reprezentarea datelor: proto | §3 |
| P2.3 | Validarea datelor și procesarea concurentă | §5, §4.4 |
| Lab 1 | 3 limbaje: broker C#, publisher Python, subscriber Java | §4, §7, §8 |
| Lab 1 | Persistență, ACK, retry, Dead Letter Queue | §4.2, §4.5 |
| Enunț | „Mesajul nu trebuie să fie alterat în timpul transmiterii” | §9.3 scenariul 9 |
| Cerință suplimentară | Broker folosit ca microserviciu de pe calculatoare cu IP-uri diferite | §2.5, §9.3 scenariile 12–13 |

### 1.2 Criteriu de „gata”

Toate scenariile din §9 trec end-to-end pe procese reale, în ambele moduri de stocare unde se
aplică, iar oracolul independent (§9.3) confirmă direct din SQLite că fiecare mesaj și fiecare
livrare se află într-o stare cu nume, cu sumele egale cu totalurile.

## 2. Protocolul de lucru al agentului

### 2.1 Transport: TCP (prin HTTP/2)

gRPC rulează peste HTTP/2, care rulează peste TCP. Alegerea e și argumentată, nu doar impusă:
- livrarea are nevoie de ordine, integritate și confirmare. TCP le oferă la nivel de transport;
  peste UDP ar trebui reimplementate (numerotare, retransmisie, reasamblare pentru mesaje mari);
- conexiunea persistentă permite brokerului să împingă mesaje spre subscriber (server streaming)
  fără polling, deci fără presiunea de rețea descrisă în enunț pentru „message pooling”;
- latența suplimentară a TCP față de UDP nu contează pentru un broker cu ACK, unde oricum se
  așteaptă confirmarea aplicației.

### 2.2 Canale (fix, independent de numărul de tipuri de mesaje)

| Participant | Conexiuni TCP | Canale logice (stream-uri HTTP/2) |
|---|---|---|
| Publisher | 1 canal gRPC | câte un apel unar per publicare: cerere publisher→broker, răspuns broker→publisher |
| Subscriber | 1 canal gRPC | 1 stream de lungă durată broker→subscriber (`Listen`, unidirecțional) + apeluri unare subscriber→broker (`Subscribe`, `Unsubscribe`, `Ack`, `Ping`) |

Argument: numărul de topicuri e dinamic, deci un canal per topic ar înmulți conexiunile odată cu
topicurile. Rutarea e făcută de broker, nu de transport, iar HTTP/2 multiplexează toate apelurile
pe o singură conexiune. Livrarea și comenzile sunt separate în direcții diferite, astfel încât un
ACK lent nu blochează livrarea și invers.

### 2.3 Structura comunicației: unul-la-unu

Fiecare livrare este o relație punct-la-punct broker→un singur receptor, cu propria stare
(`PENDING`/`DELIVERED`/`DEAD`), propriul contor de încercări și propriul ACK. Un topic cu mai mulți
abonați produce mai multe livrări unul-la-unu independente (fan-out). Căderea unui receptor nu
afectează livrarea către ceilalți.

### 2.4 Politici de livrare

| Caz | Politică |
|---|---|
| Mesaj invalid (validare §5) | respins cu `INVALID_ARGUMENT`; salvat în DLQ cu motivul |
| Bytes corupți pe fir (nu se pot deserializa) | respins de framework; brokerul continuă să ruleze |
| `message_id` duplicat | respins cu `ALREADY_EXISTS` (idempotență), nu se dublează livrările |
| Niciun abonat la topic | mesajul rămâne stocat `PENDING`; e livrat ca backlog la primul subscribe |
| Abonat offline | livrarea rămâne `PENDING`; pornește la reconectare (`Listen`) |
| Fără ACK în 5 s | retrimitere; după 3 încercări fără ACK → `DEAD` + DLQ |
| Același mesaj primit de două ori de subscriber | subscriberul deduplică după `message_id` și retrimite ACK |
| Subscriberul cade în timpul procesării | lipsa ACK declanșează retrimiterea |
| Căderea brokerului, mod persistent | toată starea e în SQLite; la repornire livrările `PENDING` se reiau |
| Căderea brokerului, mod transient | starea se pierde (compromis asumat al modului transient) |
| Excepție neprevăzută într-un handler | interceptor global → `INTERNAL`, log; brokerul continuă |

### 2.5 Rulare distribuită (brokerul ca microserviciu)

Brokerul este un serviciu de rețea independent: rulează pe un calculator, iar publisherii și
subscriberii rulează pe alte calculatoare și îl folosesc doar prin contractul `.proto` și
adresa `IP:port`.
- Brokerul ascultă pe `0.0.0.0:<port>`, adică pe toate interfețele, nu doar pe loopback. La
  pornire afișează adresele IPv4 ale mașinii, ca să știi ce adresă dai clienților.
- Clienții primesc `--host`/`--port` (publisher) sau `host port` (subscriber).
- Windows Firewall blochează implicit conexiunile venite din afară. README-ul conține comanda
  pentru regula de intrare TCP pe port (necesită drepturi de administrator).
- Keepalive HTTP/2 pe ambele capete: brokerul trimite ping la 30 s și închide conexiunea dacă
  nu primește răspuns în 10 s; subscriberul trimite ping la 30 s. Așa, un subscriber dispărut
  din rețea (cablu scos, Wi-Fi căzut) e detectat în aproximativ 40 s și scos din lista celor
  online. Din acel moment nu i se mai consumă încercările de livrare. Mesajele trimise în
  fereastra de detecție pot ajunge în DLQ după 3 încercări fără ACK, ceea ce e corect conform
  politicii de livrare. Tot keepalive-ul ține conexiunile inactive deschise prin routere și NAT.
- `subscriber_id` implicit: `java-<hostname>`, ca doi subscriberi de pe calculatoare diferite să
  nu se suprapună. Dacă totuși se conectează doi cu același id, ultima conexiune câștigă, iar
  prima primește `ABORTED` / `SESSION_REPLACED` și nu se mai reconectează automat.
- Log-urile brokerului includ adresa clientului (`context.Peer`), ca în lab 1.
- Dimensiunea maximă a unui mesaj primit de broker este de 64 KB. Mesajele mai mari sunt respinse
  de framework cu `RESOURCE_EXHAUSTED`, înainte de deserializare.

## 3. Reprezentarea datelor (`proto/broker.proto`)

Contract unic, partajat de cele trei servicii. Codul generat din el nu se comite:
- C#: generat la build de `Grpc.Tools`;
- Java: generat la `mvnw package` de plugin-ul protobuf pentru Maven;
- Python: generat cu `python -m grpc_tools.protoc`, ignorat prin `.gitignore`.

```proto
syntax = "proto3";

package pad.broker.v1;

option csharp_namespace = "Broker.Contracts";
option java_package = "md.utm.pad.broker.grpc";
option java_multiple_files = true;

service MessageBroker {
  rpc Publish(PublishRequest) returns (PublishReply);
  rpc Subscribe(SubscriptionRequest) returns (SubscriptionReply);
  rpc Unsubscribe(SubscriptionRequest) returns (SubscriptionReply);
  rpc Listen(ListenRequest) returns (stream DeliveredMessage);
  rpc Ack(AckRequest) returns (AckReply);
  rpc Ping(PingRequest) returns (PingReply);
  rpc GetState(StateRequest) returns (BrokerState);
}

message PublishRequest      { string message_id = 1; string topic = 2; string content = 3; }
message PublishReply        { string message_id = 1; string topic = 2; int32 subscribers = 3; }
message SubscriptionRequest { string subscriber_id = 1; string topic = 2; }
message SubscriptionReply   { string subscriber_id = 1; string topic = 2; }
message ListenRequest       { string subscriber_id = 1; }
message DeliveredMessage    { string message_id = 1; string topic = 2; string content = 3;
                              int32 attempt = 4; int32 max_attempts = 5; }
message AckRequest          { string subscriber_id = 1; string message_id = 2; }
message AckReply            { string message_id = 1; bool duplicate = 2; }
message PingRequest         {}
message PingReply           { string server_time_utc = 1; string storage_mode = 2; }

message StateRequest        { int32 limit = 1; }
message BrokerState {
  string storage_mode = 1;
  repeated MessageRecord messages = 2;
  repeated DeliveryRecord deliveries = 3;
  repeated DeadLetterRecord dead_letters = 4;
  repeated SubscriptionRecord subscriptions = 5;
  repeated string online_subscribers = 6;
}
message MessageRecord      { string message_id = 1; string topic = 2; string content = 3;
                             string status = 4; string created_at_utc = 5; }
message DeliveryRecord     { string message_id = 1; string subscriber_id = 2; string status = 3;
                             int32 retry_count = 4; string last_error = 5; }
message DeadLetterRecord   { int64 id = 1; string message_id = 2; string subscriber_id = 3;
                             string topic = 4; string raw_payload = 5; string reason = 6;
                             int32 retry_count = 7; string created_at_utc = 8; }
message SubscriptionRecord { string subscriber_id = 1; string topic = 2; }
```

`GetState.limit`: 0 înseamnă 100; maxim 1000. Rezultatele sunt ordonate de la cele mai noi.

Corespondența cu lab 1:

| Lab 1 (`action` JSON) | Lab 2 (RPC) |
|---|---|
| `publish` → `publish_accepted` | `Publish` |
| `subscribe` → `subscribed` | `Subscribe` |
| `unsubscribe` → `unsubscribed` | `Unsubscribe` |
| mesaj `message` pe socket | `Listen` (stream) |
| `ack` → `ack_received` | `Ack` (cu `subscriber_id` explicit) |
| `ping` → `pong` | `Ping` |
| `error` + `reason` | status gRPC + detaliu (§6) |
| GUI citea direct `broker.db` | `GetState` |

## 4. Broker (C#)

ASP.NET Core cu `Grpc.AspNetCore`, Kestrel pe HTTP/2 fără TLS (h2c), `0.0.0.0:<port>`.

| Argument | Implicit | Sens |
|---|---|---|
| `--port` | `50051` | portul gRPC |
| `--storage` | `persistent` | `persistent` (SQLite) sau `transient` (memorie) |
| `--db` | `broker.db` | calea SQLite, relativă la directorul curent |
| `--workers` | `4` | numărul de work joburi de livrare |

Constante: `MaxRetries=3`, `AckTimeoutSeconds=5` (o livrare fără ACK se retrimite după 5 s),
`RetryIntervalSeconds=1` (cât de des verifică jobul periodic ce e scadent). Intervalul jobului e mai mic
decât timeout-ul intenționat. Cu valori egale (5/5, ca în lab 1), o încercare înregistrată imediat
după o rundă e ratată de runda următoare, iar retrimiterea ajunge la 10 s în loc de 5 s.
Reflection gRPC activat pentru inspecție cu `grpcurl`. Log-urile framework-ului sunt reduse la
`Warning`; log-urile de domeniu păstrează stilul din lab 1 (`[PERSISTED]`, `[SUBSCRIBE]`,
`[DELIVERY]`, `[ACK]`, `[DLQ]` ...).

### 4.1 Componente

| Componentă | Responsabilitate |
|---|---|
| `MessageBrokerService` | mapează RPC → operații de domeniu; nu conține logică de rutare sau SQL |
| `ExceptionInterceptor` | orice excepție neprevăzută → `INTERNAL` + log; brokerul nu cade |
| `RequestValidator` | regulile din §5; întoarce lista de coduri de eroare, nu aruncă |
| `TopicRouter` | nivelul de rutare (§4.3) |
| `SubscriberRegistry` | sesiunile online: `ConcurrentDictionary<string, SubscriberSession>` |
| `SubscriberSession` | `Channel<DeliveredMessage>` (coadă thread-safe) + `CancellationTokenSource` |
| `DeliveryJobQueue` | coada de joburi de livrare (§4.4) |
| `DeliveryWorkerPool` | N work joburi care consumă coada și livrează (§4.4) |
| `RetryScheduler` | jobul periodic (cron, la 1 s) care replanifică subscriberii online |
| Store-uri (§4.2) | stocarea, în spatele unor interfețe mici |

Dependențele merg spre interfețe; modul de stocare se alege o singură dată, la pornire, prin
dependency injection. Restul codului nu știe care implementare rulează.

### 4.2 Stocare: transientă și persistentă

Interfețe separate după cine le folosește:

| Interfață | Operații | Folosită de |
|---|---|---|
| `ISubscriptionStore` | adaugă subscripție + livrări de backlog (atomic); șterge | `TopicRouter` |
| `IMessageStore` | adaugă mesaj + livrări pentru abonații curenți (atomic) → `inserted`, numărul de destinatari | `TopicRouter` |
| `IDeliveryStore` | livrările scadente ale unui subscriber; înregistrează încercarea; marchează livrat → `Acked`/`Duplicate`/`NotFound`; recalculează statusul mesajului | worker-i, `Ack` |
| `IDeadLetterStore` | mută o livrare în DLQ (atomic); salvează un payload invalid | worker-i, `Publish` |
| `IStateReader` | instantaneul pentru `GetState` | `GetState` |

**`SqliteStore`** (persistent) implementează toate interfețele:
- schema e identică cu lab 1 (`Messages`, `Subscriptions`, `Deliveries`, `DeadLetters`);
- toate operațiile sunt asincrone (`Microsoft.Data.Sqlite`, `*Async`);
- `PRAGMA journal_mode=WAL` și `Default Timeout=5`;
- operațiile compuse rulează într-o singură tranzacție de scriere. Diferență față de lab 1: acolo
  inserarea mesajului și crearea livrărilor erau separate, iar o cădere între ele lăsa un mesaj
  fără livrări.

**`InMemoryStore`** (transient) implementează aceleași interfețe:
- colecțiile sunt `ConcurrentDictionary`: mesaje după id, subscripții după (subscriber, topic),
  livrări după (mesaj, subscriber);
- DLQ-ul e un `ConcurrentQueue`;
- operațiile compuse (mesaj + livrări, subscripție + backlog, livrare → DLQ) sunt protejate de un
  lock comun. Colecțiile thread-safe garantează operațiile individuale, nu invariantul dintre două
  colecții, de aceea e nevoie de lock-ul suplimentar.

Ambele implementări trec **aceeași** suită de teste de contract (§9.1). Asta dovedește că sunt
substituibile.

### 4.3 Nivelul de rutare (`TopicRouter`)

Rutarea se face pe subiect (topic), adică pe tipul mesajului inclus în corpul lui. `TopicRouter`
este singurul loc care decide cine primește un mesaj:
- `PublishAsync(message)`: stochează mesajul atomic împreună cu câte o livrare pentru fiecare
  abonat curent al topicului, apoi pune în `DeliveryJobQueue` destinatarii care sunt online.
- `SubscribeAsync(subscriber, topic)`: înregistrează subscripția și creează livrări pentru mesajele
  existente ale topicului (backlog, ca lab 1); dacă subscriberul e online, îl pune în coadă.
- `UnsubscribeAsync(subscriber, topic)`: șterge subscripția. Livrările deja create rămân `PENDING`
  și sunt ignorate cât timp subscripția lipsește; se reiau la re-subscribe (ca lab 1).

Potrivirea topicului e exactă (`sport` ≠ `sport.fotbal`).

### 4.4 Concurență și work joburi

- **Cereri:** Kestrel tratează fiecare apel RPC concurent, pe thread pool, cu I/O asincron, deci
  canalul nu se blochează. E echivalentul „thread per request” din enunț, fără costul unui thread
  dedicat per conexiune.
- **`DeliveryJobQueue`:** `Channel<string>` cu id-uri de subscriber, plus un set
  `ConcurrentDictionary` care împiedică același subscriber să stea de mai multe ori în coadă.
- **`DeliveryWorkerPool`:** pornește `--workers` bucle concurente. Fiecare ia un subscriber din
  coadă și îi livrează din storage, în ordinea creării, toate livrările scadente: `PENDING` și cu
  ultima încercare mai veche de `AckTimeoutSeconds`, pe topicuri la care e încă abonat. Pentru
  fiecare livrare:
  1. `RetryCount >= MaxRetries` → mutare atomică în DLQ (`DEAD` + rând `DeadLetters`, reason
     `ACK_NOT_RECEIVED_AFTER_MAX_RETRIES`), apoi recalcularea statusului mesajului;
  2. altfel scrie `DeliveredMessage{attempt = RetryCount+1}` în canalul sesiunii, apoi
     înregistrează încercarea (`RetryCount+1`, `LastAttemptAt=now`). Dacă sesiunea s-a închis
     între timp, încercarea nu se consumă.
- **Exclusivitate:** un `SemaphoreSlim` per subscriber (într-un `ConcurrentDictionary`) asigură că
  doi worker-i nu livrează simultan aceluiași subscriber. Ordinea se păstrează, iar subscriberi
  diferiți sunt serviți în paralel. Diferență față de lab 1: acolo lock-ul era per pereche
  (mesaj, subscriber), iar dicționarul creștea nelimitat.
- **`RetryScheduler`:** la fiecare `RetryIntervalSeconds` pune în coadă toți subscriberii online.
  Ăsta e „cron jobul” din enunț.
- **Stream-ul `Listen`:** singurul care scrie pe `IServerStreamWriter`. Citește din `Channel`-ul
  sesiunii, fiindcă writer-ul gRPC nu suportă scrieri concurente.

### 4.5 Fluxuri

**Publish:** validare → invalid: `IDeadLetterStore` (payload = request-ul serializat JSON cu
`JsonFormatter`, `MessageId` NULL, ca lab 1) + `INVALID_ARGUMENT` → valid: `TopicRouter.PublishAsync`
→ duplicat: `ALREADY_EXISTS` → altfel `PublishReply{subscribers}`.

**Listen:**
1. Validare `subscriber_id`.
2. Creează sesiunea și o înregistrează. O sesiune existentă cu același id e înlocuită și anulată
   (ultima conexiune câștigă, ca lab 1).
3. Pune subscriberul în `DeliveryJobQueue`, ca să livreze pending-ul pe toate topicurile abonate
   în store.
4. Transmite din canal până la anulare.
5. `finally`: deînregistrare doar dacă sesiunea din registry e tot aceasta.

Diferență față de lab 1: subscripțiile sunt durabile la reconectare. În lab 1, topicurile erau în
memoria conexiunii și trebuia dat din nou subscribe după reconectare.

**Ack:** validare → `IDeliveryStore.MarkDelivered`:
- `Acked` → recalculează statusul mesajului, `AckReply{duplicate=false}`;
- `Duplicate` (deja `DELIVERED`) → `AckReply{duplicate=true}`;
- `NotFound` (inexistentă sau `DEAD`) → `NOT_FOUND`.

`Ack` nu cere ca subscriberul să fie online.

**Statusul mesajului** (regula din lab 1): fără livrări → `PENDING`; are livrări `PENDING` →
`PENDING`; altfel cel puțin una `DELIVERED` → `DELIVERED`; altfel `DEAD`. Statusul se recalculează
în aceeași tranzacție cu orice schimbare a livrărilor: ACK, mutare în DLQ, dar și subscribe, care
poate adăuga o livrare nouă `PENDING` unui mesaj deja `DELIVERED`. În lab 1, subscribe nu
recalcula statusul. Invariant verificat de oracol: `Messages.Status` = regula aplicată pe
`Deliveries`.

### 4.6 Tratarea excepțiilor
- Validarea nu aruncă excepții; erorile sunt valori.
- `ExceptionInterceptor` prinde orice excepție care scapă dintr-un handler (în afară de
  `RpcException` intenționate și de anulare): o loghează și răspunde `INTERNAL` cu mesaj generic.
- Un worker care eșuează pe un subscriber loghează eroarea și trece la următorul job; bucla nu moare.
- Un mesaj corupt pe fir e respins de framework înainte să ajungă la handler (verificat în §9.3).

## 5. Validare

| Câmp | Regulă | Coduri |
|---|---|---|
| `message_id` | obligatoriu, max 64, aceleași caractere ca topicul (un UUID trece) | `MESSAGE_ID_REQUIRED`, `MESSAGE_ID_TOO_LONG`, `MESSAGE_ID_INVALID_CHARS` |
| `topic` | obligatoriu, max 64, doar litere, cifre, `.`, `_`, `-` (`^[\p{L}\p{N}._-]+$`) | `TOPIC_REQUIRED`, `TOPIC_TOO_LONG`, `TOPIC_INVALID_CHARS` |
| `content` | obligatoriu (nu doar spații), max 4096 caractere | `CONTENT_REQUIRED`, `CONTENT_TOO_LONG` |
| `subscriber_id` | obligatoriu, max 64, aceleași caractere ca topicul | `SUBSCRIBER_ID_REQUIRED`, `SUBSCRIBER_ID_TOO_LONG`, `SUBSCRIBER_ID_INVALID_CHARS` |

„Caractere” înseamnă code points Unicode, nu unități UTF-16. Așa, un emoji contează ca 1, exact
ca `len()` din Python. Un `Publish` invalid se salvează în DLQ cu `Reason` = toate codurile de
eroare, separate prin virgulă.

Doar un `Publish` invalid ajunge în DLQ (ca lab 1). Celelalte RPC-uri invalide primesc doar
`INVALID_ARGUMENT`. Validarea se face în broker (sursa de adevăr) și, pentru mesaje clare, și în
clienți. Opțiunile de test DLQ din publisher ocolesc însă validarea locală.

## 6. Erori → status gRPC

| Situație | Status | Detaliu |
|---|---|---|
| validare eșuată | `INVALID_ARGUMENT` | codurile de eroare, separate prin virgulă |
| `message_id` duplicat | `ALREADY_EXISTS` | `DUPLICATE_MESSAGE_ID` |
| ACK pentru livrare inexistentă sau `DEAD` | `NOT_FOUND` | `DELIVERY_NOT_FOUND` |
| excepție neprevăzută | `INTERNAL` | mesaj generic; detaliul doar în log-ul brokerului |

## 7. Publisher (Python)

`publisher/publisher.py`, cu `grpcio` + `grpcio-tools` în `requirements.txt`, rulat din `.venv`
local. Argumente: `--host` (implicit `127.0.0.1`), `--port` (implicit `50051`). Deadline 5 s pe
fiecare apel. Erorile `grpc.RpcError` se afișează ca `code` + `details`; brokerul indisponibil
(`UNAVAILABLE`) produce un mesaj clar, fără stack trace.

Meniu (extinde lab 1):
1. Publish (topic + mesaj, `message_id` = uuid4)
2. Test DLQ: publish fără topic
3. Test DLQ: publish fără conținut
4. Test idempotență: retrimite ultimul mesaj (același `message_id`)
5. Ping
0. Ieșire

## 8. Subscriber (Java)

Proiect Maven cu Maven Wrapper (`mvnw`/`mvnw.cmd`, distribuția oficială „only-script”): pe mașină
e nevoie doar de JDK 11+. `mvnw package` produce un jar executabil cu toate dependențele.
Argumente: `host` `port` (ca lab 1, implicit `127.0.0.1 50051`).

| Clasă | Responsabilitate |
|---|---|
| `SubscriberApp` | `main`, meniu consolă |
| `BrokerClient` | canal gRPC, apeluri unare cu deadline, `Listen` cu reconectare automată la 3 s |
| `MessageProcessor` | afișare, deduplicare după `message_id` (set thread-safe), politica de ACK |

Meniu (extinde lab 1): 1 Subscribe, 2 Unsubscribe, 3 Topicuri abonate, 4 Ping,
5 Auto-ACK on/off (pentru demonstrarea DLQ pe timeout), 0 Ieșire.

Deduplicare (ca lab 1): un mesaj deja procesat nu se reafișează, dar ACK-ul se retrimite. Cu
auto-ACK oprit, mesajul se afișează marcat `ACK disabled` și nu intră în setul de procesate.

`Subscriber ID` gol → implicit `java-<hostname>` (§2.5).

`Listen` pornește imediat după citirea `Subscriber ID`. Dacă stream-ul cade (broker oprit),
subscriberul reîncearcă la fiecare 3 s și primește pending-ul la revenire.

## 9. Testare și verificare

### 9.1 Unit (C#, xUnit)
- `RequestValidator`: fiecare regulă, limitele exacte (64/65, 4096/4097), Unicode în topic.
- Suită de contract pentru store-uri, rulată pe **ambele** implementări (`InMemoryStore`,
  `SqliteStore` pe fișier temporar): publish + livrări atomic, duplicat, backlog la subscribe,
  livrări scadente (timeout, topic dezabonat), ACK / ACK duplicat / ACK inexistent, mutare în DLQ,
  regula de status a mesajului.
- `DeliveryJobQueue`: deduplicarea subscriberilor în coadă.

### 9.2 Unit (Java, JUnit 5)
- `MessageProcessor`: deduplicare, ACK retrimis la duplicat, auto-ACK oprit.

### 9.3 E2E + oracol independent (`tests/e2e/`, Python)

Testele pornesc brokerul compilat pe un port și o bază de date temporare și rulează scenariile prin
gRPC, cu clienți proprii de test. Verificarea finală citește SQLite direct, cu SQL propriu, fără
cod din `broker/`.

Scenarii (P = persistent, T = transient):

| # | Scenariu | Mod |
|---|---|---|
| 1 | Publish → subscriber online → livrare → ACK → mesaj `DELIVERED` | P, T |
| 2 | Publish fără abonați → `PENDING`; subscribe ulterior → livrare backlog | P, T |
| 3 | Subscriber fără ACK → 3 încercări la 5–6 s una de alta → `DEAD` + rând în DLQ | P, T |
| 4 | Publish fără topic / fără conținut / topic invalid → `INVALID_ARGUMENT` + rând în DLQ | P, T |
| 5 | Același `message_id` de două ori → `ALREADY_EXISTS`, un singur mesaj | P, T |
| 6 | ACK duplicat → `duplicate=true`; ACK inexistent → `NOT_FOUND` | P, T |
| 7 | Restart broker cu subscriber abonat, dar offline → la `Listen` primește pending-ul | P |
| 8 | Concurență: 5 publisheri paraleli × 40 mesaje, 3 subscriberi → fiecare livrare `DELIVERED` exact o dată | P, T |
| 9 | Mesaj nealterat: conținut cu diacritice, emoji, `\n`, ghilimele, 4096 caractere → identic la subscriber | P |
| 10 | Bytes corupți și mesaj > 64 KB pe metoda `Publish` → eroare la client, nimic stocat, brokerul răspunde în continuare la `Ping` | P |
| 11 | Restart broker în mod transient → stare goală (compromis documentat în §2.4) | T |
| 12 | Clienții se conectează prin IP-ul LAN al mașinii, nu prin `127.0.0.1` (dovada că brokerul ascultă pe rețea) | P |
| 13 | Doi subscriberi cu același id → primul primește `ABORTED`/`SESSION_REPLACED`, al doilea primește mesajele | P |

În modul transient nu există fișier, deci starea se citește prin `GetState`. Asta nu e un oracol
independent, fiindcă trece prin codul brokerului. Oracolul complet rulează doar în modul persistent,
iar pentru transient verificarea se sprijină pe ce au primit efectiv clienții de test.

Conservarea numerelor, în modul persistent, din SQLite:
- `count(Messages)` = suma pe statusurile `PENDING` + `DELIVERED` + `DEAD`;
- `count(Deliveries)` = suma pe statusurile `PENDING` + `DELIVERED` + `DEAD`;
- publish-uri invalide trimise = rânduri `DeadLetters` cu `MessageId IS NULL`;
- livrări `DEAD` = rânduri `DeadLetters` cu `MessageId` nenul, pe aceeași pereche (mesaj, subscriber);
- în scenariul 8: mesaje trimise = mesaje în DB; livrări = mesaje × abonați; mesaje primite de
  fiecare subscriber, fără duplicate = mesajele topicului lui.

### 9.4 Interoperabilitate pe procesele reale
Broker C# + `publisher.py` + subscriber Java, rulate cu input scriptat. Output-ul trebuie să arate
livrarea, ACK-ul, deduplicarea și DLQ-ul cu auto-ACK oprit.

## 10. În afara scopului
- Interfața grafică (pasul următor; consumă `GetState`).
- TLS și autentificare.
- Replicarea brokerului (single point of failure, descris în enunț ca limitare a șablonului).
- Docker / orchestrare.
