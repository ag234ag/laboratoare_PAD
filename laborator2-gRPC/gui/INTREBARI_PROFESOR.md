# Întrebări posibile la apărare: gRPC, Protobuf, HTTP/2 (Laborator 2)

Pentru: Adrian și colegii lui, ca material de învățat. Fiecare răspuns are patru părți:
**De zis** (varianta scurtă, 2-3 propoziții), **Explicat** (ca să înțelegi de fapt), **La noi în proiect** (dovada din cod) și **Atenție** (unde se greșește des sau unde profesorul poate pune o întrebare capcană).

Ordinea logică de învățat: 7 → 1 → 2 → 8 → 3 → 4 → 11 → 10 → 5 → 9 → 6.

Notă de onestitate: în proiect **nu am măsurat** viteza (nu am făcut benchmark) și canalele noastre sunt **fără TLS**. La apărare, spune ce știm din teorie și ce am verificat noi. Nu afirma cifre pe care nu le-ai măsurat.

---

## Fundamentele, într-o schemă

```
        broker.proto   (un singur contract)
              |
     protoc + plugin gRPC     <- generează cod pentru fiecare limbaj
     /         |          \
   C#        Python       Java
 (broker)  (publisher)  (subscriber)

  cerere:  obiect Python  --serializare protobuf-->  octeți  --HTTP/2 peste TCP-->  broker (C#)
  răspuns: octeți  --deserializare-->  obiect C# / Python / Java
```

---

## 7. Fișierul protobuf: cum lucrează, ce face

**De zis.** Fișierul `.proto` este **contractul** dintre servicii: descrie ce mesaje există (câmpuri, tipuri, numere) și ce apeluri la distanță expune serviciul (`service`). Din el se generează automat cod în orice limbaj, iar același fișier garantează că broker, publisher și subscriber înțeleg exact aceleași date.

**Explicat.** Un `.proto` are două lucruri:
- `message`: structura datelor. Fiecare câmp are tip, nume și un **număr** (`string topic = 2;`). Numărul, nu numele, este ce circulă pe rețea.
- `service`: lista de metode (RPC) cu tipul cererii și al răspunsului. Cuvântul `stream` în fața tipului face apelul de tip streaming.

Compilatorul `protoc` citește fișierul și, cu un plugin pentru limbajul țintă, produce clase pentru mesaje (cu serializare și deserializare) și cod de client și server pentru servicii.

**La noi în proiect.** `proto/broker.proto` are 7 RPC-uri: `Publish`, `Subscribe`, `Unsubscribe`, `Listen` (stream), `Ack`, `Ping`, `GetState`, și mesaje ca `PublishRequest { message_id=1, topic=2, content=3 }`. Un singur fișier este folosit de C# (broker), Python (publisher și GUI) și Java (subscriber).

**Atenție.** Numerele câmpurilor nu se schimbă niciodată după ce contractul e folosit. Așa, un client vechi poate vorbi cu un server nou: câmpurile necunoscute sunt ignorate (compatibilitate înainte și înapoi).

---

## 1. gRPC: cum este el generat

**De zis.** Codul gRPC nu se scrie de mână. Se **generează din `broker.proto`** cu compilatorul `protoc` și un plugin gRPC pentru fiecare limbaj. Rezultă clase de mesaje și un *stub* (client) și o clasă de bază (server) pentru serviciu.

**Explicat.** Generarea produce două lucruri:
1. codul **protobuf** (mesajele: câmpuri, serializare);
2. codul **gRPC** (serviciul: metodele apelabile pe client, metodele de implementat pe server).

Pe client apelezi o metodă obișnuită (`stub.Publish(request)`), iar biblioteca face restul: serializează, deschide un stream HTTP/2, trimite, așteaptă răspunsul, deserializează. Serverul extinde o clasă generată și implementează metodele.

**La noi în proiect.** Fiecare limbaj își generează propriul cod, iar codul generat **nu se comite**:
- C#: la `build`, prin pachetul `Grpc.Tools` (în `Broker.csproj`: `<Protobuf Include="..\proto\broker.proto" GrpcServices="Server" />`). Brokerul extinde `MessageBroker.MessageBrokerBase`.
- Java: la `mvnw package`, prin `protobuf-maven-plugin` (vezi `pom.xml`).
- Python: prin `grpc_tools.protoc`. În GUI, `gui/core/stubs.py` face asta automat la prima pornire și scrie în `gui/generated/` (ignorat de git).

**Atenție.** „gRPC e generat” înseamnă *clientul și scheletul serverului*, nu logica brokerului. Logica (rutare, retry, DLQ) a scris-o echipa.

---

## 2. De ce ne trebuie mesajul generat protobuf?

**De zis.** Pentru că el definește **forma exactă și tipizată** a datelor și le transformă automat în octeți și înapoi. Fără el ar trebui să scriem de mână parsarea, ca în laboratorul 1, unde fiecare limbaj construia și citea JSON „cu mâna”.

**Explicat.** Beneficii concrete:
- **Contract unic** între 3 limbaje: nu mai există „publisherul trimite `messageId`, brokerul așteaptă `message_id`”.
- **Tipuri verificate la compilare**: nu poți pune un text acolo unde e număr.
- **Serializare/deserializare generată**: zero cod de parsare scris manual, deci mai puține bug-uri.
- **Evoluție controlată**: poți adăuga câmpuri noi fără să strici clienții vechi.

**La noi în proiect.** În laboratorul 1, `Subscriber.java` avea 855 de linii, în mare parte pentru socket, construit de JSON cu `escapeJson(...)` și parsare; brokerul de la lab 1 (`Program.cs`, 1528 linii) citea JSON și decidea după un câmp text (`case "ack":`). În lab 2, `MessageBrokerService.cs` are 133 de linii: metoda `Ack(AckRequest, ...)` primește deja un obiect tipizat.

**Atenție.** Nu spune că „protobuf validează datele”. Protobuf verifică **structura și tipul**, nu regulile de business. Regulile (topic nenul, max 64 de caractere, doar litere/cifre/`._-`) le verifică `RequestValidator.cs`.

---

## 8. Avem toate tipurile de date sau doar specifice (char, int)?

**De zis.** Protobuf are tipuri **scalare** (numere întregi, virgulă mobilă, bool, string, bytes), plus tipuri **compuse** (mesaje imbricate, `repeated`, `map`, `enum`, `oneof`). Nu există `char`: un caracter este un `string` de lungime 1.

**Explicat.** Tipuri scalare principale:

| Protobuf | C# | Java | Python | Note |
|---|---|---|---|---|
| `int32` / `int64` | int / long | int / long | int | cu semn; numere mici sunt codate pe puțini octeți |
| `uint32` / `uint64` | uint / ulong | int / long | int | fără semn |
| `sint32` / `sint64` | int / long | int / long | int | eficient pentru valori negative (ZigZag) |
| `fixed32` / `fixed64` | uint / ulong | int / long | int | mereu 4 sau 8 octeți |
| `float` / `double` | float / double | float / double | float | |
| `bool` | bool | boolean | bool | |
| `string` | string | String | str | text UTF-8 |
| `bytes` | ByteString | ByteString | bytes | date binare brute |

Compuse: `message` în `message`, `repeated T` (listă), `map<K,V>`, `enum`, `oneof` (unul din mai multe câmpuri), și tipuri standard precum `google.protobuf.Timestamp`.

**La noi în proiect.** Folosim: `string` (ID-uri, topic, conținut, status), `int32` (`subscribers`, `attempt`, `max_attempts`, `retry_count`), `int64` (`id` din `DeadLetterRecord`), `bool` (`duplicate`), `repeated` (liste de mesaje/livrări/abonați în `BrokerState`) și mesaje imbricate. **Nu** folosim `bytes`, `enum`, `oneof`, `map` sau `Timestamp`: datele calendaristice sunt `string` (`created_at_utc`), iar statusul (`PENDING`/`DELIVERED`/`DEAD`) este tot `string`.

**Atenție.** Întrebare-capcană posibilă: „de ce nu ați folosit enum pentru status?”. Răspuns onest: simplitate și lizibilitate în logs și SQLite; enum ar fi fost mai strict.

---

## 3. Avantajul structurii mesajului în gRPC față de TCP. E „viteza mai mare”?

**De zis.** Avantajul principal este **contractul tipizat și compatibil între limbaje**, plus un format **binar compact**. Viteza e un avantaj real, dar secundar și nu l-am măsurat noi.

**Explicat.** Corecție importantă la formularea „la TCP nu e structură”: **TCP este un flux de octeți fără limite de mesaj**. Structura nu vine „de la gRPC în loc de TCP”: gRPC rulează **peste** TCP. În laboratorul 1 am pus noi structura manual: **JSON Lines** (un obiect JSON pe linie, terminat cu `\n`). În laboratorul 2, structura vine din protobuf, iar încadrarea mesajelor (unde începe și unde se termină unul) o face gRPC/HTTP/2.

Avantajele gRPC/protobuf față de „TCP + JSON scris de noi”:
1. **Contract și cod generat** (nu mai scriem protocolul de mână).
2. **Tipuri stricte** și validare structurală.
3. **Compact**: câmpurile sunt identificate prin număr, nu prin nume text.
4. **Funcții de rețea incluse**: timeout-uri (deadline-uri), coduri de stare standard (`UNAVAILABLE`, `ALREADY_EXISTS`…), streaming, keepalive.
5. **Compatibilitate între versiuni** prin numerele de câmp.

**La noi în proiect.** Exemplu concret de compactitate: câmpul `int32 subscribers = 3` cu valoarea 2 se scrie pe **2 octeți** (`0x18 0x02`); în JSON, `{"subscribers":2}` are **17 octeți**. Diferența reală la mesaje mici ca ale noastre e neglijabilă ca timp, dar arată principiul.

**Atenție.** Nu spune „gRPC e de N ori mai rapid”. Spune: „teoretic mai rapid, din motivele de la întrebarea 4; la noi mesajele sunt mici și brokerul are ACK, deci viteza nu a fost criteriul, ci contractul, streaming-ul și multi-limbajul”.

---

## 4. Ce face gRPC mai rapid?

**De zis.** Trei lucruri: **serializare binară** (protobuf, mai mică și mai ieftin de parsat decât JSON text), **HTTP/2** (conexiune persistentă, multiplexare, comprimarea antetelor) și **fără costuri la fiecare apel** (nu se redeschide conexiunea, nu se parsează text).

**Explicat.**
- **Protobuf binar**: nume de câmpuri nu se trimit, doar numere; numerele întregi folosesc *varint* (valori mici = puțini octeți). Parsarea unui format binar cu schemă cunoscută e mai ieftină decât a unui text JSON.
- **HTTP/2**: o singură conexiune TCP pentru toate apelurile (multiplexare), antete comprimate (HPACK), cadre binare.
- **Conexiune persistentă**: handshake-ul TCP se face o dată, nu la fiecare cerere.
- **Streaming**: serverul trimite mesaje pe aceeași conexiune când apar, fără „polling” (fără ca clientul să întrebe mereu „am ceva nou?”).

**La noi în proiect.** `Listen` este un stream de la broker către subscriber: brokerul împinge mesajele imediat ce există, în loc ca subscriberul să le ceară periodic. Toate apelurile unui client (`Subscribe`, `Ack`, `Ping`) merg pe **aceeași conexiune** cu stream-ul `Listen`.

**Atenție.** Viteza se vede mai ales la volum mare și la mesaje frecvente. Pentru a afirma „e mai rapid” ai nevoie de măsurători. Poți propune: „am putea compara lab 1 și lab 2 cu 10 000 de mesaje”.

---

## 11. Cum face gRPC streaming? HTTP/2, proprietăți, streaming vs multiplexare

**De zis.** gRPC folosește **stream-urile HTTP/2**: fiecare apel RPC este un stream logic pe o conexiune TCP. Un apel de tip streaming trimite mai multe mesaje, în timp, pe același stream, fără să-l închidă după primul.

**Explicat.**

*Tipuri de apeluri gRPC:*
| Tip | Cerere | Răspuns | La noi |
|---|---|---|---|
| Unar | 1 | 1 | `Publish`, `Subscribe`, `Ack`, `Ping`, `GetState` |
| Server streaming | 1 | mai multe, în timp | **`Listen`** (brokerul împinge mesaje spre subscriber) |
| Client streaming | mai multe | 1 | nu avem |
| Bidirecțional | mai multe | mai multe | nu avem |

*Cum arată pe fir:* fiecare mesaj gRPC este pus într-un **cadru DATA** HTTP/2, cu un prefix de 5 octeți (1 octet „comprimat?” + 4 octeți lungimea). Apelul are un antet (calea `/pad.broker.v1.MessageBroker/Listen`) și, la sfârșit, un *trailer* cu `grpc-status` (codul de stare). Un stream cu mai multe mesaje înseamnă mai multe cadre DATA pe același stream HTTP/2.

*Proprietățile HTTP/2:*
- **cadre binare** (nu text ca HTTP/1.1);
- **multiplexare**: mai multe stream-uri în paralel pe o singură conexiune TCP;
- **compresia antetelor** (HPACK);
- **controlul fluxului** per stream (receptorul spune cât poate primi);
- **conexiune persistentă**;
- prioritizare și *server push* (gRPC nu folosește server push);
- în practică, HTTP/2 se folosește cu TLS; la noi rulează **fără TLS** (HTTP/2 „în clar”, configurat explicit în `Program.cs`: `HttpProtocols.Http2`).

*Streaming vs multiplexare (întrebare clasică, nu le confunda):*
- **Streaming** este un concept al **aplicației (gRPC)**: *un* apel care schimbă *mai multe mesaje* în timp.
- **Multiplexarea** este o proprietate a **transportului (HTTP/2)**: *mai multe apeluri diferite* (stream-uri) rulează *în același timp*, intercalate, pe *aceeași conexiune* TCP.
- Se combină: pe o conexiune avem multiplexate stream-ul lung `Listen` și, în paralel, apeluri scurte `Ack`/`Ping`.

*Ce ne-a adus HTTP/2 mai bun (față de HTTP/1.1):* o singură conexiune în loc de mai multe, fără blocare la nivel de cerere (*head-of-line blocking* la nivel HTTP), antete mai mici, push de la server prin streaming, mai puțină latență. Rămâne o limită: pierderea unui pachet TCP întârzie toate stream-urile (de aceea există HTTP/3 peste QUIC).

**La noi în proiect.** Subscriberul are 1 conexiune: `Listen` (de lungă durată) + `Subscribe`, `Unsubscribe`, `Ack`, `Ping` (scurte) multiplexate. Din spec: „un ACK lent nu blochează livrarea și invers”. Keepalive HTTP/2: ping la 30 s, timeout 10 s, ca un subscriber dispărut din rețea să fie detectat.

**Atenție.** Nu spune „streaming = multiplexare”. Dacă ți se cere un exemplu: „`Listen` este streaming; faptul că `Ack` poate merge în timp ce `Listen` e deschis, pe aceeași conexiune, este multiplexare”.

---

## 10. De ce avem nevoie de gRPC?

**De zis.** Pentru că avem **trei servicii în trei limbaje** (broker C#, publisher Python, subscriber Java) care trebuie să comunice fiabil, cu un contract comun, cu **streaming** pentru livrare și fără să scriem protocolul de rețea de mână.

**Explicat.** gRPC rezolvă:
- contract unic și cod generat pentru mai multe limbaje (*polyglot*);
- apeluri la distanță care arată ca apeluri locale (RPC), cerința laboratorului 2;
- streaming nativ (livrare broker→subscriber fără polling);
- timeout-uri (deadline), coduri de eroare standard, interceptori (la noi un interceptor prinde excepțiile neașteptate și întoarce `INTERNAL` fără să cadă brokerul);
- HTTP/2: o conexiune, multiplexare.

**La noi în proiect.** Laboratorul cerea „RPC în loc de socket-uri” (P2.1) și „proto în loc de XML/JSON” (P2.2). Brokerul poate fi folosit ca microserviciu de pe calculatoare diferite, doar cu `IP:port` și contractul `.proto`.

**Atenție.** Alternative pe care profesorul le poate pomeni: REST/JSON (simplu, dar fără streaming nativ și fără contract strict), WebSocket (streaming, dar protocol de definit manual), Thrift (similar cu gRPC). Pregătește un argument de ce gRPC: contract + streaming + multi-limbaj.

---

## 5. Diferențele lab 1 vs lab 2; dificultăți; „mai securizat”?

**De zis.** Arhitectura a rămas aceeași (broker, publisher, subscriber, ACK, retry, DLQ, SQLite); s-a schimbat **modul de comunicare**: din TCP + JSON Lines scris manual, în **gRPC + protobuf**. Dificultățile au fost de mediu (generare cod în 3 limbaje, firewall, ID-uri unice) și de concurență.

**Explicat.** Comparație:

| Aspect | Lab 1 | Lab 2 |
|---|---|---|
| Transport | socket TCP direct | gRPC (HTTP/2 peste TCP) |
| Format | JSON Lines (`messageId`, `action`…) | Protobuf (`message_id` etc.), contract în `broker.proto` |
| Cod de protocol | scris de mână în 3 limbaje | generat |
| Port | 5000 | 50051 |
| Tratarea comenzii | `switch` după un câmp text (`"ack"`, `"subscribe"`) | metode tipizate (`Ack`, `Subscribe`…) |
| Livrare spre subscriber | mesaje pe același socket | stream `Listen` (server streaming) |
| Erori | mesaje JSON de eroare | coduri gRPC: `INVALID_ARGUMENT`, `ALREADY_EXISTS`, `NOT_FOUND`, `UNAVAILABLE`… |
| Stare broker | doar în fișierul SQLite | RPC `GetState` (nou) |
| Structura codului broker | un fișier de 1528 de linii | pachete: Api, Delivery, Routing, Storage, Validation |
| Reîncercări | interval 5 s | interval 1 s, timeout ACK 5 s, 3 încercări, apoi DLQ |

**„Mai securizat”: nu pretinde asta fără rezerve.** gRPC *poate* folosi TLS (criptare și autentificare), dar **în proiectul nostru canalele sunt necriptate** (`grpc.insecure_channel` în Python, HTTP/2 fără TLS în Kestrel). Răspuns corect: „gRPC suportă TLS și autentificare; la noi nu am activat, fiind un laborator pe rețea de test. Pentru producție am activa TLS”. O mică formă de siguranță avem: protobuf respinge octeții corupți înainte de logica brokerului, iar brokerul limitează mesajul la 64 KB.

*Dificultăți reale (din documentele colegilor și din lucrul meu la GUI):*
- generarea codului din același `.proto` în trei limbaje (Maven descarcă `protoc` pe sistemul de operare; Python îl generează la rulare);
- rularea pe mai multe calculatoare: **Windows Firewall** blochează conexiunile primite, brokerul ascultă pe `0.0.0.0` și își afișează adresele;
- **ID-uri de subscriber unice** (altfel `SESSION_REPLACED`);
- diacritice în consola Windows (`chcp 65001`);
- detectarea unui subscriber dispărut din rețea (keepalive HTTP/2);
- în GUI: oprirea arborelui de procese al brokerului, curse între thread-uri, ignorarea răspunsurilor de la conexiuni vechi.

**Atenție.** Nu spune că lab 2 „a schimbat arhitectura”. Spune: „aceeași arhitectură, alt protocol de comunicare”. Spec-ul colegilor spune exact asta.

---

## 9. Ce s-a schimbat în publisher, subscriber, broker?

**De zis.** Logica de business (rutare pe topic, ACK, retry, DLQ, persistență) a rămas; în fiecare componentă s-a **înlocuit partea de comunicare**: socket + JSON → gRPC + protobuf. Brokerul s-a și **restructurat** modular.

**Explicat.**

| Componentă | Lab 1 | Lab 2 |
|---|---|---|
| **Broker (C#)** | `TcpListener`, un fișier mare, JSON parsat manual, `case "ack"` | Kestrel + `Grpc.AspNetCore`, `MessageBrokerService` (133 linii) peste module: `Delivery` (workeri, retry, sesiuni), `Routing`, `Storage` (InMemory și SQLite), `Validation`, interceptor de excepții |
| **Publisher (Python)** | socket, `json.dumps` + `\n`, `readline` | `grpc.insecure_channel`, `stub.Publish(PublishRequest(...), timeout=5)`; răspuns tipizat (`reply.subscribers`) |
| **Subscriber (Java)** | socket, `escapeJson`, parsare manuală, 855 de linii | `BrokerClient`, `MessageProcessor`, `AckSender`, `SubscriberApp`: stream `Listen` și apeluri `Ack`, `Subscribe` |

Noutăți în broker: moduri de stocare (`--storage persistent|transient`), număr de workeri, `GetState`, `Ping`, validare centralizată, idempotență prin `message_id` (`ALREADY_EXISTS`), reflection gRPC (poți interoga cu `grpcurl`).

**La noi în proiect (GUI).** Nu am atins niciuna dintre cele trei componente; interfața folosește același contract ca ele. De aceea o aplicație grafică poate fi pornită pe alt calculator doar cu `--host` și `--port`.

**Atenție.** Spune clar ce a făcut cine: backend-ul lab 2 (broker, publisher, subscriber CLI) este al Anastasiei; interfețele grafice sunt adăugate peste.

---

## 6. Cum verificăm bug-urile de conexiune la gRPC? Cum știm că mesajul a ajuns?

**De zis.** Pe trei niveluri: **codul de stare gRPC** (dacă apelul a reușit sau de ce a eșuat), **confirmarea de aplicație** (ACK de la subscriber, `PublishReply` de la broker) și **instrumentele de observare** (`Ping`, `GetState`, loguri, SQLite, teste end-to-end).

**Explicat.**
1. **Cod de stare la fiecare apel.** Un apel unar care nu aruncă excepție înseamnă că serverul l-a procesat și a răspuns. Eșecurile au coduri: `UNAVAILABLE` (conexiune/broker oprit/firewall), `DEADLINE_EXCEEDED` (nu a răspuns în timp), `INVALID_ARGUMENT` (date invalide), `ALREADY_EXISTS` (duplicat), `NOT_FOUND`.
2. **Deadline (timeout)** pe fiecare apel (la noi 5 s), ca un apel să nu stea la infinit.
3. **Confirmare de livrare la nivel de aplicație.** `PublishReply` spune câți abonați a găsit brokerul. Subscriberul trimite `Ack`; fără ACK, brokerul retrimite (de max 3 ori), apoi mută în DLQ. Garanția este **cel puțin o dată** (*at-least-once*): un mesaj poate veni de două ori, de aceea subscriberul deduplică după `message_id`, iar brokerul respinge duplicate la publicare.
4. **Observare.** `Ping` (conexiune), `GetState` (ce e în broker: mesaje, livrări, DLQ, abonați), logurile brokerului (`[RECEIVED]`, `[ACK]`, `[DLQ]`, `[ONLINE]`), `Test-NetConnection IP -Port 50051` pentru rețea, `grpcurl` (reflection activat), fișierul SQLite.
5. **Keepalive HTTP/2** detectează conexiuni moarte (ping 30 s, răspuns în 10 s).
6. **Teste end-to-end**: pornim brokerul real și comparăm ce raportează aplicațiile cu baza de date citită direct.

**La noi în proiect.** În GUI, fiecare rezultat e vizibil: Publisher arată pe fiecare rând `OK, N abonați`, `DUPLICATE`, `INVALID -> DLQ` sau `INDISPONIBIL`; Receiver arată `asteapta ACK` / `ACK trimis` și `încercarea 2/3`; fereastra Broker arată starea reală.

**Atenție.** „A ajuns la broker” nu înseamnă „a ajuns la subscriber”. Ca să afirmi livrarea, ai nevoie de ACK-ul subscriberului. Asta este diferența dintre `PublishReply` și starea `DELIVERED`.

---

## Autotest: răspunde fără să te uiți

1. Ce conține un fișier `.proto` și de ce numerele câmpurilor nu se schimbă?
2. Ce se generează din `.proto` în Python, C# și Java?
3. Care este diferența dintre TCP și gRPC? (Pe care dintre ele rulează cealaltă?)
4. Dă trei motive pentru care gRPC poate fi mai rapid decât TCP + JSON.
5. Care apel din proiect este streaming și în ce sens?
6. Diferența dintre streaming și multiplexare, cu un exemplu din proiect.
7. Ce înseamnă `UNAVAILABLE`? Dar `DEADLINE_EXCEEDED`? Ce arată `ALREADY_EXISTS`?
8. De ce există ACK dacă gRPC deja confirmă apelul?
9. Este lab 2 „securizat”? De ce da/nu?
10. Ce părți au rămas la fel între lab 1 și lab 2 și ce s-a schimbat?
11. Ce tipuri de date am folosit în `broker.proto` și ce nu?
12. Cum aflăm dacă un subscriber a primit mesajul?

Dacă la una nu poți răspunde, citește secțiunea corespunzătoare și încearcă din nou.
