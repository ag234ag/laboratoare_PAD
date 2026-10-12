# Agent de mesagerie gRPC — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Backend-ul Laboratorului 2 (broker C# gRPC, publisher Python, subscriber Java) cu aceeași arhitectură ca Laboratorul 1, plus stocare transientă/persistentă, work joburi de livrare, DLQ și rulare distribuită pe mai multe calculatoare.

**Architecture:** Un contract `proto/broker.proto` partajat de cele trei servicii. Brokerul (ASP.NET Core gRPC, h2c pe TCP) are straturi separate: API → validare → rutare (`TopicRouter`) → store (în spatele unor interfețe mici, cu implementare SQLite sau în memorie) și un pool de work joburi care livrează din store pe stream-uri `Listen`. Clienții sunt programe consolă care primesc `host`/`port`.

**Tech Stack:** .NET 9 (`Grpc.AspNetCore` 2.84.0, `Grpc.AspNetCore.Server.Reflection` 2.84.0, `Microsoft.Data.Sqlite` 10.0.12, xUnit din template-ul `dotnet new xunit`); Python 3.14 (`grpcio` 1.84.0, `grpcio-tools` 1.84.0, `pytest` 9.1.1); Java 11 (grpc-java 1.84.2, protobuf 3.25.9, JUnit 5.14.4, Maven 3.9.16 prin Maven Wrapper 3.3.4).

**Spec:** `laborator2-gRPC/docs/specs/2026-10-12-grpc-broker-design.md`

## Global Constraints

- Toate căile din plan sunt relative la `laborator2-gRPC/` dacă nu se spune altfel. Shell: PowerShell 5.1 (fără `&&`; se folosește `;`).
- **NU se face niciun `git commit`.** Anastasia comite singură. La final de task se listează fișierele create/modificate.
- Fără comentarii în cod care repetă codul; doar o linie scurtă pentru ceva ce nu se poate deduce.
- SOLID: o responsabilitate per clasă; dependențe spre interfețe.
- Versiuni fixe: exact cele din Tech Stack. Nu se schimbă fără verificare.
- Port implicit `50051`; `MaxRetries=3`; `RetryInterval=1s` (corectat în execuție de la 5 s, vezi spec §4); `AckTimeout=5s`; `--workers` implicit `4`.
- Validare: nume (`message_id`, `topic`, `subscriber_id`) max 64 code points, `^[\p{L}\p{N}._-]+\z`; `content` max 4096 code points, nu doar spații. „Caractere” = code points Unicode.
- Statusuri: `PENDING`, `DELIVERED`, `DEAD`. Reason DLQ pentru timeout ACK: `ACK_NOT_RECEIVED_AFTER_MAX_RETRIES`.
- Erori gRPC: `INVALID_ARGUMENT` (coduri unite cu `,`), `ALREADY_EXISTS`/`DUPLICATE_MESSAGE_ID`, `NOT_FOUND`/`DELIVERY_NOT_FOUND`, `ABORTED`/`SESSION_REPLACED`, `INTERNAL`/`INTERNAL_ERROR`.
- Schema SQLite identică cu `laborator1/broker/Program.cs` (tabelele `Messages`, `Subscriptions`, `Deliveries`, `DeadLetters`).
- Namespace C# generat din proto: `Broker.Contracts` (nu `Broker.Grpc`, ca să nu umbrească namespace-ul `Grpc.Core`).
- Brokerul ascultă pe `0.0.0.0`; keepalive HTTP/2 30 s / timeout 10 s; mesaj primit maxim 64 KB.

## Review Focus

1. **Topic cu newline final (`"sport\n"`)**: `$` din regex .NET acceptă un `\n` final. Se așteaptă respingere `TOPIC_INVALID_CHARS`. Test: Task 3, `Topic_rejects_other_characters` cu `"sport\n"`; de aceea regex-ul folosește `\z`.
2. **Emoji și limita de 4096**: în C#, `string.Length` numără UTF-16 (un emoji = 2), în Python `len` numără code points. Se așteaptă ca 4096 emoji să fie acceptați. Test: Task 3, `Content_length_counts_unicode_code_points_not_utf16_units`; E2E Task 11, `test_content_is_not_altered`.
3. **Publish și subscribe simultane pe același topic**: dacă rezolvarea destinatarilor și backlog-ul nu sunt atomice, un subscriber poate rata un mesaj. Se așteaptă ca nicio livrare să nu se piardă. Test: Task 4, `Concurrent_publish_and_subscribe_never_lose_a_delivery`, pe ambele store-uri.
4. **Subscriber care cade după ce a primit, dar înainte de ACK**: se așteaptă ca la reconectare să primească din nou mesajul, cu `attempt` mai mare. Test: Task 10, `test_reconnecting_subscriber_gets_unacked_message_again`.
5. **Același subscriber_id de pe două calculatoare**: se așteaptă ca ultima conexiune să câștige, iar prima să primească `ABORTED`/`SESSION_REPLACED` fără bucle de reconectare. Test: Task 6, `Opening_same_id_replaces_and_closes_previous_session`; E2E Task 11, `test_second_connection_with_same_id_replaces_first`.

---

## File Structure

```
laborator2-gRPC/
├── .gitignore
├── README.md                                   Task 12
├── proto/broker.proto                          Task 1
├── broker/                                     Tasks 2–7
│   ├── Broker.csproj
│   ├── Program.cs                              compoziție + pornire
│   ├── BrokerOptions.cs                        argumente linie de comandă
│   ├── Logging/BrokerLog.cs                    IBrokerLog + ConsoleBrokerLog
│   ├── Validation/RequestValidator.cs
│   ├── Storage/StoreContracts.cs               interfețe + record-uri + Statuses + Timestamps
│   ├── Storage/InMemoryStore.cs                transient
│   ├── Storage/SqliteStore.cs                  persistent
│   ├── Delivery/SubscriberSession.cs
│   ├── Delivery/SubscriberRegistry.cs
│   ├── Delivery/DeliveryJobQueue.cs
│   ├── Delivery/DeliveryProcessor.cs           livrarea pentru un subscriber
│   ├── Delivery/DeliveryWorkerPool.cs          N work joburi
│   ├── Delivery/RetryScheduler.cs              cron la 5 s
│   ├── Routing/TopicRouter.cs
│   ├── Api/MessageBrokerService.cs
│   ├── Api/ExceptionInterceptor.cs
│   ├── Api/PayloadFormatter.cs
│   ├── Api/StateMapper.cs
│   └── Hosting/NetworkAddresses.cs
├── broker.Tests/                               Tasks 2–7
├── publisher/                                  Tasks 1, 8
│   ├── requirements.txt
│   ├── stubs.py                                generează și încarcă stub-urile Python
│   └── publisher.py
├── subscriber/                                 Task 9
│   ├── mvnw, mvnw.cmd, .mvn/wrapper/maven-wrapper.properties
│   ├── pom.xml
│   └── src/{main,test}/java/md/utm/pad/subscriber/*.java
└── tests/e2e/                                  Tasks 10–12
    ├── requirements.txt
    ├── conftest.py, clients.py, broker_process.py, oracle.py
    └── test_*.py
```

---

### Task 1: Scaffolding — contract proto, .gitignore, mediul Python

**Files:**
- Create: `.gitignore`
- Create: `proto/broker.proto`
- Create: `publisher/requirements.txt`
- Create: `publisher/stubs.py`
- Create: `tests/e2e/requirements.txt`

**Interfaces:**
- Produces: contractul `pad.broker.v1.MessageBroker` (C# namespace `Broker.Contracts`, Java package `md.utm.pad.broker.grpc`); `stubs.load() -> (broker_pb2, broker_pb2_grpc)`; venv `laborator2-gRPC/.venv`.

- [ ] **Step 1: Creează `.gitignore`**

```gitignore
bin/
obj/
*.db
*.db-shm
*.db-wal
__pycache__/
.venv/
.pytest_cache/
publisher/generated/
subscriber/target/
```

- [ ] **Step 2: Creează `proto/broker.proto`**

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

message PublishRequest {
  string message_id = 1;
  string topic = 2;
  string content = 3;
}

message PublishReply {
  string message_id = 1;
  string topic = 2;
  int32 subscribers = 3;
}

message SubscriptionRequest {
  string subscriber_id = 1;
  string topic = 2;
}

message SubscriptionReply {
  string subscriber_id = 1;
  string topic = 2;
}

message ListenRequest {
  string subscriber_id = 1;
}

message DeliveredMessage {
  string message_id = 1;
  string topic = 2;
  string content = 3;
  int32 attempt = 4;
  int32 max_attempts = 5;
}

message AckRequest {
  string subscriber_id = 1;
  string message_id = 2;
}

message AckReply {
  string message_id = 1;
  bool duplicate = 2;
}

message PingRequest {}

message PingReply {
  string server_time_utc = 1;
  string storage_mode = 2;
}

message StateRequest {
  int32 limit = 1;
}

message BrokerState {
  string storage_mode = 1;
  repeated MessageRecord messages = 2;
  repeated DeliveryRecord deliveries = 3;
  repeated DeadLetterRecord dead_letters = 4;
  repeated SubscriptionRecord subscriptions = 5;
  repeated string online_subscribers = 6;
}

message MessageRecord {
  string message_id = 1;
  string topic = 2;
  string content = 3;
  string status = 4;
  string created_at_utc = 5;
}

message DeliveryRecord {
  string message_id = 1;
  string subscriber_id = 2;
  string status = 3;
  int32 retry_count = 4;
  string last_error = 5;
}

message DeadLetterRecord {
  int64 id = 1;
  string message_id = 2;
  string subscriber_id = 3;
  string topic = 4;
  string raw_payload = 5;
  string reason = 6;
  int32 retry_count = 7;
  string created_at_utc = 8;
}

message SubscriptionRecord {
  string subscriber_id = 1;
  string topic = 2;
}
```

- [ ] **Step 3: Creează `publisher/requirements.txt`**

```text
grpcio==1.84.0
grpcio-tools==1.84.0
```

- [ ] **Step 4: Creează `tests/e2e/requirements.txt`**

```text
-r ../../publisher/requirements.txt
pytest==9.1.1
```

- [ ] **Step 5: Creează `publisher/stubs.py`**

```python
import importlib
import sys
from pathlib import Path

from grpc_tools import protoc

_HERE = Path(__file__).resolve().parent
PROTO_DIR = _HERE.parent / "proto"
PROTO_FILE = PROTO_DIR / "broker.proto"
OUT_DIR = _HERE / "generated"


def load():
    OUT_DIR.mkdir(exist_ok=True)
    generated = OUT_DIR / "broker_pb2.py"
    if not generated.exists() or generated.stat().st_mtime < PROTO_FILE.stat().st_mtime:
        exit_code = protoc.main([
            "grpc_tools.protoc",
            f"-I{PROTO_DIR}",
            f"--python_out={OUT_DIR}",
            f"--grpc_python_out={OUT_DIR}",
            str(PROTO_FILE),
        ])
        if exit_code != 0:
            raise RuntimeError(f"protoc failed for {PROTO_FILE} (exit code {exit_code})")
    if str(OUT_DIR) not in sys.path:
        sys.path.insert(0, str(OUT_DIR))
    return importlib.import_module("broker_pb2"), importlib.import_module("broker_pb2_grpc")
```

- [ ] **Step 6: Creează venv-ul și instalează dependențele**

Run (din `laborator2-gRPC/`):
```powershell
python -m venv .venv; .\.venv\Scripts\python -m pip install --upgrade pip; .\.venv\Scripts\python -m pip install -r tests/e2e/requirements.txt
```
Expected: `Successfully installed ... grpcio-1.84.0 grpcio-tools-1.84.0 ... pytest-9.1.1`.

- [ ] **Step 7: Verifică faptul că proto-ul compilează**

Run:
```powershell
.\.venv\Scripts\python -c "import sys; sys.path.insert(0, 'publisher'); import stubs; pb, grpc_pb = stubs.load(); print(pb.PublishRequest(message_id='a', topic='b', content='c')); print(hasattr(grpc_pb, 'MessageBrokerStub'))"
```
Expected: cele trei câmpuri afișate și `True`. Fișierele `publisher/generated/broker_pb2.py` și `broker_pb2_grpc.py` există.

- [ ] **Step 8: Checkpoint** — fișiere de comis de Anastasia: `.gitignore`, `proto/broker.proto`, `publisher/requirements.txt`, `publisher/stubs.py`, `tests/e2e/requirements.txt`.

---

### Task 2: Proiectul broker, proiectul de teste, opțiuni și log

**Files:**
- Create: `broker/Broker.csproj`
- Create: `broker/BrokerOptions.cs`
- Create: `broker/Logging/BrokerLog.cs`
- Create: `broker/Program.cs` (temporar minimal, înlocuit în Task 7)
- Create: `broker.Tests/` (din template) + `broker.Tests/BrokerOptionsTests.cs` + `broker.Tests/TestDoubles.cs`

**Interfaces:**
- Consumes: `proto/broker.proto` (Task 1).
- Produces: `Broker.BrokerOptions { Port, Storage, DatabasePath, Workers, MaxRetries, RetryInterval, AckTimeout, StorageName; static Parse(IReadOnlyList<string>) ; const Usage }`, `Broker.StorageMode { Persistent, Transient }`, `Broker.Logging.IBrokerLog { Info(string), Error(string) }`, `ConsoleBrokerLog`, test doubles `ManualTimeProvider(DateTimeOffset) { GetUtcNow(), Advance(TimeSpan) }`, `RecordingLog { Lines }`.

- [ ] **Step 1: Creează `broker/Broker.csproj`**

```xml
<Project Sdk="Microsoft.NET.Sdk.Web">

  <PropertyGroup>
    <TargetFramework>net9.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <RootNamespace>Broker</RootNamespace>
    <AssemblyName>Broker</AssemblyName>
  </PropertyGroup>

  <ItemGroup>
    <Protobuf Include="..\proto\broker.proto" GrpcServices="Server" Link="Protos\broker.proto" />
  </ItemGroup>

  <ItemGroup>
    <PackageReference Include="Grpc.AspNetCore" Version="2.84.0" />
    <PackageReference Include="Grpc.AspNetCore.Server.Reflection" Version="2.84.0" />
    <PackageReference Include="Microsoft.Data.Sqlite" Version="10.0.12" />
  </ItemGroup>

</Project>
```

- [ ] **Step 2: Creează `broker/Program.cs` minimal (ca proiectul să compileze)**

```csharp
return 0;
```

- [ ] **Step 3: Creează proiectul de teste**

Run:
```powershell
dotnet new xunit -n Broker.Tests -o broker.Tests -f net9.0; dotnet add broker.Tests/Broker.Tests.csproj reference broker/Broker.csproj; Remove-Item broker.Tests/UnitTest1.cs
```
Expected: proiect creat și referința adăugată.

- [ ] **Step 4: Creează `broker.Tests/TestDoubles.cs`**

```csharp
using System.Collections.Concurrent;
using Broker.Logging;

namespace Broker.Tests;

public sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
{
    private DateTimeOffset _now = start;

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan duration) => _now += duration;
}

public sealed class RecordingLog : IBrokerLog
{
    public ConcurrentQueue<string> Lines { get; } = new();

    public void Info(string message) => Lines.Enqueue(message);

    public void Error(string message) => Lines.Enqueue($"[ERROR] {message}");
}
```

- [ ] **Step 5: Scrie testele care pică — `broker.Tests/BrokerOptionsTests.cs`**

```csharp
namespace Broker.Tests;

public sealed class BrokerOptionsTests
{
    [Fact]
    public void Defaults_when_no_arguments()
    {
        BrokerOptions options = BrokerOptions.Parse([]);

        Assert.Equal(50051, options.Port);
        Assert.Equal(StorageMode.Persistent, options.Storage);
        Assert.Equal("broker.db", options.DatabasePath);
        Assert.Equal(4, options.Workers);
        Assert.Equal(3, options.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(1), options.RetryInterval);
        Assert.Equal(TimeSpan.FromSeconds(5), options.AckTimeout);
        Assert.Equal("persistent", options.StorageName);
    }

    [Fact]
    public void Parses_all_arguments()
    {
        BrokerOptions options = BrokerOptions.Parse(
            ["--port", "6000", "--storage", "Transient", "--db", "data/x.db", "--workers", "8"]);

        Assert.Equal(6000, options.Port);
        Assert.Equal(StorageMode.Transient, options.Storage);
        Assert.Equal("data/x.db", options.DatabasePath);
        Assert.Equal(8, options.Workers);
        Assert.Equal("transient", options.StorageName);
    }

    [Theory]
    [InlineData("--port", "0")]
    [InlineData("--port", "70000")]
    [InlineData("--port", "abc")]
    [InlineData("--storage", "memory")]
    [InlineData("--workers", "0")]
    [InlineData("--workers", "65")]
    [InlineData("--db", " ")]
    [InlineData("--unknown", "1")]
    [InlineData("port", "50051")]
    public void Rejects_invalid_arguments(string key, string value) =>
        Assert.Throws<ArgumentException>(() => BrokerOptions.Parse([key, value]));

    [Fact]
    public void Rejects_argument_without_value() =>
        Assert.Throws<ArgumentException>(() => BrokerOptions.Parse(["--port"]));
}
```

- [ ] **Step 6: Rulează testele ca să confirmi că pică**

Run: `dotnet test broker.Tests --nologo`
Expected: eroare de compilare `The type or namespace name 'BrokerOptions' could not be found`.

- [ ] **Step 7: Creează `broker/BrokerOptions.cs`**

```csharp
using System.Globalization;

namespace Broker;

public enum StorageMode
{
    Persistent,
    Transient
}

public sealed class BrokerOptions
{
    public const string Usage =
        "Usage: Broker [--port 50051] [--storage persistent|transient] [--db broker.db] [--workers 4]";

    public int Port { get; init; } = 50051;
    public StorageMode Storage { get; init; } = StorageMode.Persistent;
    public string DatabasePath { get; init; } = "broker.db";
    public int Workers { get; init; } = 4;
    public int MaxRetries { get; init; } = 3;
    public TimeSpan RetryInterval { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan AckTimeout { get; init; } = TimeSpan.FromSeconds(5);

    public string StorageName => Storage == StorageMode.Persistent ? "persistent" : "transient";

    public static BrokerOptions Parse(IReadOnlyList<string> args)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < args.Count; i += 2)
        {
            string key = args[i];
            if (!key.StartsWith("--", StringComparison.Ordinal) || i + 1 >= args.Count)
                throw new ArgumentException($"Invalid argument: {key}");
            values[key[2..]] = args[i + 1];
        }

        var defaults = new BrokerOptions();
        var options = new BrokerOptions
        {
            Port = values.Remove("port", out string? port) ? ParseRange(port, "port", 1, 65535) : defaults.Port,
            Storage = values.Remove("storage", out string? storage) ? ParseStorage(storage) : defaults.Storage,
            DatabasePath = values.Remove("db", out string? db) ? RequireValue(db, "db") : defaults.DatabasePath,
            Workers = values.Remove("workers", out string? workers) ? ParseRange(workers, "workers", 1, 64) : defaults.Workers
        };

        if (values.Count > 0)
            throw new ArgumentException($"Unknown argument: --{values.Keys.First()}");
        return options;
    }

    private static int ParseRange(string value, string name, int min, int max)
    {
        if (int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out int number)
            && number >= min && number <= max)
            return number;
        throw new ArgumentException($"--{name} must be an integer between {min} and {max}");
    }

    private static StorageMode ParseStorage(string value) => value.ToLowerInvariant() switch
    {
        "persistent" => StorageMode.Persistent,
        "transient" => StorageMode.Transient,
        _ => throw new ArgumentException("--storage must be 'persistent' or 'transient'")
    };

    private static string RequireValue(string value, string name) =>
        string.IsNullOrWhiteSpace(value) ? throw new ArgumentException($"--{name} cannot be empty") : value;
}
```

- [ ] **Step 8: Creează `broker/Logging/BrokerLog.cs`**

```csharp
namespace Broker.Logging;

public interface IBrokerLog
{
    void Info(string message);
    void Error(string message);
}

public sealed class ConsoleBrokerLog : IBrokerLog
{
    public void Info(string message) => Console.WriteLine($"{DateTime.Now:HH:mm:ss} {message}");

    public void Error(string message) => Console.WriteLine($"{DateTime.Now:HH:mm:ss} [ERROR] {message}");
}
```

- [ ] **Step 9: Rulează testele**

Run: `dotnet test broker.Tests --nologo`
Expected: PASS, 12 teste (2 Fact + 9 Theory + 1 Fact).

- [ ] **Step 10: Checkpoint** — de comis: `broker/Broker.csproj`, `broker/Program.cs`, `broker/BrokerOptions.cs`, `broker/Logging/BrokerLog.cs`, `broker.Tests/*`.

---

### Task 3: Validarea cererilor

**Files:**
- Create: `broker/Validation/RequestValidator.cs`
- Test: `broker.Tests/RequestValidatorTests.cs`

**Interfaces:**
- Consumes: tipurile generate `Broker.Contracts.PublishRequest`, `SubscriptionRequest`, `ListenRequest`, `AckRequest`.
- Produces: `Broker.Validation.RequestValidator` cu `IReadOnlyList<string> ValidatePublish(PublishRequest)`, `ValidateSubscription(SubscriptionRequest)`, `ValidateListen(ListenRequest)`, `ValidateAck(AckRequest)`; constante `MaxNameLength = 64`, `MaxContentLength = 4096`. Codurile au forma `{FIELD}_REQUIRED|_TOO_LONG|_INVALID_CHARS` și `CONTENT_REQUIRED|CONTENT_TOO_LONG`, în ordinea câmpurilor.

- [ ] **Step 1: Scrie testele care pică — `broker.Tests/RequestValidatorTests.cs`**

```csharp
using Broker.Contracts;
using Broker.Validation;

namespace Broker.Tests;

public sealed class RequestValidatorTests
{
    private readonly RequestValidator _validator = new();

    private static PublishRequest Publish(
        string id = "3f0c2a1e-9b7d-4c55-8e0f-1a2b3c4d5e6f",
        string topic = "sport",
        string content = "goal") =>
        new() { MessageId = id, Topic = topic, Content = content };

    [Fact]
    public void Valid_publish_has_no_errors() =>
        Assert.Empty(_validator.ValidatePublish(Publish()));

    [Fact]
    public void Missing_fields_report_every_error_in_field_order() =>
        Assert.Equal(
            new[] { "MESSAGE_ID_REQUIRED", "TOPIC_REQUIRED", "CONTENT_REQUIRED" },
            _validator.ValidatePublish(Publish("", "", "")));

    [Theory]
    [InlineData("   ")]
    [InlineData("\t\n")]
    public void Whitespace_only_content_is_required_error(string content) =>
        Assert.Equal(new[] { "CONTENT_REQUIRED" }, _validator.ValidatePublish(Publish(content: content)));

    [Theory]
    [InlineData(64, null)]
    [InlineData(65, "TOPIC_TOO_LONG")]
    public void Topic_length_limit_is_64(int length, string? expected) =>
        AssertSingleOrNone(expected, _validator.ValidatePublish(Publish(topic: new string('t', length))));

    [Theory]
    [InlineData(64, null)]
    [InlineData(65, "MESSAGE_ID_TOO_LONG")]
    public void Message_id_length_limit_is_64(int length, string? expected) =>
        AssertSingleOrNone(expected, _validator.ValidatePublish(Publish(id: new string('m', length))));

    [Theory]
    [InlineData(4096, null)]
    [InlineData(4097, "CONTENT_TOO_LONG")]
    public void Content_length_limit_is_4096(int length, string? expected) =>
        AssertSingleOrNone(expected, _validator.ValidatePublish(Publish(content: new string('c', length))));

    [Fact]
    public void Content_length_counts_unicode_code_points_not_utf16_units()
    {
        string content = string.Concat(Enumerable.Repeat("😀", 4096));

        Assert.Empty(_validator.ValidatePublish(Publish(content: content)));
        Assert.Equal(new[] { "CONTENT_TOO_LONG" }, _validator.ValidatePublish(Publish(content: content + "😀")));
    }

    [Theory]
    [InlineData("sport")]
    [InlineData("știri.locale")]
    [InlineData("a_b-c.1")]
    [InlineData("ĂÎÂȘȚ")]
    public void Topic_accepts_letters_digits_dot_underscore_dash(string topic) =>
        Assert.Empty(_validator.ValidatePublish(Publish(topic: topic)));

    [Theory]
    [InlineData("bad topic")]
    [InlineData("sport!")]
    [InlineData("sport\n")]
    [InlineData("a/b")]
    public void Topic_rejects_other_characters(string topic) =>
        Assert.Equal(new[] { "TOPIC_INVALID_CHARS" }, _validator.ValidatePublish(Publish(topic: topic)));

    [Fact]
    public void Subscription_requires_subscriber_and_topic() =>
        Assert.Equal(
            new[] { "SUBSCRIBER_ID_REQUIRED", "TOPIC_REQUIRED" },
            _validator.ValidateSubscription(new SubscriptionRequest()));

    [Fact]
    public void Listen_rejects_subscriber_id_with_spaces() =>
        Assert.Equal(
            new[] { "SUBSCRIBER_ID_INVALID_CHARS" },
            _validator.ValidateListen(new ListenRequest { SubscriberId = "java pc" }));

    [Fact]
    public void Ack_requires_subscriber_and_message_id() =>
        Assert.Equal(
            new[] { "SUBSCRIBER_ID_REQUIRED", "MESSAGE_ID_REQUIRED" },
            _validator.ValidateAck(new AckRequest()));

    private static void AssertSingleOrNone(string? expected, IReadOnlyList<string> errors)
    {
        if (expected is null)
            Assert.Empty(errors);
        else
            Assert.Equal(new[] { expected }, errors);
    }
}
```

- [ ] **Step 2: Rulează ca să confirmi că pică**

Run: `dotnet test broker.Tests --nologo`
Expected: eroare de compilare, `RequestValidator` nu există.

- [ ] **Step 3: Creează `broker/Validation/RequestValidator.cs`**

```csharp
using System.Text.RegularExpressions;
using Broker.Contracts;

namespace Broker.Validation;

public sealed partial class RequestValidator
{
    public const int MaxNameLength = 64;
    public const int MaxContentLength = 4096;

    public IReadOnlyList<string> ValidatePublish(PublishRequest request)
    {
        var errors = new List<string>();
        CheckName(request.MessageId, "MESSAGE_ID", errors);
        CheckName(request.Topic, "TOPIC", errors);
        CheckContent(request.Content, errors);
        return errors;
    }

    public IReadOnlyList<string> ValidateSubscription(SubscriptionRequest request)
    {
        var errors = new List<string>();
        CheckName(request.SubscriberId, "SUBSCRIBER_ID", errors);
        CheckName(request.Topic, "TOPIC", errors);
        return errors;
    }

    public IReadOnlyList<string> ValidateListen(ListenRequest request)
    {
        var errors = new List<string>();
        CheckName(request.SubscriberId, "SUBSCRIBER_ID", errors);
        return errors;
    }

    public IReadOnlyList<string> ValidateAck(AckRequest request)
    {
        var errors = new List<string>();
        CheckName(request.SubscriberId, "SUBSCRIBER_ID", errors);
        CheckName(request.MessageId, "MESSAGE_ID", errors);
        return errors;
    }

    private static void CheckName(string value, string field, List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(value))
            errors.Add($"{field}_REQUIRED");
        else if (CountCodePoints(value) > MaxNameLength)
            errors.Add($"{field}_TOO_LONG");
        else if (!NamePattern().IsMatch(value))
            errors.Add($"{field}_INVALID_CHARS");
    }

    private static void CheckContent(string value, List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(value))
            errors.Add("CONTENT_REQUIRED");
        else if (CountCodePoints(value) > MaxContentLength)
            errors.Add("CONTENT_TOO_LONG");
    }

    private static int CountCodePoints(string value) => value.EnumerateRunes().Count();

    [GeneratedRegex(@"^[\p{L}\p{N}._-]+\z")]
    private static partial Regex NamePattern();
}
```

- [ ] **Step 4: Rulează testele**

Run: `dotnet test broker.Tests --nologo`
Expected: PASS (toate testele din Task 2 și Task 3).

- [ ] **Step 5: Checkpoint** — de comis: `broker/Validation/RequestValidator.cs`, `broker.Tests/RequestValidatorTests.cs`.

---

### Task 4: Contractele de stocare, `InMemoryStore` și suita de contract

**Files:**
- Create: `broker/Storage/StoreContracts.cs`
- Create: `broker/Storage/InMemoryStore.cs`
- Test: `broker.Tests/StoreContractTests.cs`, `broker.Tests/InMemoryStoreTests.cs`

**Interfaces:**
- Produces (namespace `Broker.Storage`):
  - `Statuses { Pending, Delivered, Dead; static string ForMessage(int total, int pending, int delivered) }`
  - `Timestamps.Format(DateTimeOffset) -> string` (UTC, format `O`)
  - records `StoredMessage(MessageId, Topic, Content)`, `PublishResult(bool Inserted, IReadOnlyList<string> Recipients)`, `PendingDelivery(MessageId, SubscriberId, Topic, Content, int RetryCount)`, `MessageRow`, `DeliveryRow`, `DeadLetterRow`, `SubscriptionRow`, `BrokerSnapshot`; `enum AckOutcome { Acked, Duplicate, NotFound }`
  - `IMessageStore.AddMessageAsync(StoredMessage, DateTimeOffset createdAt, CancellationToken = default) -> Task<PublishResult>`
  - `ISubscriptionStore.AddSubscriptionAsync(string subscriberId, string topic, DateTimeOffset createdAt, CancellationToken = default) -> Task`; `RemoveSubscriptionAsync(string, string, CancellationToken = default) -> Task<bool>`
  - `IDeliveryStore.GetDueDeliveriesAsync(string subscriberId, DateTimeOffset attemptedAtOrBefore, CancellationToken = default) -> Task<IReadOnlyList<PendingDelivery>>`; `RecordAttemptAsync(string messageId, string subscriberId, DateTimeOffset attemptedAt, CancellationToken = default) -> Task`; `AcknowledgeAsync(string messageId, string subscriberId, CancellationToken = default) -> Task<AckOutcome>`
  - `IDeadLetterStore.MoveToDeadLetterAsync(PendingDelivery, string reason, DateTimeOffset at, CancellationToken = default) -> Task<bool>`; `AddInvalidPayloadAsync(string rawPayload, string reason, DateTimeOffset at, CancellationToken = default) -> Task`
  - `IStateReader.ReadSnapshotAsync(int limit, CancellationToken = default) -> Task<BrokerSnapshot>`
  - `IBrokerStore : all of the above` + `InitializeAsync(CancellationToken = default)`
  - `InMemoryStore : IBrokerStore`
  - test base `StoreContractTests` (abstract, `CreateStore()`), reused by Task 5.

- [ ] **Step 1: Creează `broker/Storage/StoreContracts.cs`**

```csharp
using System.Globalization;

namespace Broker.Storage;

public static class Statuses
{
    public const string Pending = "PENDING";
    public const string Delivered = "DELIVERED";
    public const string Dead = "DEAD";

    public static string ForMessage(int total, int pending, int delivered) =>
        total == 0 || pending > 0 ? Pending : delivered > 0 ? Delivered : Dead;
}

public static class Timestamps
{
    public static string Format(DateTimeOffset value) =>
        value.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
}

public sealed record StoredMessage(string MessageId, string Topic, string Content);

public sealed record PublishResult(bool Inserted, IReadOnlyList<string> Recipients);

public sealed record PendingDelivery(string MessageId, string SubscriberId, string Topic, string Content, int RetryCount);

public enum AckOutcome
{
    Acked,
    Duplicate,
    NotFound
}

public sealed record MessageRow(string MessageId, string Topic, string Content, string Status, string CreatedAtUtc);

public sealed record DeliveryRow(string MessageId, string SubscriberId, string Status, int RetryCount, string? LastError);

public sealed record DeadLetterRow(
    long Id,
    string? MessageId,
    string? SubscriberId,
    string? Topic,
    string? RawPayload,
    string Reason,
    int RetryCount,
    string CreatedAtUtc);

public sealed record SubscriptionRow(string SubscriberId, string Topic);

public sealed record BrokerSnapshot(
    IReadOnlyList<MessageRow> Messages,
    IReadOnlyList<DeliveryRow> Deliveries,
    IReadOnlyList<DeadLetterRow> DeadLetters,
    IReadOnlyList<SubscriptionRow> Subscriptions);

public interface IMessageStore
{
    Task<PublishResult> AddMessageAsync(StoredMessage message, DateTimeOffset createdAt, CancellationToken cancellationToken = default);
}

public interface ISubscriptionStore
{
    Task AddSubscriptionAsync(string subscriberId, string topic, DateTimeOffset createdAt, CancellationToken cancellationToken = default);
    Task<bool> RemoveSubscriptionAsync(string subscriberId, string topic, CancellationToken cancellationToken = default);
}

public interface IDeliveryStore
{
    Task<IReadOnlyList<PendingDelivery>> GetDueDeliveriesAsync(string subscriberId, DateTimeOffset attemptedAtOrBefore, CancellationToken cancellationToken = default);
    Task RecordAttemptAsync(string messageId, string subscriberId, DateTimeOffset attemptedAt, CancellationToken cancellationToken = default);
    Task<AckOutcome> AcknowledgeAsync(string messageId, string subscriberId, CancellationToken cancellationToken = default);
}

public interface IDeadLetterStore
{
    Task<bool> MoveToDeadLetterAsync(PendingDelivery delivery, string reason, DateTimeOffset at, CancellationToken cancellationToken = default);
    Task AddInvalidPayloadAsync(string rawPayload, string reason, DateTimeOffset at, CancellationToken cancellationToken = default);
}

public interface IStateReader
{
    Task<BrokerSnapshot> ReadSnapshotAsync(int limit, CancellationToken cancellationToken = default);
}

public interface IBrokerStore : IMessageStore, ISubscriptionStore, IDeliveryStore, IDeadLetterStore, IStateReader
{
    Task InitializeAsync(CancellationToken cancellationToken = default);
}
```

- [ ] **Step 2: Scrie suita de contract care pică — `broker.Tests/StoreContractTests.cs`**

```csharp
using Broker.Storage;

namespace Broker.Tests;

public abstract class StoreContractTests : IDisposable
{
    protected static readonly DateTimeOffset T0 = new(2026, 10, 12, 10, 0, 0, TimeSpan.Zero);

    protected StoreContractTests()
    {
        Store = CreateStore();
        Store.InitializeAsync().GetAwaiter().GetResult();
    }

    protected IBrokerStore Store { get; }

    protected abstract IBrokerStore CreateStore();

    public virtual void Dispose()
    {
    }

    private static StoredMessage Message(string id, string topic = "sport", string content = "goal") =>
        new(id, topic, content);

    private Task<BrokerSnapshot> SnapshotAsync() => Store.ReadSnapshotAsync(1000);

    private async Task<string> MessageStatusAsync(string messageId) =>
        (await SnapshotAsync()).Messages.Single(m => m.MessageId == messageId).Status;

    private async Task<DeliveryRow> DeliveryAsync(string messageId, string subscriberId) =>
        (await SnapshotAsync()).Deliveries.Single(d => d.MessageId == messageId && d.SubscriberId == subscriberId);

    [Fact]
    public async Task Publish_creates_one_pending_delivery_per_current_subscriber()
    {
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddSubscriptionAsync("sub-b", "sport", T0);
        await Store.AddSubscriptionAsync("sub-c", "news", T0);

        PublishResult result = await Store.AddMessageAsync(Message("m1"), T0);

        Assert.True(result.Inserted);
        Assert.Equal(new[] { "sub-a", "sub-b" }, result.Recipients.OrderBy(id => id));
        IReadOnlyList<DeliveryRow> deliveries = (await SnapshotAsync()).Deliveries;
        Assert.Equal(2, deliveries.Count);
        Assert.All(deliveries, d =>
        {
            Assert.Equal(Statuses.Pending, d.Status);
            Assert.Equal(0, d.RetryCount);
        });
        Assert.Equal(Statuses.Pending, await MessageStatusAsync("m1"));
    }

    [Fact]
    public async Task Publish_with_existing_id_is_rejected_and_keeps_original()
    {
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddMessageAsync(Message("m1", content: "first"), T0);

        PublishResult second = await Store.AddMessageAsync(Message("m1", content: "second"), T0);

        Assert.False(second.Inserted);
        Assert.Empty(second.Recipients);
        BrokerSnapshot snapshot = await SnapshotAsync();
        Assert.Equal("first", Assert.Single(snapshot.Messages).Content);
        Assert.Single(snapshot.Deliveries);
    }

    [Fact]
    public async Task Publish_without_subscribers_is_pending_without_deliveries()
    {
        PublishResult result = await Store.AddMessageAsync(Message("m1"), T0);

        Assert.True(result.Inserted);
        Assert.Empty(result.Recipients);
        Assert.Empty((await SnapshotAsync()).Deliveries);
        Assert.Equal(Statuses.Pending, await MessageStatusAsync("m1"));
    }

    [Fact]
    public async Task Subscribe_creates_backlog_and_reopens_delivered_message()
    {
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddMessageAsync(Message("m1"), T0);
        Assert.Equal(AckOutcome.Acked, await Store.AcknowledgeAsync("m1", "sub-a"));
        Assert.Equal(Statuses.Delivered, await MessageStatusAsync("m1"));

        await Store.AddSubscriptionAsync("sub-b", "sport", T0.AddSeconds(1));

        Assert.Equal(Statuses.Pending, (await DeliveryAsync("m1", "sub-b")).Status);
        Assert.Equal(Statuses.Pending, await MessageStatusAsync("m1"));
    }

    [Fact]
    public async Task Subscribing_twice_does_not_duplicate_anything()
    {
        await Store.AddMessageAsync(Message("m1"), T0);

        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);

        BrokerSnapshot snapshot = await SnapshotAsync();
        Assert.Single(snapshot.Deliveries);
        Assert.Single(snapshot.Subscriptions);
    }

    [Fact]
    public async Task Due_deliveries_are_ordered_and_respect_ack_timeout()
    {
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddMessageAsync(Message("m1", content: "one"), T0);
        await Store.AddMessageAsync(Message("m2", content: "two"), T0);
        await Store.AddMessageAsync(Message("m3", content: "three"), T0.AddSeconds(1));

        IReadOnlyList<PendingDelivery> due = await Store.GetDueDeliveriesAsync("sub-a", T0);
        Assert.Equal(new[] { "m1", "m2", "m3" }, due.Select(d => d.MessageId));
        Assert.Equal(("sport", "one", 0), (due[0].Topic, due[0].Content, due[0].RetryCount));

        await Store.RecordAttemptAsync("m1", "sub-a", T0.AddSeconds(10));

        Assert.Equal(
            new[] { "m2", "m3" },
            (await Store.GetDueDeliveriesAsync("sub-a", T0.AddSeconds(9))).Select(d => d.MessageId));
        IReadOnlyList<PendingDelivery> later = await Store.GetDueDeliveriesAsync("sub-a", T0.AddSeconds(10));
        Assert.Equal(new[] { "m1", "m2", "m3" }, later.Select(d => d.MessageId));
        Assert.Equal(1, later[0].RetryCount);
    }

    [Fact]
    public async Task Due_deliveries_pause_after_unsubscribe_and_resume_after_resubscribe()
    {
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddMessageAsync(Message("m1"), T0);

        Assert.True(await Store.RemoveSubscriptionAsync("sub-a", "sport"));
        Assert.False(await Store.RemoveSubscriptionAsync("sub-a", "sport"));
        Assert.Empty(await Store.GetDueDeliveriesAsync("sub-a", T0));
        Assert.Equal(Statuses.Pending, (await DeliveryAsync("m1", "sub-a")).Status);

        await Store.AddSubscriptionAsync("sub-a", "sport", T0);

        Assert.Equal("m1", Assert.Single(await Store.GetDueDeliveriesAsync("sub-a", T0)).MessageId);
    }

    [Fact]
    public async Task Acknowledge_marks_delivered_then_reports_duplicate()
    {
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddMessageAsync(Message("m1"), T0);

        Assert.Equal(AckOutcome.Acked, await Store.AcknowledgeAsync("m1", "sub-a"));
        Assert.Equal(AckOutcome.Duplicate, await Store.AcknowledgeAsync("m1", "sub-a"));

        Assert.Equal(Statuses.Delivered, (await DeliveryAsync("m1", "sub-a")).Status);
        Assert.Equal(Statuses.Delivered, await MessageStatusAsync("m1"));
        Assert.Empty(await Store.GetDueDeliveriesAsync("sub-a", T0.AddHours(1)));
    }

    [Fact]
    public async Task Acknowledge_unknown_delivery_is_not_found()
    {
        await Store.AddMessageAsync(Message("m1"), T0);

        Assert.Equal(AckOutcome.NotFound, await Store.AcknowledgeAsync("m1", "nobody"));
        Assert.Equal(AckOutcome.NotFound, await Store.AcknowledgeAsync("missing", "nobody"));
    }

    [Fact]
    public async Task Dead_letter_marks_delivery_dead_writes_row_and_updates_status()
    {
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddMessageAsync(Message("m1"), T0);
        for (int i = 0; i < 3; i++)
            await Store.RecordAttemptAsync("m1", "sub-a", T0.AddSeconds(i));
        PendingDelivery delivery = Assert.Single(await Store.GetDueDeliveriesAsync("sub-a", T0.AddMinutes(1)));
        Assert.Equal(3, delivery.RetryCount);

        Assert.True(await Store.MoveToDeadLetterAsync(delivery, "ACK_NOT_RECEIVED_AFTER_MAX_RETRIES", T0.AddMinutes(1)));

        BrokerSnapshot snapshot = await SnapshotAsync();
        Assert.Equal(Statuses.Dead, Assert.Single(snapshot.Deliveries).Status);
        Assert.Equal(Statuses.Dead, Assert.Single(snapshot.Messages).Status);
        DeadLetterRow row = Assert.Single(snapshot.DeadLetters);
        Assert.Equal(
            ("m1", "sub-a", "sport", "goal", "ACK_NOT_RECEIVED_AFTER_MAX_RETRIES", 3),
            (row.MessageId, row.SubscriberId, row.Topic, row.RawPayload, row.Reason, row.RetryCount));
        Assert.Equal(AckOutcome.NotFound, await Store.AcknowledgeAsync("m1", "sub-a"));
        Assert.Empty(await Store.GetDueDeliveriesAsync("sub-a", T0.AddHours(1)));
    }

    [Fact]
    public async Task Dead_letter_is_refused_when_delivery_changed_meanwhile()
    {
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddMessageAsync(Message("m1"), T0);
        PendingDelivery stale = Assert.Single(await Store.GetDueDeliveriesAsync("sub-a", T0));
        await Store.RecordAttemptAsync("m1", "sub-a", T0);

        Assert.False(await Store.MoveToDeadLetterAsync(stale, "X", T0));

        PendingDelivery current = Assert.Single(await Store.GetDueDeliveriesAsync("sub-a", T0));
        await Store.AcknowledgeAsync("m1", "sub-a");

        Assert.False(await Store.MoveToDeadLetterAsync(current, "X", T0));
        Assert.Empty((await SnapshotAsync()).DeadLetters);
    }

    [Fact]
    public async Task Message_with_one_delivered_and_one_dead_delivery_is_delivered()
    {
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddSubscriptionAsync("sub-b", "sport", T0);
        await Store.AddMessageAsync(Message("m1"), T0);
        await Store.AcknowledgeAsync("m1", "sub-a");
        PendingDelivery forB = Assert.Single(await Store.GetDueDeliveriesAsync("sub-b", T0));

        await Store.MoveToDeadLetterAsync(forB, "X", T0);

        Assert.Equal(Statuses.Delivered, await MessageStatusAsync("m1"));
    }

    [Fact]
    public async Task Invalid_payload_is_stored_without_message_reference()
    {
        await Store.AddInvalidPayloadAsync("{\"topic\":\"\"}", "TOPIC_REQUIRED", T0);

        BrokerSnapshot snapshot = await SnapshotAsync();
        DeadLetterRow row = Assert.Single(snapshot.DeadLetters);
        Assert.Null(row.MessageId);
        Assert.Null(row.SubscriberId);
        Assert.Equal(("{\"topic\":\"\"}", "TOPIC_REQUIRED", 0), (row.RawPayload, row.Reason, row.RetryCount));
        Assert.Empty(snapshot.Messages);
    }

    [Fact]
    public async Task Snapshot_is_newest_first_and_limited()
    {
        for (int i = 1; i <= 5; i++)
            await Store.AddMessageAsync(Message($"m{i}"), T0.AddSeconds(i));

        BrokerSnapshot snapshot = await Store.ReadSnapshotAsync(3);

        Assert.Equal(new[] { "m5", "m4", "m3" }, snapshot.Messages.Select(m => m.MessageId));
    }

    [Fact]
    public async Task Concurrent_publish_and_subscribe_never_lose_a_delivery()
    {
        IEnumerable<Task> publishes = Enumerable.Range(0, 50)
            .Select(i => Task.Run(() => Store.AddMessageAsync(Message($"m{i}"), T0.AddMilliseconds(i))));
        IEnumerable<Task> subscribes = Enumerable.Range(0, 10)
            .Select(i => Task.Run(() => Store.AddSubscriptionAsync($"sub-{i}", "sport", T0)));

        await Task.WhenAll(publishes.Concat(subscribes));

        Assert.Equal(500, (await SnapshotAsync()).Deliveries.Count);
    }
}
```

- [ ] **Step 3: Creează `broker.Tests/InMemoryStoreTests.cs`**

```csharp
using Broker.Storage;

namespace Broker.Tests;

public sealed class InMemoryStoreTests : StoreContractTests
{
    protected override IBrokerStore CreateStore() => new InMemoryStore();
}
```

- [ ] **Step 4: Rulează ca să confirmi că pică**

Run: `dotnet test broker.Tests --nologo`
Expected: eroare de compilare, `InMemoryStore` nu există.

- [ ] **Step 5: Creează `broker/Storage/InMemoryStore.cs`**

```csharp
using System.Collections.Concurrent;

namespace Broker.Storage;

public sealed class InMemoryStore : IBrokerStore
{
    private readonly object _gate = new();
    private readonly ConcurrentDictionary<string, MessageEntry> _messages = new();
    private readonly ConcurrentDictionary<(string SubscriberId, string Topic), long> _subscriptions = new();
    private readonly ConcurrentDictionary<(string MessageId, string SubscriberId), DeliveryEntry> _deliveries = new();
    private readonly ConcurrentQueue<DeadLetterRow> _deadLetters = new();
    private long _sequence;
    private long _deadLetterId;

    public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task<PublishResult> AddMessageAsync(StoredMessage message, DateTimeOffset createdAt, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_messages.TryAdd(message.MessageId, new MessageEntry(message, Timestamps.Format(createdAt), ++_sequence)))
                return Task.FromResult(new PublishResult(false, []));

            List<string> recipients = _subscriptions.Keys
                .Where(key => key.Topic == message.Topic)
                .Select(key => key.SubscriberId)
                .ToList();
            foreach (string subscriberId in recipients)
                AddDelivery(message.MessageId, subscriberId);
            return Task.FromResult(new PublishResult(true, recipients));
        }
    }

    public Task AddSubscriptionAsync(string subscriberId, string topic, DateTimeOffset createdAt, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            _subscriptions.TryAdd((subscriberId, topic), ++_sequence);
            foreach (MessageEntry message in _messages.Values.Where(m => m.Topic == topic))
            {
                AddDelivery(message.MessageId, subscriberId);
                RefreshStatus(message);
            }
        }
        return Task.CompletedTask;
    }

    public Task<bool> RemoveSubscriptionAsync(string subscriberId, string topic, CancellationToken cancellationToken = default)
    {
        lock (_gate)
            return Task.FromResult(_subscriptions.TryRemove((subscriberId, topic), out _));
    }

    public Task<IReadOnlyList<PendingDelivery>> GetDueDeliveriesAsync(string subscriberId, DateTimeOffset attemptedAtOrBefore, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            List<PendingDelivery> due = _deliveries.Values
                .Where(d => d.SubscriberId == subscriberId
                    && d.Status == Statuses.Pending
                    && (d.LastAttemptAt is null || d.LastAttemptAt <= attemptedAtOrBefore))
                .Select(d => (Delivery: d, Message: _messages[d.MessageId]))
                .Where(x => _subscriptions.ContainsKey((subscriberId, x.Message.Topic)))
                .OrderBy(x => x.Message.Sequence)
                .Select(x => new PendingDelivery(x.Message.MessageId, subscriberId, x.Message.Topic, x.Message.Content, x.Delivery.RetryCount))
                .ToList();
            return Task.FromResult<IReadOnlyList<PendingDelivery>>(due);
        }
    }

    public Task RecordAttemptAsync(string messageId, string subscriberId, DateTimeOffset attemptedAt, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_deliveries.TryGetValue((messageId, subscriberId), out DeliveryEntry? delivery) && delivery.Status == Statuses.Pending)
            {
                delivery.RetryCount++;
                delivery.LastAttemptAt = attemptedAt;
            }
        }
        return Task.CompletedTask;
    }

    public Task<AckOutcome> AcknowledgeAsync(string messageId, string subscriberId, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_deliveries.TryGetValue((messageId, subscriberId), out DeliveryEntry? delivery))
                return Task.FromResult(AckOutcome.NotFound);
            if (delivery.Status == Statuses.Delivered)
                return Task.FromResult(AckOutcome.Duplicate);
            if (delivery.Status != Statuses.Pending)
                return Task.FromResult(AckOutcome.NotFound);

            delivery.Status = Statuses.Delivered;
            delivery.LastError = null;
            RefreshStatus(_messages[messageId]);
            return Task.FromResult(AckOutcome.Acked);
        }
    }

    public Task<bool> MoveToDeadLetterAsync(PendingDelivery delivery, string reason, DateTimeOffset at, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_deliveries.TryGetValue((delivery.MessageId, delivery.SubscriberId), out DeliveryEntry? entry)
                || entry.Status != Statuses.Pending
                || entry.RetryCount != delivery.RetryCount)
                return Task.FromResult(false);

            entry.Status = Statuses.Dead;
            entry.LastError = reason;
            _deadLetters.Enqueue(new DeadLetterRow(
                ++_deadLetterId, delivery.MessageId, delivery.SubscriberId, delivery.Topic,
                delivery.Content, reason, delivery.RetryCount, Timestamps.Format(at)));
            RefreshStatus(_messages[delivery.MessageId]);
            return Task.FromResult(true);
        }
    }

    public Task AddInvalidPayloadAsync(string rawPayload, string reason, DateTimeOffset at, CancellationToken cancellationToken = default)
    {
        lock (_gate)
            _deadLetters.Enqueue(new DeadLetterRow(++_deadLetterId, null, null, null, rawPayload, reason, 0, Timestamps.Format(at)));
        return Task.CompletedTask;
    }

    public Task<BrokerSnapshot> ReadSnapshotAsync(int limit, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var snapshot = new BrokerSnapshot(
                _messages.Values.OrderByDescending(m => m.Sequence).Take(limit)
                    .Select(m => new MessageRow(m.MessageId, m.Topic, m.Content, m.Status, m.CreatedAtUtc)).ToList(),
                _deliveries.Values.OrderByDescending(d => d.Sequence).Take(limit)
                    .Select(d => new DeliveryRow(d.MessageId, d.SubscriberId, d.Status, d.RetryCount, d.LastError)).ToList(),
                _deadLetters.OrderByDescending(d => d.Id).Take(limit).ToList(),
                _subscriptions.OrderByDescending(s => s.Value).Take(limit)
                    .Select(s => new SubscriptionRow(s.Key.SubscriberId, s.Key.Topic)).ToList());
            return Task.FromResult(snapshot);
        }
    }

    private void AddDelivery(string messageId, string subscriberId)
    {
        if (!_deliveries.ContainsKey((messageId, subscriberId)))
            _deliveries[(messageId, subscriberId)] = new DeliveryEntry(messageId, subscriberId, ++_sequence);
    }

    private void RefreshStatus(MessageEntry message)
    {
        List<DeliveryEntry> deliveries = _deliveries.Values.Where(d => d.MessageId == message.MessageId).ToList();
        message.Status = Statuses.ForMessage(
            deliveries.Count,
            deliveries.Count(d => d.Status == Statuses.Pending),
            deliveries.Count(d => d.Status == Statuses.Delivered));
    }

    private sealed class MessageEntry(StoredMessage message, string createdAtUtc, long sequence)
    {
        public string MessageId { get; } = message.MessageId;
        public string Topic { get; } = message.Topic;
        public string Content { get; } = message.Content;
        public string CreatedAtUtc { get; } = createdAtUtc;
        public long Sequence { get; } = sequence;
        public string Status { get; set; } = Statuses.Pending;
    }

    private sealed class DeliveryEntry(string messageId, string subscriberId, long sequence)
    {
        public string MessageId { get; } = messageId;
        public string SubscriberId { get; } = subscriberId;
        public long Sequence { get; } = sequence;
        public string Status { get; set; } = Statuses.Pending;
        public int RetryCount { get; set; }
        public DateTimeOffset? LastAttemptAt { get; set; }
        public string? LastError { get; set; }
    }
}
```

- [ ] **Step 6: Rulează testele**

Run: `dotnet test broker.Tests --nologo`
Expected: PASS, inclusiv cele 15 teste de contract pe `InMemoryStoreTests`.

- [ ] **Step 7: Checkpoint** — de comis: `broker/Storage/StoreContracts.cs`, `broker/Storage/InMemoryStore.cs`, `broker.Tests/StoreContractTests.cs`, `broker.Tests/InMemoryStoreTests.cs`.

---

### Task 5: `SqliteStore` (persistent), aceeași suită de contract

**Files:**
- Create: `broker/Storage/SqliteStore.cs`
- Test: `broker.Tests/SqliteStoreTests.cs`

**Interfaces:**
- Consumes: toate tipurile din `broker/Storage/StoreContracts.cs` (Task 4), `StoreContractTests` (Task 4).
- Produces: `SqliteStore(string databasePath) : IBrokerStore`; schema identică cu lab 1.

- [ ] **Step 1: Scrie testul care pică — `broker.Tests/SqliteStoreTests.cs`**

```csharp
using Broker.Storage;
using Microsoft.Data.Sqlite;

namespace Broker.Tests;

public sealed class SqliteStoreTests : StoreContractTests
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"broker-test-{Guid.NewGuid():N}.db");

    protected override IBrokerStore CreateStore() => new SqliteStore(_path);

    [Fact]
    public async Task Data_survives_a_new_store_instance_on_the_same_file()
    {
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddMessageAsync(new StoredMessage("m1", "sport", "goal"), T0);

        var reopened = new SqliteStore(_path);
        await reopened.InitializeAsync();

        Assert.Equal("m1", Assert.Single(await reopened.GetDueDeliveriesAsync("sub-a", T0)).MessageId);
    }

    public override void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (string suffix in new[] { "", "-wal", "-shm" })
            File.Delete(_path + suffix);
    }
}
```

- [ ] **Step 2: Rulează ca să confirmi că pică**

Run: `dotnet test broker.Tests --nologo`
Expected: eroare de compilare, `SqliteStore` nu există.

- [ ] **Step 3: Creează `broker/Storage/SqliteStore.cs`**

```csharp
using Microsoft.Data.Sqlite;

namespace Broker.Storage;

public sealed class SqliteStore : IBrokerStore
{
    private const string Schema = """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS Messages
        (
            MessageId TEXT PRIMARY KEY,
            Topic TEXT NOT NULL,
            Content TEXT NOT NULL,
            Status TEXT NOT NULL DEFAULT 'PENDING',
            CreatedAt TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS Subscriptions
        (
            SubscriberId TEXT NOT NULL,
            Topic TEXT NOT NULL,
            CreatedAt TEXT NOT NULL,
            PRIMARY KEY (SubscriberId, Topic)
        );
        CREATE TABLE IF NOT EXISTS Deliveries
        (
            MessageId TEXT NOT NULL,
            SubscriberId TEXT NOT NULL,
            Status TEXT NOT NULL DEFAULT 'PENDING',
            RetryCount INTEGER NOT NULL DEFAULT 0,
            LastAttemptAt TEXT,
            LastError TEXT,
            PRIMARY KEY (MessageId, SubscriberId)
        );
        CREATE TABLE IF NOT EXISTS DeadLetters
        (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            MessageId TEXT,
            SubscriberId TEXT,
            Topic TEXT,
            RawPayload TEXT,
            Reason TEXT NOT NULL,
            RetryCount INTEGER NOT NULL DEFAULT 0,
            CreatedAt TEXT NOT NULL
        );
        """;

    private const string DerivedMessageStatus = """
        (SELECT CASE
             WHEN COUNT(*) = 0 THEN 'PENDING'
             WHEN SUM(CASE WHEN d.Status = 'PENDING' THEN 1 ELSE 0 END) > 0 THEN 'PENDING'
             WHEN SUM(CASE WHEN d.Status = 'DELIVERED' THEN 1 ELSE 0 END) > 0 THEN 'DELIVERED'
             ELSE 'DEAD'
         END
         FROM Deliveries d
         WHERE d.MessageId = Messages.MessageId)
        """;

    private readonly string _connectionString;

    public SqliteStore(string databasePath)
    {
        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            DefaultTimeout = 5,
            Pooling = true
        }.ToString();
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await ExecuteAsync(connection, null, Schema, cancellationToken);
    }

    public async Task<PublishResult> AddMessageAsync(StoredMessage message, DateTimeOffset createdAt, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await using SqliteTransaction transaction = connection.BeginTransaction();

        int inserted = await ExecuteAsync(connection, transaction, """
            INSERT OR IGNORE INTO Messages (MessageId, Topic, Content, Status, CreatedAt)
            VALUES ($messageId, $topic, $content, 'PENDING', $createdAt);
            """, cancellationToken,
            ("$messageId", message.MessageId), ("$topic", message.Topic),
            ("$content", message.Content), ("$createdAt", Timestamps.Format(createdAt)));
        if (inserted == 0)
            return new PublishResult(false, []);

        await ExecuteAsync(connection, transaction, """
            INSERT OR IGNORE INTO Deliveries (MessageId, SubscriberId, Status, RetryCount)
            SELECT $messageId, SubscriberId, 'PENDING', 0
            FROM Subscriptions
            WHERE Topic = $topic;
            """, cancellationToken, ("$messageId", message.MessageId), ("$topic", message.Topic));

        List<string> recipients = await QueryAsync(connection, transaction,
            "SELECT SubscriberId FROM Deliveries WHERE MessageId = $messageId ORDER BY SubscriberId;",
            reader => reader.GetString(0), cancellationToken, ("$messageId", message.MessageId));

        await transaction.CommitAsync(cancellationToken);
        return new PublishResult(true, recipients);
    }

    public async Task AddSubscriptionAsync(string subscriberId, string topic, DateTimeOffset createdAt, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await using SqliteTransaction transaction = connection.BeginTransaction();

        await ExecuteAsync(connection, transaction, $"""
            INSERT OR IGNORE INTO Subscriptions (SubscriberId, Topic, CreatedAt)
            VALUES ($subscriberId, $topic, $createdAt);

            INSERT OR IGNORE INTO Deliveries (MessageId, SubscriberId, Status, RetryCount)
            SELECT MessageId, $subscriberId, 'PENDING', 0
            FROM Messages
            WHERE Topic = $topic;

            UPDATE Messages
            SET Status = {DerivedMessageStatus}
            WHERE Topic = $topic;
            """, cancellationToken,
            ("$subscriberId", subscriberId), ("$topic", topic), ("$createdAt", Timestamps.Format(createdAt)));

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<bool> RemoveSubscriptionAsync(string subscriberId, string topic, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        int removed = await ExecuteAsync(connection, null,
            "DELETE FROM Subscriptions WHERE SubscriberId = $subscriberId AND Topic = $topic;",
            cancellationToken, ("$subscriberId", subscriberId), ("$topic", topic));
        return removed == 1;
    }

    public async Task<IReadOnlyList<PendingDelivery>> GetDueDeliveriesAsync(string subscriberId, DateTimeOffset attemptedAtOrBefore, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        return await QueryAsync(connection, null, """
            SELECT d.MessageId, d.SubscriberId, m.Topic, m.Content, d.RetryCount
            FROM Deliveries d
            INNER JOIN Messages m ON m.MessageId = d.MessageId
            INNER JOIN Subscriptions s ON s.SubscriberId = d.SubscriberId AND s.Topic = m.Topic
            WHERE d.SubscriberId = $subscriberId
              AND d.Status = 'PENDING'
              AND (d.LastAttemptAt IS NULL OR d.LastAttemptAt <= $cutoff)
            ORDER BY m.CreatedAt, m.rowid;
            """,
            reader => new PendingDelivery(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt32(4)),
            cancellationToken, ("$subscriberId", subscriberId), ("$cutoff", Timestamps.Format(attemptedAtOrBefore)));
    }

    public async Task RecordAttemptAsync(string messageId, string subscriberId, DateTimeOffset attemptedAt, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await ExecuteAsync(connection, null, """
            UPDATE Deliveries
            SET RetryCount = RetryCount + 1, LastAttemptAt = $attemptedAt, LastError = NULL
            WHERE MessageId = $messageId AND SubscriberId = $subscriberId AND Status = 'PENDING';
            """, cancellationToken,
            ("$messageId", messageId), ("$subscriberId", subscriberId), ("$attemptedAt", Timestamps.Format(attemptedAt)));
    }

    public async Task<AckOutcome> AcknowledgeAsync(string messageId, string subscriberId, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await using SqliteTransaction transaction = connection.BeginTransaction();

        int updated = await ExecuteAsync(connection, transaction, """
            UPDATE Deliveries
            SET Status = 'DELIVERED', LastError = NULL
            WHERE MessageId = $messageId AND SubscriberId = $subscriberId AND Status = 'PENDING';
            """, cancellationToken, ("$messageId", messageId), ("$subscriberId", subscriberId));

        if (updated == 1)
        {
            await RefreshMessageStatusAsync(connection, transaction, messageId, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return AckOutcome.Acked;
        }

        List<string> statuses = await QueryAsync(connection, transaction,
            "SELECT Status FROM Deliveries WHERE MessageId = $messageId AND SubscriberId = $subscriberId;",
            reader => reader.GetString(0), cancellationToken, ("$messageId", messageId), ("$subscriberId", subscriberId));
        return statuses.SingleOrDefault() == Statuses.Delivered ? AckOutcome.Duplicate : AckOutcome.NotFound;
    }

    public async Task<bool> MoveToDeadLetterAsync(PendingDelivery delivery, string reason, DateTimeOffset at, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await using SqliteTransaction transaction = connection.BeginTransaction();

        int updated = await ExecuteAsync(connection, transaction, """
            UPDATE Deliveries
            SET Status = 'DEAD', LastError = $reason
            WHERE MessageId = $messageId AND SubscriberId = $subscriberId
              AND Status = 'PENDING' AND RetryCount = $retryCount;
            """, cancellationToken,
            ("$reason", reason), ("$messageId", delivery.MessageId),
            ("$subscriberId", delivery.SubscriberId), ("$retryCount", delivery.RetryCount));
        if (updated == 0)
            return false;

        await ExecuteAsync(connection, transaction, """
            INSERT INTO DeadLetters (MessageId, SubscriberId, Topic, RawPayload, Reason, RetryCount, CreatedAt)
            VALUES ($messageId, $subscriberId, $topic, $payload, $reason, $retryCount, $createdAt);
            """, cancellationToken,
            ("$messageId", delivery.MessageId), ("$subscriberId", delivery.SubscriberId), ("$topic", delivery.Topic),
            ("$payload", delivery.Content), ("$reason", reason), ("$retryCount", delivery.RetryCount),
            ("$createdAt", Timestamps.Format(at)));
        await RefreshMessageStatusAsync(connection, transaction, delivery.MessageId, cancellationToken);

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    public async Task AddInvalidPayloadAsync(string rawPayload, string reason, DateTimeOffset at, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await ExecuteAsync(connection, null, """
            INSERT INTO DeadLetters (RawPayload, Reason, RetryCount, CreatedAt)
            VALUES ($payload, $reason, 0, $createdAt);
            """, cancellationToken, ("$payload", rawPayload), ("$reason", reason), ("$createdAt", Timestamps.Format(at)));
    }

    public async Task<BrokerSnapshot> ReadSnapshotAsync(int limit, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        (string, object?) limitParameter = ("$limit", limit);

        List<MessageRow> messages = await QueryAsync(connection, null,
            "SELECT MessageId, Topic, Content, Status, CreatedAt FROM Messages ORDER BY CreatedAt DESC, rowid DESC LIMIT $limit;",
            r => new MessageRow(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetString(4)),
            cancellationToken, limitParameter);
        List<DeliveryRow> deliveries = await QueryAsync(connection, null,
            "SELECT MessageId, SubscriberId, Status, RetryCount, LastError FROM Deliveries ORDER BY rowid DESC LIMIT $limit;",
            r => new DeliveryRow(r.GetString(0), r.GetString(1), r.GetString(2), r.GetInt32(3), NullableString(r, 4)),
            cancellationToken, limitParameter);
        List<DeadLetterRow> deadLetters = await QueryAsync(connection, null,
            "SELECT Id, MessageId, SubscriberId, Topic, RawPayload, Reason, RetryCount, CreatedAt FROM DeadLetters ORDER BY Id DESC LIMIT $limit;",
            r => new DeadLetterRow(r.GetInt64(0), NullableString(r, 1), NullableString(r, 2), NullableString(r, 3),
                NullableString(r, 4), r.GetString(5), r.GetInt32(6), r.GetString(7)),
            cancellationToken, limitParameter);
        List<SubscriptionRow> subscriptions = await QueryAsync(connection, null,
            "SELECT SubscriberId, Topic FROM Subscriptions ORDER BY CreatedAt DESC, rowid DESC LIMIT $limit;",
            r => new SubscriptionRow(r.GetString(0), r.GetString(1)),
            cancellationToken, limitParameter);

        return new BrokerSnapshot(messages, deliveries, deadLetters, subscriptions);
    }

    private static Task RefreshMessageStatusAsync(SqliteConnection connection, SqliteTransaction transaction, string messageId, CancellationToken cancellationToken) =>
        ExecuteAsync(connection, transaction,
            $"UPDATE Messages SET Status = {DerivedMessageStatus} WHERE MessageId = $messageId;",
            cancellationToken, ("$messageId", messageId));

    private async Task<SqliteConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static SqliteCommand CreateCommand(SqliteConnection connection, SqliteTransaction? transaction, string sql, (string Name, object? Value)[] parameters)
    {
        SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        foreach ((string name, object? value) in parameters)
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        return command;
    }

    private static async Task<int> ExecuteAsync(SqliteConnection connection, SqliteTransaction? transaction, string sql, CancellationToken cancellationToken, params (string Name, object? Value)[] parameters)
    {
        await using SqliteCommand command = CreateCommand(connection, transaction, sql, parameters);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<List<T>> QueryAsync<T>(SqliteConnection connection, SqliteTransaction? transaction, string sql, Func<SqliteDataReader, T> map, CancellationToken cancellationToken, params (string Name, object? Value)[] parameters)
    {
        await using SqliteCommand command = CreateCommand(connection, transaction, sql, parameters);
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<T>();
        while (await reader.ReadAsync(cancellationToken))
            rows.Add(map(reader));
        return rows;
    }

    private static string? NullableString(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
}
```

- [ ] **Step 4: Rulează testele**

Run: `dotnet test broker.Tests --nologo`
Expected: PASS. Suita de contract rulează de două ori (In-memory și SQLite), plus `Data_survives_a_new_store_instance_on_the_same_file`.

- [ ] **Step 5: Checkpoint** — de comis: `broker/Storage/SqliteStore.cs`, `broker.Tests/SqliteStoreTests.cs`.

---

### Task 6: Nucleul de livrare — sesiuni, registry, coadă de joburi, procesor

**Files:**
- Create: `broker/Delivery/SubscriberSession.cs`
- Create: `broker/Delivery/SubscriberRegistry.cs`
- Create: `broker/Delivery/DeliveryJobQueue.cs`
- Create: `broker/Delivery/DeliveryProcessor.cs`
- Test: `broker.Tests/SubscriberRegistryTests.cs`, `broker.Tests/DeliveryJobQueueTests.cs`, `broker.Tests/DeliveryProcessorTests.cs`

**Interfaces:**
- Consumes: `IDeliveryStore`, `IDeadLetterStore`, `PendingDelivery` (Task 4); `BrokerOptions`, `IBrokerLog` (Task 2); `Broker.Contracts.DeliveredMessage`.
- Produces (namespace `Broker.Delivery`):
  - `SubscriberSession(string subscriberId, string peer)` cu `SubscriberId`, `Peer`, `ChannelReader<DeliveredMessage> Outbox`, `CancellationToken Closed`, `bool IsReplaced`, `bool TryEnqueue(DeliveredMessage)`, `void MarkReplaced()`, `void Close()`
  - `SubscriberRegistry` cu `SubscriberSession Open(string subscriberId, string peer)`, `void Close(SubscriberSession)`, `bool TryGet(string, out SubscriberSession)`, `bool IsOnline(string)`, `IReadOnlyList<string> OnlineSubscriberIds`
  - `DeliveryJobQueue` cu `bool Enqueue(string subscriberId)`, `ValueTask<string> DequeueAsync(CancellationToken)`, `int Count`
  - `DeliveryProcessor(IDeliveryStore, IDeadLetterStore, SubscriberRegistry, BrokerOptions, TimeProvider, IBrokerLog)` cu `Task ProcessAsync(string subscriberId, CancellationToken)`, `const string AckNotReceivedReason = "ACK_NOT_RECEIVED_AFTER_MAX_RETRIES"`

- [ ] **Step 1: Scrie testele care pică**

`broker.Tests/SubscriberRegistryTests.cs`:
```csharp
using Broker.Contracts;
using Broker.Delivery;

namespace Broker.Tests;

public sealed class SubscriberRegistryTests
{
    [Fact]
    public void Opening_same_id_replaces_and_closes_previous_session()
    {
        var registry = new SubscriberRegistry();
        SubscriberSession first = registry.Open("sub-a", "ipv4:10.0.0.1:5000");
        SubscriberSession second = registry.Open("sub-a", "ipv4:10.0.0.2:5000");

        Assert.True(first.IsReplaced);
        Assert.True(first.Closed.IsCancellationRequested);
        Assert.False(first.TryEnqueue(new DeliveredMessage()));
        Assert.True(registry.TryGet("sub-a", out SubscriberSession? current));
        Assert.Same(second, current);

        registry.Close(first);
        Assert.True(registry.IsOnline("sub-a"));

        registry.Close(second);
        Assert.False(registry.IsOnline("sub-a"));
        Assert.Empty(registry.OnlineSubscriberIds);
    }

    [Fact]
    public void Open_session_accepts_messages_until_closed()
    {
        var registry = new SubscriberRegistry();
        SubscriberSession session = registry.Open("sub-a", "test");

        Assert.True(session.TryEnqueue(new DeliveredMessage { MessageId = "m1" }));
        Assert.True(session.Outbox.TryRead(out DeliveredMessage? message));
        Assert.Equal("m1", message.MessageId);

        registry.Close(session);

        Assert.False(session.IsReplaced);
        Assert.False(session.TryEnqueue(new DeliveredMessage { MessageId = "m2" }));
    }
}
```

`broker.Tests/DeliveryJobQueueTests.cs`:
```csharp
using Broker.Delivery;

namespace Broker.Tests;

public sealed class DeliveryJobQueueTests
{
    [Fact]
    public async Task Same_subscriber_is_queued_once_until_dequeued()
    {
        var queue = new DeliveryJobQueue();

        Assert.True(queue.Enqueue("a"));
        Assert.False(queue.Enqueue("a"));
        Assert.True(queue.Enqueue("b"));
        Assert.Equal(2, queue.Count);

        Assert.Equal("a", await queue.DequeueAsync(CancellationToken.None));
        Assert.True(queue.Enqueue("a"));
        Assert.Equal("b", await queue.DequeueAsync(CancellationToken.None));
        Assert.Equal("a", await queue.DequeueAsync(CancellationToken.None));
        Assert.Equal(0, queue.Count);
    }
}
```

`broker.Tests/DeliveryProcessorTests.cs`:
```csharp
using Broker.Contracts;
using Broker.Delivery;
using Broker.Storage;

namespace Broker.Tests;

public sealed class DeliveryProcessorTests
{
    private static readonly DateTimeOffset T0 = new(2026, 10, 12, 10, 0, 0, TimeSpan.Zero);
    private readonly ManualTimeProvider _time = new(T0);
    private readonly InMemoryStore _store = new();
    private readonly SubscriberRegistry _registry = new();
    private readonly DeliveryProcessor _processor;

    public DeliveryProcessorTests()
    {
        var options = new BrokerOptions { MaxRetries = 3, AckTimeout = TimeSpan.FromSeconds(5) };
        _processor = new DeliveryProcessor(_store, _store, _registry, options, _time, new RecordingLog());
    }

    [Fact]
    public async Task Delivers_due_message_as_first_attempt()
    {
        SubscriberSession session = await OnlineSubscriberAsync("sub-a", "sport");
        await _store.AddMessageAsync(new StoredMessage("m1", "sport", "goal"), T0);

        await _processor.ProcessAsync("sub-a", CancellationToken.None);

        DeliveredMessage message = Assert.Single(Drain(session));
        Assert.Equal(("m1", "sport", "goal", 1, 3), (message.MessageId, message.Topic, message.Content, message.Attempt, message.MaxAttempts));
    }

    [Fact]
    public async Task Redelivers_only_after_ack_timeout()
    {
        SubscriberSession session = await OnlineSubscriberAsync("sub-a", "sport");
        await _store.AddMessageAsync(new StoredMessage("m1", "sport", "goal"), T0);
        await _processor.ProcessAsync("sub-a", CancellationToken.None);
        Drain(session);

        await _processor.ProcessAsync("sub-a", CancellationToken.None);
        _time.Advance(TimeSpan.FromSeconds(4));
        await _processor.ProcessAsync("sub-a", CancellationToken.None);
        Assert.Empty(Drain(session));

        _time.Advance(TimeSpan.FromSeconds(1));
        await _processor.ProcessAsync("sub-a", CancellationToken.None);
        Assert.Equal(2, Assert.Single(Drain(session)).Attempt);
    }

    [Fact]
    public async Task Moves_to_dead_letter_after_max_attempts_without_ack()
    {
        SubscriberSession session = await OnlineSubscriberAsync("sub-a", "sport");
        await _store.AddMessageAsync(new StoredMessage("m1", "sport", "goal"), T0);

        for (int i = 0; i < 3; i++)
        {
            await _processor.ProcessAsync("sub-a", CancellationToken.None);
            _time.Advance(TimeSpan.FromSeconds(5));
        }
        await _processor.ProcessAsync("sub-a", CancellationToken.None);

        Assert.Equal(new[] { 1, 2, 3 }, Drain(session).Select(m => m.Attempt));
        BrokerSnapshot snapshot = await _store.ReadSnapshotAsync(100);
        Assert.Equal(Statuses.Dead, Assert.Single(snapshot.Messages).Status);
        Assert.Equal(DeliveryProcessor.AckNotReceivedReason, Assert.Single(snapshot.DeadLetters).Reason);
    }

    [Fact]
    public async Task Offline_subscriber_does_not_consume_attempts()
    {
        await _store.AddSubscriptionAsync("sub-a", "sport", T0);
        await _store.AddMessageAsync(new StoredMessage("m1", "sport", "goal"), T0);

        await _processor.ProcessAsync("sub-a", CancellationToken.None);

        Assert.Equal(0, Assert.Single(await _store.GetDueDeliveriesAsync("sub-a", T0)).RetryCount);
    }

    [Fact]
    public async Task Closed_session_does_not_consume_attempts()
    {
        SubscriberSession session = await OnlineSubscriberAsync("sub-a", "sport");
        await _store.AddMessageAsync(new StoredMessage("m1", "sport", "goal"), T0);
        session.Close();

        await _processor.ProcessAsync("sub-a", CancellationToken.None);

        Assert.Equal(0, Assert.Single(await _store.GetDueDeliveriesAsync("sub-a", T0)).RetryCount);
    }

    [Fact]
    public async Task Acknowledged_message_is_not_redelivered()
    {
        SubscriberSession session = await OnlineSubscriberAsync("sub-a", "sport");
        await _store.AddMessageAsync(new StoredMessage("m1", "sport", "goal"), T0);
        await _processor.ProcessAsync("sub-a", CancellationToken.None);
        await _store.AcknowledgeAsync("m1", "sub-a");

        _time.Advance(TimeSpan.FromSeconds(30));
        await _processor.ProcessAsync("sub-a", CancellationToken.None);

        Assert.Single(Drain(session));
    }

    private async Task<SubscriberSession> OnlineSubscriberAsync(string subscriberId, string topic)
    {
        await _store.AddSubscriptionAsync(subscriberId, topic, T0);
        return _registry.Open(subscriberId, "test");
    }

    private static List<DeliveredMessage> Drain(SubscriberSession session)
    {
        var messages = new List<DeliveredMessage>();
        while (session.Outbox.TryRead(out DeliveredMessage? message))
            messages.Add(message);
        return messages;
    }
}
```

- [ ] **Step 2: Rulează ca să confirmi că pică**

Run: `dotnet test broker.Tests --nologo`
Expected: eroare de compilare, namespace-ul `Broker.Delivery` nu există.

- [ ] **Step 3: Creează `broker/Delivery/SubscriberSession.cs`**

```csharp
using System.Threading.Channels;
using Broker.Contracts;

namespace Broker.Delivery;

public sealed class SubscriberSession(string subscriberId, string peer)
{
    private readonly Channel<DeliveredMessage> _outbox =
        Channel.CreateUnbounded<DeliveredMessage>(new UnboundedChannelOptions { SingleReader = true });
    private readonly CancellationTokenSource _closed = new();
    private int _replaced;

    public string SubscriberId { get; } = subscriberId;
    public string Peer { get; } = peer;
    public ChannelReader<DeliveredMessage> Outbox => _outbox.Reader;
    public CancellationToken Closed => _closed.Token;
    public bool IsReplaced => Volatile.Read(ref _replaced) == 1;

    public bool TryEnqueue(DeliveredMessage message) =>
        !_closed.IsCancellationRequested && _outbox.Writer.TryWrite(message);

    public void MarkReplaced()
    {
        Volatile.Write(ref _replaced, 1);
        Close();
    }

    public void Close()
    {
        _outbox.Writer.TryComplete();
        _closed.Cancel();
    }
}
```

- [ ] **Step 4: Creează `broker/Delivery/SubscriberRegistry.cs`**

```csharp
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace Broker.Delivery;

public sealed class SubscriberRegistry
{
    private readonly ConcurrentDictionary<string, SubscriberSession> _sessions = new();

    public IReadOnlyList<string> OnlineSubscriberIds => _sessions.Keys.ToList();

    public SubscriberSession Open(string subscriberId, string peer)
    {
        var session = new SubscriberSession(subscriberId, peer);
        while (true)
        {
            if (_sessions.TryGetValue(subscriberId, out SubscriberSession? existing))
            {
                if (_sessions.TryUpdate(subscriberId, session, existing))
                {
                    existing.MarkReplaced();
                    return session;
                }
            }
            else if (_sessions.TryAdd(subscriberId, session))
            {
                return session;
            }
        }
    }

    public void Close(SubscriberSession session)
    {
        _sessions.TryRemove(new KeyValuePair<string, SubscriberSession>(session.SubscriberId, session));
        session.Close();
    }

    public bool TryGet(string subscriberId, [MaybeNullWhen(false)] out SubscriberSession session) =>
        _sessions.TryGetValue(subscriberId, out session);

    public bool IsOnline(string subscriberId) => _sessions.ContainsKey(subscriberId);
}
```

- [ ] **Step 5: Creează `broker/Delivery/DeliveryJobQueue.cs`**

```csharp
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Broker.Delivery;

public sealed class DeliveryJobQueue
{
    private readonly Channel<string> _jobs = Channel.CreateUnbounded<string>();
    private readonly ConcurrentDictionary<string, byte> _queued = new();

    public int Count => _queued.Count;

    public bool Enqueue(string subscriberId)
    {
        if (!_queued.TryAdd(subscriberId, 0))
            return false;
        _jobs.Writer.TryWrite(subscriberId);
        return true;
    }

    public async ValueTask<string> DequeueAsync(CancellationToken cancellationToken)
    {
        string subscriberId = await _jobs.Reader.ReadAsync(cancellationToken);
        _queued.TryRemove(subscriberId, out _);
        return subscriberId;
    }
}
```

- [ ] **Step 6: Creează `broker/Delivery/DeliveryProcessor.cs`**

```csharp
using System.Collections.Concurrent;
using Broker.Contracts;
using Broker.Logging;
using Broker.Storage;

namespace Broker.Delivery;

public sealed class DeliveryProcessor(
    IDeliveryStore deliveries,
    IDeadLetterStore deadLetters,
    SubscriberRegistry registry,
    BrokerOptions options,
    TimeProvider time,
    IBrokerLog log)
{
    public const string AckNotReceivedReason = "ACK_NOT_RECEIVED_AFTER_MAX_RETRIES";

    private readonly ConcurrentDictionary<string, SemaphoreSlim> _subscriberLocks = new();

    public async Task ProcessAsync(string subscriberId, CancellationToken cancellationToken)
    {
        SemaphoreSlim subscriberLock = _subscriberLocks.GetOrAdd(subscriberId, _ => new SemaphoreSlim(1, 1));
        await subscriberLock.WaitAsync(cancellationToken);
        try
        {
            if (!registry.TryGet(subscriberId, out SubscriberSession? session))
                return;

            DateTimeOffset now = time.GetUtcNow();
            IReadOnlyList<PendingDelivery> due =
                await deliveries.GetDueDeliveriesAsync(subscriberId, now - options.AckTimeout, cancellationToken);

            foreach (PendingDelivery delivery in due)
            {
                if (delivery.RetryCount >= options.MaxRetries)
                {
                    if (await deadLetters.MoveToDeadLetterAsync(delivery, AckNotReceivedReason, now, cancellationToken))
                        log.Info($"[DLQ] {delivery.MessageId} for {subscriberId} after {delivery.RetryCount} attempts without ACK");
                    continue;
                }

                var message = new DeliveredMessage
                {
                    MessageId = delivery.MessageId,
                    Topic = delivery.Topic,
                    Content = delivery.Content,
                    Attempt = delivery.RetryCount + 1,
                    MaxAttempts = options.MaxRetries
                };
                if (!session.TryEnqueue(message))
                    return;

                await deliveries.RecordAttemptAsync(delivery.MessageId, subscriberId, now, cancellationToken);
                log.Info($"[DELIVERY] {delivery.MessageId} -> {subscriberId} (attempt {message.Attempt}/{options.MaxRetries})");
            }
        }
        finally
        {
            subscriberLock.Release();
        }
    }
}
```

- [ ] **Step 7: Rulează testele**

Run: `dotnet test broker.Tests --nologo`
Expected: PASS (toate, inclusiv cele 9 teste noi).

- [ ] **Step 8: Checkpoint** — de comis: `broker/Delivery/*.cs` (4 fișiere), `broker.Tests/SubscriberRegistryTests.cs`, `broker.Tests/DeliveryJobQueueTests.cs`, `broker.Tests/DeliveryProcessorTests.cs`.

---

### Task 7: Rutare, API gRPC, work joburi și găzduirea brokerului

**Files:**
- Create: `broker/Routing/TopicRouter.cs`
- Create: `broker/Delivery/DeliveryWorkerPool.cs`
- Create: `broker/Delivery/RetryScheduler.cs`
- Create: `broker/Api/MessageBrokerService.cs`
- Create: `broker/Api/ExceptionInterceptor.cs`
- Create: `broker/Api/PayloadFormatter.cs`
- Create: `broker/Api/StateMapper.cs`
- Create: `broker/Hosting/NetworkAddresses.cs`
- Modify: `broker/Program.cs` (înlocuire completă)
- Test: `broker.Tests/TopicRouterTests.cs`

**Interfaces:**
- Consumes: tot ce produc Task 2–6.
- Produces: `TopicRouter(IMessageStore, ISubscriptionStore, SubscriberRegistry, DeliveryJobQueue, TimeProvider, IBrokerLog)` cu `Task<PublishResult> PublishAsync(StoredMessage, CancellationToken)`, `Task SubscribeAsync(string, string, CancellationToken)`, `Task<bool> UnsubscribeAsync(string, string, CancellationToken)`; executabilul `broker/bin/Release/net9.0/Broker.dll` cu argumentele `--port --storage --db --workers`.

- [ ] **Step 1: Scrie testele care pică — `broker.Tests/TopicRouterTests.cs`**

```csharp
using Broker.Delivery;
using Broker.Routing;
using Broker.Storage;

namespace Broker.Tests;

public sealed class TopicRouterTests
{
    private static readonly DateTimeOffset T0 = new(2026, 10, 12, 10, 0, 0, TimeSpan.Zero);
    private readonly InMemoryStore _store = new();
    private readonly SubscriberRegistry _registry = new();
    private readonly DeliveryJobQueue _queue = new();
    private readonly TopicRouter _router;

    public TopicRouterTests()
    {
        _router = new TopicRouter(_store, _store, _registry, _queue, new ManualTimeProvider(T0), new RecordingLog());
    }

    [Fact]
    public async Task Publish_schedules_only_online_recipients()
    {
        await _store.AddSubscriptionAsync("online", "sport", T0);
        await _store.AddSubscriptionAsync("offline", "sport", T0);
        _registry.Open("online", "test");

        PublishResult result = await _router.PublishAsync(new StoredMessage("m1", "sport", "goal"), CancellationToken.None);

        Assert.Equal(2, result.Recipients.Count);
        Assert.Equal(1, _queue.Count);
        Assert.Equal("online", await _queue.DequeueAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Duplicate_publish_schedules_nothing()
    {
        await _store.AddSubscriptionAsync("online", "sport", T0);
        _registry.Open("online", "test");
        await _router.PublishAsync(new StoredMessage("m1", "sport", "goal"), CancellationToken.None);
        await _queue.DequeueAsync(CancellationToken.None);

        PublishResult duplicate = await _router.PublishAsync(new StoredMessage("m1", "sport", "goal"), CancellationToken.None);

        Assert.False(duplicate.Inserted);
        Assert.Equal(0, _queue.Count);
    }

    [Fact]
    public async Task Subscribe_schedules_online_subscriber_and_not_offline_one()
    {
        await _store.AddMessageAsync(new StoredMessage("m1", "sport", "goal"), T0);
        _registry.Open("online", "test");

        await _router.SubscribeAsync("offline", "sport", CancellationToken.None);
        Assert.Equal(0, _queue.Count);

        await _router.SubscribeAsync("online", "sport", CancellationToken.None);
        Assert.Equal("online", await _queue.DequeueAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Unsubscribe_reports_whether_a_subscription_existed()
    {
        await _router.SubscribeAsync("sub-a", "sport", CancellationToken.None);

        Assert.True(await _router.UnsubscribeAsync("sub-a", "sport", CancellationToken.None));
        Assert.False(await _router.UnsubscribeAsync("sub-a", "sport", CancellationToken.None));
    }
}
```

- [ ] **Step 2: Rulează ca să confirmi că pică**

Run: `dotnet test broker.Tests --nologo`
Expected: eroare de compilare, `Broker.Routing` nu există.

- [ ] **Step 3: Creează `broker/Routing/TopicRouter.cs`**

```csharp
using Broker.Delivery;
using Broker.Logging;
using Broker.Storage;

namespace Broker.Routing;

public sealed class TopicRouter(
    IMessageStore messages,
    ISubscriptionStore subscriptions,
    SubscriberRegistry registry,
    DeliveryJobQueue queue,
    TimeProvider time,
    IBrokerLog log)
{
    public async Task<PublishResult> PublishAsync(StoredMessage message, CancellationToken cancellationToken)
    {
        PublishResult result = await messages.AddMessageAsync(message, time.GetUtcNow(), cancellationToken);
        if (!result.Inserted)
            return result;

        log.Info($"[STORED] {message.MessageId} topic={message.Topic} recipients={result.Recipients.Count}");
        if (result.Recipients.Count == 0)
            log.Info($"[PENDING] {message.MessageId} has no subscribers yet; kept for future subscribers");
        ScheduleOnline(result.Recipients);
        return result;
    }

    public async Task SubscribeAsync(string subscriberId, string topic, CancellationToken cancellationToken)
    {
        await subscriptions.AddSubscriptionAsync(subscriberId, topic, time.GetUtcNow(), cancellationToken);
        log.Info($"[SUBSCRIBE] {subscriberId} -> {topic}");
        ScheduleOnline([subscriberId]);
    }

    public async Task<bool> UnsubscribeAsync(string subscriberId, string topic, CancellationToken cancellationToken)
    {
        bool removed = await subscriptions.RemoveSubscriptionAsync(subscriberId, topic, cancellationToken);
        log.Info(removed
            ? $"[UNSUBSCRIBE] {subscriberId} -> {topic}"
            : $"[UNSUBSCRIBE] {subscriberId} -> {topic} (was not subscribed)");
        return removed;
    }

    private void ScheduleOnline(IEnumerable<string> subscriberIds)
    {
        foreach (string subscriberId in subscriberIds)
        {
            if (registry.IsOnline(subscriberId))
                queue.Enqueue(subscriberId);
        }
    }
}
```

- [ ] **Step 4: Rulează testele**

Run: `dotnet test broker.Tests --nologo`
Expected: PASS.

- [ ] **Step 5: Creează `broker/Delivery/DeliveryWorkerPool.cs`**

```csharp
using Broker.Logging;
using Microsoft.Extensions.Hosting;

namespace Broker.Delivery;

public sealed class DeliveryWorkerPool(
    DeliveryJobQueue queue,
    DeliveryProcessor processor,
    BrokerOptions options,
    IBrokerLog log) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.WhenAll(Enumerable.Range(1, options.Workers)
            .Select(workerId => Task.Run(() => RunWorkerAsync(workerId, stoppingToken), stoppingToken)));

    private async Task RunWorkerAsync(int workerId, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            string subscriberId;
            try
            {
                subscriberId = await queue.DequeueAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                await processor.ProcessAsync(subscriberId, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                log.Error($"[WORKER {workerId}] delivery to {subscriberId} failed: {ex.Message}");
            }
        }
    }
}
```

- [ ] **Step 6: Creează `broker/Delivery/RetryScheduler.cs`**

```csharp
using Microsoft.Extensions.Hosting;

namespace Broker.Delivery;

public sealed class RetryScheduler(
    SubscriberRegistry registry,
    DeliveryJobQueue queue,
    BrokerOptions options,
    TimeProvider time) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.RetryInterval, time);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                foreach (string subscriberId in registry.OnlineSubscriberIds)
                    queue.Enqueue(subscriberId);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
```

- [ ] **Step 7: Creează `broker/Api/PayloadFormatter.cs`**

```csharp
using Google.Protobuf;

namespace Broker.Api;

public static class PayloadFormatter
{
    private static readonly JsonFormatter Formatter = new(JsonFormatter.Settings.Default.WithFormatDefaultValues(true));

    public static string Format(IMessage message) => Formatter.Format(message);
}
```

- [ ] **Step 8: Creează `broker/Api/StateMapper.cs`**

```csharp
using Broker.Contracts;
using Broker.Storage;

namespace Broker.Api;

public static class StateMapper
{
    public static BrokerState ToProto(BrokerSnapshot snapshot, string storageMode, IEnumerable<string> onlineSubscribers)
    {
        var state = new BrokerState { StorageMode = storageMode };
        state.Messages.AddRange(snapshot.Messages.Select(m => new MessageRecord
        {
            MessageId = m.MessageId,
            Topic = m.Topic,
            Content = m.Content,
            Status = m.Status,
            CreatedAtUtc = m.CreatedAtUtc
        }));
        state.Deliveries.AddRange(snapshot.Deliveries.Select(d => new DeliveryRecord
        {
            MessageId = d.MessageId,
            SubscriberId = d.SubscriberId,
            Status = d.Status,
            RetryCount = d.RetryCount,
            LastError = d.LastError ?? ""
        }));
        state.DeadLetters.AddRange(snapshot.DeadLetters.Select(d => new DeadLetterRecord
        {
            Id = d.Id,
            MessageId = d.MessageId ?? "",
            SubscriberId = d.SubscriberId ?? "",
            Topic = d.Topic ?? "",
            RawPayload = d.RawPayload ?? "",
            Reason = d.Reason,
            RetryCount = d.RetryCount,
            CreatedAtUtc = d.CreatedAtUtc
        }));
        state.Subscriptions.AddRange(snapshot.Subscriptions.Select(s => new SubscriptionRecord
        {
            SubscriberId = s.SubscriberId,
            Topic = s.Topic
        }));
        state.OnlineSubscribers.AddRange(onlineSubscribers.Order());
        return state;
    }
}
```

- [ ] **Step 9: Creează `broker/Api/ExceptionInterceptor.cs`**

```csharp
using Broker.Logging;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Broker.Api;

public sealed class ExceptionInterceptor(IBrokerLog log) : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context);
        }
        catch (Exception ex) when (IsUnexpected(ex, context))
        {
            throw Fail(ex, context);
        }
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            await continuation(request, responseStream, context);
        }
        catch (Exception ex) when (IsUnexpected(ex, context))
        {
            throw Fail(ex, context);
        }
    }

    private static bool IsUnexpected(Exception ex, ServerCallContext context) =>
        ex is not RpcException
        && !(ex is OperationCanceledException && context.CancellationToken.IsCancellationRequested);

    private RpcException Fail(Exception ex, ServerCallContext context)
    {
        log.Error($"{context.Method} from {context.Peer}: {ex}");
        return new RpcException(new Status(StatusCode.Internal, "INTERNAL_ERROR"));
    }
}
```

- [ ] **Step 10: Creează `broker/Api/MessageBrokerService.cs`**

```csharp
using Broker.Contracts;
using Broker.Delivery;
using Broker.Logging;
using Broker.Routing;
using Broker.Storage;
using Broker.Validation;
using Grpc.Core;

namespace Broker.Api;

public sealed class MessageBrokerService(
    RequestValidator validator,
    TopicRouter router,
    IDeliveryStore deliveries,
    IDeadLetterStore deadLetters,
    IStateReader state,
    SubscriberRegistry registry,
    DeliveryJobQueue queue,
    BrokerOptions options,
    TimeProvider time,
    IBrokerLog log) : MessageBroker.MessageBrokerBase
{
    private const int DefaultStateLimit = 100;
    private const int MaxStateLimit = 1000;

    public override async Task<PublishReply> Publish(PublishRequest request, ServerCallContext context)
    {
        IReadOnlyList<string> errors = validator.ValidatePublish(request);
        if (errors.Count > 0)
        {
            string reason = string.Join(",", errors);
            await deadLetters.AddInvalidPayloadAsync(PayloadFormatter.Format(request), reason, time.GetUtcNow(), CancellationToken.None);
            log.Info($"[DLQ] invalid publish from {context.Peer}: {reason}");
            throw new RpcException(new Status(StatusCode.InvalidArgument, reason));
        }

        log.Info($"[RECEIVED from {context.Peer}] publish {request.MessageId} topic={request.Topic}");
        PublishResult result = await router.PublishAsync(
            new StoredMessage(request.MessageId, request.Topic, request.Content), context.CancellationToken);
        if (!result.Inserted)
        {
            log.Info($"[DUPLICATE] message {request.MessageId} already exists");
            throw new RpcException(new Status(StatusCode.AlreadyExists, "DUPLICATE_MESSAGE_ID"));
        }

        return new PublishReply
        {
            MessageId = request.MessageId,
            Topic = request.Topic,
            Subscribers = result.Recipients.Count
        };
    }

    public override async Task<SubscriptionReply> Subscribe(SubscriptionRequest request, ServerCallContext context)
    {
        EnsureValid(validator.ValidateSubscription(request));
        await router.SubscribeAsync(request.SubscriberId, request.Topic, context.CancellationToken);
        return new SubscriptionReply { SubscriberId = request.SubscriberId, Topic = request.Topic };
    }

    public override async Task<SubscriptionReply> Unsubscribe(SubscriptionRequest request, ServerCallContext context)
    {
        EnsureValid(validator.ValidateSubscription(request));
        await router.UnsubscribeAsync(request.SubscriberId, request.Topic, context.CancellationToken);
        return new SubscriptionReply { SubscriberId = request.SubscriberId, Topic = request.Topic };
    }

    public override async Task Listen(ListenRequest request, IServerStreamWriter<DeliveredMessage> responseStream, ServerCallContext context)
    {
        EnsureValid(validator.ValidateListen(request));
        SubscriberSession session = registry.Open(request.SubscriberId, context.Peer);
        log.Info($"[ONLINE] {request.SubscriberId} from {context.Peer}");
        queue.Enqueue(request.SubscriberId);

        try
        {
            await foreach (DeliveredMessage message in session.Outbox.ReadAllAsync(context.CancellationToken))
                await responseStream.WriteAsync(message);
        }
        catch (Exception) when (context.CancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            registry.Close(session);
            log.Info(session.IsReplaced
                ? $"[REPLACED] {request.SubscriberId} ({context.Peer}) by a newer connection"
                : $"[OFFLINE] {request.SubscriberId} ({context.Peer})");
        }

        if (session.IsReplaced)
            throw new RpcException(new Status(StatusCode.Aborted, "SESSION_REPLACED"));
    }

    public override async Task<AckReply> Ack(AckRequest request, ServerCallContext context)
    {
        EnsureValid(validator.ValidateAck(request));
        AckOutcome outcome = await deliveries.AcknowledgeAsync(request.MessageId, request.SubscriberId, context.CancellationToken);
        switch (outcome)
        {
            case AckOutcome.Acked:
                log.Info($"[ACK] {request.MessageId} confirmed by {request.SubscriberId}");
                return new AckReply { MessageId = request.MessageId };
            case AckOutcome.Duplicate:
                log.Info($"[DUPLICATE ACK] {request.MessageId} from {request.SubscriberId}");
                return new AckReply { MessageId = request.MessageId, Duplicate = true };
            default:
                throw new RpcException(new Status(StatusCode.NotFound, "DELIVERY_NOT_FOUND"));
        }
    }

    public override Task<PingReply> Ping(PingRequest request, ServerCallContext context) =>
        Task.FromResult(new PingReply
        {
            ServerTimeUtc = Timestamps.Format(time.GetUtcNow()),
            StorageMode = options.StorageName
        });

    public override async Task<BrokerState> GetState(StateRequest request, ServerCallContext context)
    {
        int limit = request.Limit <= 0 ? DefaultStateLimit : Math.Min(request.Limit, MaxStateLimit);
        BrokerSnapshot snapshot = await state.ReadSnapshotAsync(limit, context.CancellationToken);
        return StateMapper.ToProto(snapshot, options.StorageName, registry.OnlineSubscriberIds);
    }

    private static void EnsureValid(IReadOnlyList<string> errors)
    {
        if (errors.Count > 0)
            throw new RpcException(new Status(StatusCode.InvalidArgument, string.Join(",", errors)));
    }
}
```

- [ ] **Step 11: Creează `broker/Hosting/NetworkAddresses.cs`**

```csharp
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace Broker.Hosting;

public static class NetworkAddresses
{
    public static IReadOnlyList<string> LocalIPv4() =>
        NetworkInterface.GetAllNetworkInterfaces()
            .Where(n => n.OperationalStatus == OperationalStatus.Up && n.NetworkInterfaceType != NetworkInterfaceType.Loopback)
            .SelectMany(n => n.GetIPProperties().UnicastAddresses)
            .Where(a => a.Address.AddressFamily == AddressFamily.InterNetwork)
            .Select(a => a.Address.ToString())
            .Distinct()
            .ToList();
}
```

- [ ] **Step 12: Înlocuiește `broker/Program.cs`**

```csharp
using System.Text;
using Broker;
using Broker.Api;
using Broker.Delivery;
using Broker.Hosting;
using Broker.Logging;
using Broker.Routing;
using Broker.Storage;
using Broker.Validation;
using Microsoft.AspNetCore.Server.Kestrel.Core;

BrokerOptions options;
try
{
    options = BrokerOptions.Parse(args);
}
catch (ArgumentException ex)
{
    Console.Error.WriteLine(ex.Message);
    Console.Error.WriteLine(BrokerOptions.Usage);
    return 1;
}

Console.OutputEncoding = Encoding.UTF8;

IBrokerStore store = options.Storage == StorageMode.Persistent
    ? new SqliteStore(options.DatabasePath)
    : new InMemoryStore();
await store.InitializeAsync();

WebApplicationBuilder builder = WebApplication.CreateBuilder();
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole();
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.ListenAnyIP(options.Port, listen => listen.Protocols = HttpProtocols.Http2);
    kestrel.Limits.Http2.KeepAlivePingDelay = TimeSpan.FromSeconds(30);
    kestrel.Limits.Http2.KeepAlivePingTimeout = TimeSpan.FromSeconds(10);
});

builder.Services.AddSingleton(options);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<IBrokerLog, ConsoleBrokerLog>();
builder.Services.AddSingleton(store);
builder.Services.AddSingleton<IMessageStore>(store);
builder.Services.AddSingleton<ISubscriptionStore>(store);
builder.Services.AddSingleton<IDeliveryStore>(store);
builder.Services.AddSingleton<IDeadLetterStore>(store);
builder.Services.AddSingleton<IStateReader>(store);
builder.Services.AddSingleton<RequestValidator>();
builder.Services.AddSingleton<SubscriberRegistry>();
builder.Services.AddSingleton<DeliveryJobQueue>();
builder.Services.AddSingleton<DeliveryProcessor>();
builder.Services.AddSingleton<TopicRouter>();
builder.Services.AddHostedService<DeliveryWorkerPool>();
builder.Services.AddHostedService<RetryScheduler>();
builder.Services.AddGrpc(grpc =>
{
    grpc.Interceptors.Add<ExceptionInterceptor>();
    grpc.MaxReceiveMessageSize = 64 * 1024;
});
builder.Services.AddGrpcReflection();

WebApplication app = builder.Build();
app.MapGrpcService<MessageBrokerService>();
app.MapGrpcReflectionService();

Console.WriteLine(" ------ MESSAGE BROKER - gRPC / Protobuf ------");
Console.WriteLine(options.Storage == StorageMode.Persistent
    ? $"Storage: persistent (SQLite: {Path.GetFullPath(options.DatabasePath)})"
    : "Storage: transient (in-memory, lost on restart)");
Console.WriteLine($"Listening on 0.0.0.0:{options.Port} (HTTP/2 over TCP), {options.Workers} delivery workers");
foreach (string address in NetworkAddresses.LocalIPv4())
    Console.WriteLine($"  clients on other computers can connect to {address}:{options.Port}");
Console.WriteLine();

await app.RunAsync();
return 0;
```

- [ ] **Step 13: Build Release și toate testele**

Run: `dotnet build broker/Broker.csproj -c Release --nologo; dotnet test broker.Tests --nologo`
Expected: `Build succeeded` fără erori; toate testele PASS.

- [ ] **Step 14: Smoke test — pornire și Ping prin gRPC**

Run:
```powershell
$broker = Start-Process dotnet -ArgumentList 'broker/bin/Release/net9.0/Broker.dll --port 50071 --storage transient' -PassThru -NoNewWindow; Start-Sleep -Seconds 3; .\.venv\Scripts\python -c "import sys; sys.path.insert(0, 'publisher'); import stubs, grpc; pb, g = stubs.load(); print(g.MessageBrokerStub(grpc.insecure_channel('127.0.0.1:50071')).Ping(pb.PingRequest(), timeout=10))"; Stop-Process -Id $broker.Id
```
Expected: bannerul brokerului cu `Storage: transient`, cel puțin o linie `clients on other computers can connect to <IP>:50071`, apoi `server_time_utc: "..."` și `storage_mode: "transient"`.

- [ ] **Step 15: Smoke test — argument invalid**

Run: `dotnet broker/bin/Release/net9.0/Broker.dll --storage memory; $LASTEXITCODE`
Expected: `--storage must be 'persistent' or 'transient'`, linia `Usage: ...` și exit code `1`.

- [ ] **Step 16: Checkpoint** — de comis: `broker/Routing/TopicRouter.cs`, `broker/Delivery/DeliveryWorkerPool.cs`, `broker/Delivery/RetryScheduler.cs`, `broker/Api/*.cs`, `broker/Hosting/NetworkAddresses.cs`, `broker/Program.cs`, `broker.Tests/TopicRouterTests.cs`.

---

### Task 8: Publisher Python

**Files:**
- Create: `publisher/publisher.py`

**Interfaces:**
- Consumes: `stubs.load()` (Task 1); brokerul rulând (Task 7).
- Produces: CLI `python publisher/publisher.py [--host H] [--port P]` cu meniul 1–5, 0. Linii de output pe care se bazează testul de interoperabilitate (Task 12): `Accepted: <id> on topic '<topic>'`, `Subscribers found: <n>`, `There are currently no subscribers for this topic.`, `Broker error: <CODE> - <details>`.

- [ ] **Step 1: Creează `publisher/publisher.py`**

```python
import argparse
import uuid

import grpc

import stubs

broker_pb2, broker_pb2_grpc = stubs.load()

CALL_TIMEOUT_SECONDS = 5
MAX_NAME_LENGTH = 64
MAX_CONTENT_LENGTH = 4096

MENU = """
------------------------------------------
1 - Publish message
2 - DLQ test: publish without topic
3 - DLQ test: publish without content
4 - Idempotency test: re-send last message
5 - Ping Broker
0 - Exit
------------------------------------------"""


def parse_args():
    parser = argparse.ArgumentParser(description="Publisher client (gRPC)")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host/IP (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=50051, help="Broker port (default: 50051)")
    return parser.parse_args()


def validate_topic(topic):
    if not topic:
        return "topic cannot be empty"
    if len(topic) > MAX_NAME_LENGTH:
        return f"topic is longer than {MAX_NAME_LENGTH} characters"
    if not all(ch.isalnum() or ch in "._-" for ch in topic):
        return "topic may contain only letters, digits, '.', '_' and '-'"
    return None


def validate_content(content):
    if not content.strip():
        return "message cannot be empty"
    if len(content) > MAX_CONTENT_LENGTH:
        return f"message is longer than {MAX_CONTENT_LENGTH} characters"
    return None


class BrokerGateway:
    def __init__(self, host, port):
        self._channel = grpc.insecure_channel(f"{host}:{port}")
        self._stub = broker_pb2_grpc.MessageBrokerStub(self._channel)

    def publish(self, message_id, topic, content):
        request = broker_pb2.PublishRequest(message_id=message_id, topic=topic, content=content)
        return self._stub.Publish(request, timeout=CALL_TIMEOUT_SECONDS)

    def ping(self):
        return self._stub.Ping(broker_pb2.PingRequest(), timeout=CALL_TIMEOUT_SECONDS)

    def close(self):
        self._channel.close()


def print_rpc_error(error):
    if error.code() == grpc.StatusCode.UNAVAILABLE:
        print("Broker unavailable. Check the host/IP, the port, the firewall and that the Broker is running.")
    else:
        print(f"Broker error: {error.code().name} - {error.details()}")


def send(gateway, message_id, topic, content):
    print()
    print(f"Sending: message_id={message_id!r} topic={topic!r} content={content!r}")
    try:
        reply = gateway.publish(message_id, topic, content)
    except grpc.RpcError as error:
        print_rpc_error(error)
        return
    print(f"Accepted: {reply.message_id} on topic '{reply.topic}'")
    if reply.subscribers == 0:
        print("There are currently no subscribers for this topic.")
        print("The message remains stored in the Broker.")
    else:
        print(f"Subscribers found: {reply.subscribers}")


def publish_interactive(gateway):
    topic = input("Topic: ").strip()
    content = input("Message: ").strip()
    error = validate_topic(topic) or validate_content(content)
    if error:
        print(f"Error: {error}.")
        return None
    message = (str(uuid.uuid4()), topic, content)
    send(gateway, *message)
    return message


def ping(gateway):
    try:
        reply = gateway.ping()
    except grpc.RpcError as error:
        print_rpc_error(error)
        return
    print(f"PONG - broker time {reply.server_time_utc}, storage {reply.storage_mode}")


def main():
    args = parse_args()
    print("=" * 50)
    print("        PUBLISHER - PYTHON (gRPC)")
    print("=" * 50)
    print(f"Broker: {args.host}:{args.port}")

    gateway = BrokerGateway(args.host, args.port)
    last_message = None
    try:
        while True:
            print(MENU)
            option = input("Choose an option: ").strip()
            if option == "1":
                last_message = publish_interactive(gateway) or last_message
            elif option == "2":
                send(gateway, str(uuid.uuid4()), "", "message without topic")
            elif option == "3":
                send(gateway, str(uuid.uuid4()), "dlq-test", "")
            elif option == "4":
                if last_message is None:
                    print("Publish a message first (option 1).")
                else:
                    send(gateway, *last_message)
            elif option == "5":
                ping(gateway)
            elif option == "0":
                print("Publisher closed.")
                break
            else:
                print("Invalid option.")
    except (EOFError, KeyboardInterrupt):
        print()
        print("Publisher closed.")
    finally:
        gateway.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verificare manuală cu input scriptat**

Run:
```powershell
$broker = Start-Process dotnet -ArgumentList 'broker/bin/Release/net9.0/Broker.dll --port 50072 --storage transient' -PassThru -NoNewWindow; Start-Sleep -Seconds 3; "1`nsport`nSalut`n4`n2`n3`n5`n0`n" | .\.venv\Scripts\python publisher/publisher.py --port 50072; Stop-Process -Id $broker.Id
```
Expected în output, în ordine:
- `Accepted: <uuid> on topic 'sport'` și `There are currently no subscribers for this topic.`;
- `Broker error: ALREADY_EXISTS - DUPLICATE_MESSAGE_ID` (opțiunea 4);
- `Broker error: INVALID_ARGUMENT - TOPIC_REQUIRED` (opțiunea 2);
- `Broker error: INVALID_ARGUMENT - CONTENT_REQUIRED` (opțiunea 3);
- `PONG - broker time ..., storage transient`;
- `Publisher closed.`

În log-ul brokerului apar `[STORED]`, `[DUPLICATE]` și două linii `[DLQ] invalid publish`.

- [ ] **Step 3: Verificare — broker oprit**

Run: `"5`n0`n" | .\.venv\Scripts\python publisher/publisher.py --port 50099`
Expected: `Broker unavailable. Check the host/IP, ...` fără stack trace, apoi `Publisher closed.`

- [ ] **Step 4: Checkpoint** — de comis: `publisher/publisher.py`.

---

### Task 9: Subscriber Java (Maven Wrapper, grpc-java)

**Files:**
- Create: `subscriber/mvnw`, `subscriber/mvnw.cmd` (din distribuția oficială)
- Create: `subscriber/.mvn/wrapper/maven-wrapper.properties`
- Create: `subscriber/pom.xml`
- Create: `subscriber/src/main/java/md/utm/pad/subscriber/AckSender.java`
- Create: `subscriber/src/main/java/md/utm/pad/subscriber/MessageProcessor.java`
- Create: `subscriber/src/main/java/md/utm/pad/subscriber/BrokerClient.java`
- Create: `subscriber/src/main/java/md/utm/pad/subscriber/SubscriberApp.java`
- Test: `subscriber/src/test/java/md/utm/pad/subscriber/MessageProcessorTest.java`

**Interfaces:**
- Consumes: `proto/broker.proto` (generat în `md.utm.pad.broker.grpc`).
- Produces: `subscriber/target/subscriber.jar`, rulat cu `java -jar target/subscriber.jar [host] [port]`. Pe stdin primește, în ordine: `Subscriber ID`, apoi opțiunile de meniu. Linii de output folosite de Task 12: `[BROKER] Subscription confirmed: <topic>`, `Content    : <content>`, `[BROKER] ACK confirmed for: <id>`, `Auto-ACK is now OFF`, `[ACK disabled]`.

- [ ] **Step 1: Instalează Maven Wrapper din distribuția oficială**

Run (din `laborator2-gRPC/`):
```powershell
$zip = Join-Path $env:TEMP 'mvnw-only-script-3.3.4.zip'; $out = Join-Path $env:TEMP 'mvnw-only-script-3.3.4'; Invoke-WebRequest -UseBasicParsing 'https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper-distribution/3.3.4/maven-wrapper-distribution-3.3.4-only-script.zip' -OutFile $zip; Expand-Archive $zip -DestinationPath $out -Force; Get-ChildItem -Recurse $out | Select-Object FullName
```
Expected: lista conține `mvnw` și `mvnw.cmd`. Citește ambele scripturi înainte de folosire și confirmă că descarcă Maven din `distributionUrl`. Apoi copiază-le:
```powershell
New-Item -ItemType Directory -Force subscriber/.mvn/wrapper | Out-Null; Copy-Item (Get-ChildItem -Recurse $out -Filter mvnw).FullName subscriber/; Copy-Item (Get-ChildItem -Recurse $out -Filter mvnw.cmd).FullName subscriber/
```

- [ ] **Step 2: Creează `subscriber/.mvn/wrapper/maven-wrapper.properties`**

```properties
wrapperVersion=3.3.4
distributionType=only-script
distributionUrl=https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.16/apache-maven-3.9.16-bin.zip
```

- [ ] **Step 3: Creează `subscriber/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>md.utm.pad</groupId>
  <artifactId>subscriber</artifactId>
  <version>1.0.0</version>
  <packaging>jar</packaging>

  <properties>
    <maven.compiler.release>11</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <grpc.version>1.84.2</grpc.version>
    <protobuf.version>3.25.9</protobuf.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-netty-shaded</artifactId>
      <version>${grpc.version}</version>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-protobuf</artifactId>
      <version>${grpc.version}</version>
    </dependency>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-stub</artifactId>
      <version>${grpc.version}</version>
    </dependency>
    <dependency>
      <groupId>com.google.protobuf</groupId>
      <artifactId>protobuf-java</artifactId>
      <version>${protobuf.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.tomcat</groupId>
      <artifactId>annotations-api</artifactId>
      <version>6.0.53</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <version>5.14.4</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <finalName>subscriber</finalName>
    <extensions>
      <extension>
        <groupId>kr.motd.maven</groupId>
        <artifactId>os-maven-plugin</artifactId>
        <version>1.7.1</version>
      </extension>
    </extensions>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.14.1</version>
      </plugin>
      <plugin>
        <groupId>org.xolstice.maven.plugins</groupId>
        <artifactId>protobuf-maven-plugin</artifactId>
        <version>0.6.1</version>
        <configuration>
          <protocArtifact>com.google.protobuf:protoc:${protobuf.version}:exe:${os.detected.classifier}</protocArtifact>
          <pluginId>grpc-java</pluginId>
          <pluginArtifact>io.grpc:protoc-gen-grpc-java:${grpc.version}:exe:${os.detected.classifier}</pluginArtifact>
          <protoSourceRoot>${project.basedir}/../proto</protoSourceRoot>
        </configuration>
        <executions>
          <execution>
            <goals>
              <goal>compile</goal>
              <goal>compile-custom</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>3.5.4</version>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.6.2</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals>
              <goal>shade</goal>
            </goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>md.utm.pad.subscriber.SubscriberApp</mainClass>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 4: Scrie testul care pică — `subscriber/src/test/java/md/utm/pad/subscriber/MessageProcessorTest.java`**

```java
package md.utm.pad.subscriber;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import md.utm.pad.broker.grpc.DeliveredMessage;
import org.junit.jupiter.api.Test;

class MessageProcessorTest {

    private final List<String> acks = new ArrayList<>();
    private final List<String> output = new ArrayList<>();
    private final MessageProcessor processor = new MessageProcessor(acks::add, output::add);

    private static DeliveredMessage message(String id, int attempt) {
        return DeliveredMessage.newBuilder()
                .setMessageId(id)
                .setTopic("sport")
                .setContent("goal")
                .setAttempt(attempt)
                .setMaxAttempts(3)
                .build();
    }

    @Test
    void acknowledgesNewMessage() {
        processor.handle(message("m1", 1));

        assertEquals(List.of("m1"), acks);
        assertTrue(output.get(0).contains("NEW MESSAGE"));
        assertTrue(output.get(0).contains("Content    : goal"));
        assertTrue(output.get(0).contains("Attempt    : 1/3"));
    }

    @Test
    void duplicateIsNotShownAgainButIsAcknowledgedAgain() {
        processor.handle(message("m1", 1));
        processor.handle(message("m1", 2));

        assertEquals(List.of("m1", "m1"), acks);
        assertTrue(output.get(1).startsWith("[DUPLICATE]"));
    }

    @Test
    void autoAckOffDisplaysWithoutAcknowledging() {
        processor.toggleAutoAck();

        processor.handle(message("m1", 1));

        assertTrue(acks.isEmpty());
        assertTrue(output.get(0).contains("[ACK disabled]"));
    }

    @Test
    void messageSeenWithAckOffIsProcessedNormallyAfterAckIsTurnedOn() {
        processor.toggleAutoAck();
        processor.handle(message("m1", 1));
        processor.toggleAutoAck();

        processor.handle(message("m1", 2));

        assertEquals(List.of("m1"), acks);
        assertTrue(output.get(1).contains("NEW MESSAGE"));
    }
}
```

- [ ] **Step 5: Rulează ca să confirmi că pică**

Run (din `laborator2-gRPC/subscriber/`): `.\mvnw.cmd -q test`
Expected: prima rulare descarcă Maven 3.9.16, protoc și pluginul grpc-java, apoi compilarea testelor pică cu `cannot find symbol ... MessageProcessor`.

- [ ] **Step 6: Creează `AckSender.java`**

```java
package md.utm.pad.subscriber;

@FunctionalInterface
public interface AckSender {
    void sendAck(String messageId);
}
```

- [ ] **Step 7: Creează `MessageProcessor.java`**

```java
package md.utm.pad.subscriber;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import md.utm.pad.broker.grpc.DeliveredMessage;

public final class MessageProcessor {

    private final Set<String> processedMessageIds = ConcurrentHashMap.newKeySet();
    private final AckSender ackSender;
    private final Consumer<String> output;
    private volatile boolean autoAck = true;

    public MessageProcessor(AckSender ackSender, Consumer<String> output) {
        this.ackSender = ackSender;
        this.output = output;
    }

    public void handle(DeliveredMessage message) {
        String attempt = message.getAttempt() + "/" + message.getMaxAttempts();

        if (!autoAck) {
            output.accept(describe(message, attempt) + System.lineSeparator()
                    + "[ACK disabled] No ACK sent; the Broker will retry, then move it to the DLQ.");
            return;
        }

        if (!processedMessageIds.add(message.getMessageId())) {
            output.accept("[DUPLICATE] Message " + message.getMessageId()
                    + " already processed (attempt " + attempt + "), re-sending ACK.");
            ackSender.sendAck(message.getMessageId());
            return;
        }

        output.accept(describe(message, attempt));
        ackSender.sendAck(message.getMessageId());
    }

    public boolean toggleAutoAck() {
        autoAck = !autoAck;
        return autoAck;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    private static String describe(DeliveredMessage message, String attempt) {
        return String.join(System.lineSeparator(),
                "==========================================",
                "NEW MESSAGE",
                "Message ID : " + message.getMessageId(),
                "Topic      : " + message.getTopic(),
                "Content    : " + message.getContent(),
                "Attempt    : " + attempt,
                "==========================================");
    }
}
```

- [ ] **Step 8: Rulează testele**

Run: `.\mvnw.cmd -q test`
Expected: `Tests run: 4, Failures: 0, Errors: 0` (cu `-q`, eșecurile apar explicit; lipsa erorilor = PASS).

- [ ] **Step 9: Creează `BrokerClient.java`**

```java
package md.utm.pad.subscriber;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import md.utm.pad.broker.grpc.AckReply;
import md.utm.pad.broker.grpc.AckRequest;
import md.utm.pad.broker.grpc.DeliveredMessage;
import md.utm.pad.broker.grpc.ListenRequest;
import md.utm.pad.broker.grpc.MessageBrokerGrpc;
import md.utm.pad.broker.grpc.PingReply;
import md.utm.pad.broker.grpc.PingRequest;
import md.utm.pad.broker.grpc.SubscriptionReply;
import md.utm.pad.broker.grpc.SubscriptionRequest;

public final class BrokerClient implements AutoCloseable {

    private static final long CALL_DEADLINE_SECONDS = 5;
    private static final long RECONNECT_DELAY_SECONDS = 3;
    private static final long KEEPALIVE_SECONDS = 30;
    private static final long KEEPALIVE_TIMEOUT_SECONDS = 10;

    private final ManagedChannel channel;
    private final MessageBrokerGrpc.MessageBrokerBlockingStub blockingStub;
    private final MessageBrokerGrpc.MessageBrokerStub asyncStub;
    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "listen-reconnect");
        thread.setDaemon(true);
        return thread;
    });
    private final Consumer<String> output;
    private volatile boolean running = true;

    public BrokerClient(String host, int port, Consumer<String> output) {
        this.output = output;
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .keepAliveTime(KEEPALIVE_SECONDS, TimeUnit.SECONDS)
                .keepAliveTimeout(KEEPALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .build();
        this.blockingStub = MessageBrokerGrpc.newBlockingStub(channel);
        this.asyncStub = MessageBrokerGrpc.newStub(channel);
    }

    public SubscriptionReply subscribe(String subscriberId, String topic) {
        return blocking().subscribe(subscription(subscriberId, topic));
    }

    public SubscriptionReply unsubscribe(String subscriberId, String topic) {
        return blocking().unsubscribe(subscription(subscriberId, topic));
    }

    public PingReply ping() {
        return blocking().ping(PingRequest.getDefaultInstance());
    }

    public void ack(String subscriberId, String messageId) {
        AckRequest request = AckRequest.newBuilder()
                .setSubscriberId(subscriberId)
                .setMessageId(messageId)
                .build();
        output.accept("[ACK] Sent for message: " + messageId);
        asyncStub.withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS).ack(request, new StreamObserver<AckReply>() {
            @Override
            public void onNext(AckReply reply) {
                output.accept(reply.getDuplicate()
                        ? "[BROKER] Duplicate ACK ignored for: " + reply.getMessageId()
                        : "[BROKER] ACK confirmed for: " + reply.getMessageId());
            }

            @Override
            public void onError(Throwable error) {
                output.accept("[ACK ERROR] " + messageId + ": " + describe(error));
            }

            @Override
            public void onCompleted() {
            }
        });
    }

    public void listen(String subscriberId, Consumer<DeliveredMessage> onMessage) {
        if (!running) {
            return;
        }
        ListenRequest request = ListenRequest.newBuilder().setSubscriberId(subscriberId).build();
        asyncStub.listen(request, new StreamObserver<DeliveredMessage>() {
            @Override
            public void onNext(DeliveredMessage message) {
                onMessage.accept(message);
            }

            @Override
            public void onError(Throwable error) {
                if (!running) {
                    return;
                }
                Status status = Status.fromThrowable(error);
                if (status.getCode() == Status.Code.ABORTED && "SESSION_REPLACED".equals(status.getDescription())) {
                    output.accept("[LISTEN] Another subscriber connected with the same ID. Listening stopped.");
                    return;
                }
                if (status.getCode() == Status.Code.INVALID_ARGUMENT) {
                    output.accept("[LISTEN] Rejected by Broker: " + status.getDescription());
                    return;
                }
                output.accept("[LISTEN] Connection to Broker lost (" + status.getCode()
                        + "). Reconnecting in " + RECONNECT_DELAY_SECONDS + "s...");
                scheduleReconnect(subscriberId, onMessage);
            }

            @Override
            public void onCompleted() {
                if (running) {
                    output.accept("[LISTEN] Stream closed by Broker. Reconnecting in " + RECONNECT_DELAY_SECONDS + "s...");
                    scheduleReconnect(subscriberId, onMessage);
                }
            }
        });
    }

    public static String describe(Throwable error) {
        Status status = Status.fromThrowable(error);
        if (status.getCode() == Status.Code.UNAVAILABLE) {
            return "Broker unavailable (check host/IP, port, firewall and that the Broker is running)";
        }
        return status.getCode() + " - " + status.getDescription();
    }

    @Override
    public void close() {
        running = false;
        reconnectScheduler.shutdownNow();
        channel.shutdownNow();
        try {
            channel.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void scheduleReconnect(String subscriberId, Consumer<DeliveredMessage> onMessage) {
        try {
            reconnectScheduler.schedule(() -> listen(subscriberId, onMessage), RECONNECT_DELAY_SECONDS, TimeUnit.SECONDS);
        } catch (RejectedExecutionException ignored) {
        }
    }

    private MessageBrokerGrpc.MessageBrokerBlockingStub blocking() {
        return blockingStub.withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS);
    }

    private static SubscriptionRequest subscription(String subscriberId, String topic) {
        return SubscriptionRequest.newBuilder().setSubscriberId(subscriberId).setTopic(topic).build();
    }
}
```

- [ ] **Step 10: Creează `SubscriberApp.java`**

```java
package md.utm.pad.subscriber;

import io.grpc.StatusRuntimeException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import md.utm.pad.broker.grpc.PingReply;
import md.utm.pad.broker.grpc.SubscriptionReply;

public final class SubscriberApp {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 50051;
    private static final int MAX_ID_LENGTH = 64;

    private final BrokerClient client;
    private final MessageProcessor processor;
    private final String subscriberId;
    private final Scanner scanner;
    private final Set<String> topics = ConcurrentHashMap.newKeySet();

    private SubscriberApp(BrokerClient client, MessageProcessor processor, String subscriberId, Scanner scanner) {
        this.client = client;
        this.processor = processor;
        this.subscriberId = subscriberId;
        this.scanner = scanner;
    }

    public static void main(String[] args) {
        String host = args.length >= 1 ? args[0] : DEFAULT_HOST;
        int port = parsePort(args);

        System.out.println(" ------ SUBSCRIBER - JAVA (gRPC) ----");
        System.out.println("Broker: " + host + ":" + port);

        Scanner scanner = new Scanner(System.in);
        String defaultId = defaultSubscriberId();
        System.out.print("Subscriber ID (Enter = " + defaultId + "): ");
        String typed = scanner.hasNextLine() ? scanner.nextLine().trim() : "";
        String subscriberId = typed.isEmpty() ? defaultId : typed;

        try (BrokerClient client = new BrokerClient(host, port, SubscriberApp::print)) {
            MessageProcessor processor = new MessageProcessor(
                    messageId -> client.ack(subscriberId, messageId), SubscriberApp::print);
            new SubscriberApp(client, processor, subscriberId, scanner).run();
        }
        System.out.println();
        System.out.println("Subscriber closed.");
    }

    private void run() {
        print("[LISTEN] Listening for messages as '" + subscriberId + "'");
        client.listen(subscriberId, processor::handle);

        while (true) {
            printMenu();
            if (!scanner.hasNextLine()) {
                return;
            }
            String option = scanner.nextLine().trim();
            switch (option) {
                case "1":
                    subscribe();
                    break;
                case "2":
                    unsubscribe();
                    break;
                case "3":
                    showTopics();
                    break;
                case "4":
                    ping();
                    break;
                case "5":
                    toggleAutoAck();
                    break;
                case "0":
                    return;
                default:
                    print("Invalid option.");
            }
        }
    }

    private void printMenu() {
        System.out.println();
        System.out.println("------------------------------------------");
        System.out.println("1 - Subscribe to a topic");
        System.out.println("2 - Unsubscribe from a topic");
        System.out.println("3 - Show subscribed topics");
        System.out.println("4 - Ping Broker");
        System.out.println("5 - Auto-ACK on/off (now: " + (processor.isAutoAck() ? "ON" : "OFF") + ")");
        System.out.println("0 - Exit");
        System.out.println("------------------------------------------");
        System.out.print("Choose an option: ");
    }

    private void subscribe() {
        String topic = readTopic();
        if (topic == null) {
            return;
        }
        try {
            SubscriptionReply reply = client.subscribe(subscriberId, topic);
            topics.add(topic);
            print("[BROKER] Subscription confirmed: " + reply.getTopic());
        } catch (StatusRuntimeException e) {
            print("Subscribe error: " + BrokerClient.describe(e));
        }
    }

    private void unsubscribe() {
        String topic = readTopic();
        if (topic == null) {
            return;
        }
        try {
            SubscriptionReply reply = client.unsubscribe(subscriberId, topic);
            topics.remove(topic);
            print("[BROKER] Unsubscription confirmed: " + reply.getTopic());
        } catch (StatusRuntimeException e) {
            print("Unsubscribe error: " + BrokerClient.describe(e));
        }
    }

    private void showTopics() {
        if (topics.isEmpty()) {
            print("You are not subscribed to any topic in this session.");
            return;
        }
        StringBuilder text = new StringBuilder("Topics:");
        for (String topic : topics) {
            text.append(System.lineSeparator()).append(" - ").append(topic);
        }
        print(text.toString());
    }

    private void ping() {
        try {
            PingReply reply = client.ping();
            print("[BROKER] PONG (broker time " + reply.getServerTimeUtc() + ", storage " + reply.getStorageMode() + ")");
        } catch (StatusRuntimeException e) {
            print("Ping error: " + BrokerClient.describe(e));
        }
    }

    private void toggleAutoAck() {
        boolean enabled = processor.toggleAutoAck();
        print(enabled
                ? "Auto-ACK is now ON"
                : "Auto-ACK is now OFF (messages are not acknowledged; the Broker retries, then moves them to the DLQ)");
    }

    private String readTopic() {
        System.out.print("Topic: ");
        if (!scanner.hasNextLine()) {
            return null;
        }
        String topic = scanner.nextLine().trim();
        if (topic.isEmpty()) {
            print("Topic cannot be empty.");
            return null;
        }
        return topic;
    }

    private static synchronized void print(String text) {
        System.out.println();
        System.out.println(text);
    }

    private static int parsePort(String[] args) {
        if (args.length < 2) {
            return DEFAULT_PORT;
        }
        try {
            return Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid port '" + args[1] + "', using default " + DEFAULT_PORT);
            return DEFAULT_PORT;
        }
    }

    private static String defaultSubscriberId() {
        try {
            String host = InetAddress.getLocalHost().getHostName()
                    .toLowerCase(Locale.ROOT)
                    .replaceAll("[^a-z0-9._-]", "-");
            String id = "java-" + host;
            return id.length() > MAX_ID_LENGTH ? id.substring(0, MAX_ID_LENGTH) : id;
        } catch (UnknownHostException e) {
            return "java-subscriber-1";
        }
    }
}
```

- [ ] **Step 11: Build jar + teste**

Run (din `subscriber/`): `.\mvnw.cmd -q package; Test-Path target/subscriber.jar`
Expected: fără erori (testele rulează în `package`) și `True`.

- [ ] **Step 12: Verificare manuală cu broker + publisher**

Run (din `laborator2-gRPC/`, în două ferestre PowerShell):
1. `dotnet broker/bin/Release/net9.0/Broker.dll --port 50073 --storage transient`
2. `java -jar subscriber/target/subscriber.jar 127.0.0.1 50073`. Apasă Enter pentru ID-ul implicit (confirmă că apare `java-<hostname>`), apoi `1`, `sport`.
3. În a treia fereastră: `"1`nsport`nSalut`n0`n" | .\.venv\Scripts\python publisher/publisher.py --port 50073`

Expected la subscriber: `[BROKER] Subscription confirmed: sport`, blocul `NEW MESSAGE` cu `Content    : Salut`, `Attempt    : 1/3`, apoi `[ACK] Sent ...` și `[BROKER] ACK confirmed for: ...`. Oprește brokerul (Ctrl+C): subscriberul afișează `[LISTEN] Connection to Broker lost (UNAVAILABLE). Reconnecting in 3s...`. Repornește brokerul: mesajele de reconectare se opresc.

- [ ] **Step 13: Checkpoint** — de comis: `subscriber/mvnw`, `subscriber/mvnw.cmd`, `subscriber/.mvn/wrapper/maven-wrapper.properties`, `subscriber/pom.xml`, `subscriber/src/**`.

---

### Task 10: Harness E2E, oracol și scenariile de livrare (1, 2, 3, 6, 7)

**Files:**
- Create: `tests/e2e/clients.py`
- Create: `tests/e2e/broker_process.py`
- Create: `tests/e2e/oracle.py`
- Create: `tests/e2e/conftest.py`
- Create: `tests/e2e/test_delivery.py`

**Interfaces:**
- Consumes: `publisher/stubs.py` (Task 1), `broker/bin/Release/net9.0/Broker.dll` (Task 7).
- Produces (folosite de Task 11 și 12):
  - `clients.BrokerApi(address)` cu `publish`, `subscribe`, `unsubscribe`, `ack`, `ping`, `state(limit=1000)`, `close`, context manager; `clients.ListeningSubscriber(address, subscriber_id, auto_ack=True)` cu `messages()`, `wait_for(predicate, timeout)`, `wait_until_closed(timeout)`, `close()`, atributele `error`, `ack_errors`; `clients.wait_until(predicate, timeout, message)`; `clients.ids(messages)`; `clients.new_id()`
  - `broker_process.BrokerProcess` cu `address`, `port`, `storage`, `db_path`, `start()`, `stop()`, `restart()`, `log_text()`
  - fixture-urile `make_broker(storage, db_path=None, port=None)`, `broker` (parametrizat pe `persistent`/`transient`), `persistent_broker`, `transient_broker`
  - `oracle.message_status`, `oracle.delivery`, `oracle.messages`, `oracle.dead_letters`, `oracle.counts`, `oracle.conservation_violations`

- [ ] **Step 1: Creează `tests/e2e/clients.py`**

```python
import sys
import threading
import time
import uuid
from pathlib import Path

import grpc

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "publisher"))

import stubs

broker_pb2, broker_pb2_grpc = stubs.load()

CALL_TIMEOUT = 5


def new_id():
    return str(uuid.uuid4())


def ids(messages):
    return [message.message_id for message in messages]


def wait_until(predicate, timeout=15, message="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError(f"{message} not met within {timeout}s")


class BrokerApi:
    def __init__(self, address):
        self._channel = grpc.insecure_channel(address)
        self.stub = broker_pb2_grpc.MessageBrokerStub(self._channel)

    def publish(self, message_id, topic, content):
        request = broker_pb2.PublishRequest(message_id=message_id, topic=topic, content=content)
        return self.stub.Publish(request, timeout=CALL_TIMEOUT)

    def subscribe(self, subscriber_id, topic):
        request = broker_pb2.SubscriptionRequest(subscriber_id=subscriber_id, topic=topic)
        return self.stub.Subscribe(request, timeout=CALL_TIMEOUT)

    def unsubscribe(self, subscriber_id, topic):
        request = broker_pb2.SubscriptionRequest(subscriber_id=subscriber_id, topic=topic)
        return self.stub.Unsubscribe(request, timeout=CALL_TIMEOUT)

    def ack(self, subscriber_id, message_id):
        request = broker_pb2.AckRequest(subscriber_id=subscriber_id, message_id=message_id)
        return self.stub.Ack(request, timeout=CALL_TIMEOUT)

    def ping(self):
        return self.stub.Ping(broker_pb2.PingRequest(), timeout=CALL_TIMEOUT)

    def state(self, limit=1000):
        return self.stub.GetState(broker_pb2.StateRequest(limit=limit), timeout=CALL_TIMEOUT)

    def close(self):
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


class ListeningSubscriber:
    def __init__(self, address, subscriber_id, auto_ack=True):
        self.subscriber_id = subscriber_id
        self.auto_ack = auto_ack
        self.api = BrokerApi(address)
        self.error = None
        self.ack_errors = []
        self._received = []
        self._lock = threading.Lock()
        self._call = self.api.stub.Listen(broker_pb2.ListenRequest(subscriber_id=subscriber_id))
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        try:
            for message in self._call:
                with self._lock:
                    self._received.append(message)
                if self.auto_ack:
                    try:
                        self.api.ack(self.subscriber_id, message.message_id)
                    except grpc.RpcError as error:
                        self.ack_errors.append(error)
        except grpc.RpcError as error:
            self.error = error

    def messages(self):
        with self._lock:
            return list(self._received)

    def wait_for(self, predicate, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            current = self.messages()
            if predicate(current):
                return current
            time.sleep(0.1)
        raise AssertionError(
            f"{self.subscriber_id}: condition not met within {timeout}s; received {ids(self.messages())}")

    def wait_until_closed(self, timeout=10):
        self._thread.join(timeout)
        return self.error

    def close(self):
        self._call.cancel()
        self._thread.join(5)
        self.api.close()
```

- [ ] **Step 2: Creează `tests/e2e/broker_process.py`**

```python
import subprocess
import time

import grpc

from clients import broker_pb2, broker_pb2_grpc


class BrokerProcess:
    def __init__(self, dll, storage, db_path, port, work_dir):
        self.dll = dll
        self.storage = storage
        self.db_path = db_path
        self.port = port
        self.work_dir = work_dir
        self.log_path = work_dir / f"broker-{port}.log"
        self._process = None
        self._log = None

    @property
    def address(self):
        return f"127.0.0.1:{self.port}"

    def start(self, timeout=30):
        self._log = open(self.log_path, "a", encoding="utf-8")
        self._process = subprocess.Popen(
            ["dotnet", str(self.dll), "--port", str(self.port), "--storage", self.storage, "--db", str(self.db_path)],
            cwd=self.work_dir,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise RuntimeError(f"broker exited early, see {self.log_path}")
            try:
                with grpc.insecure_channel(self.address) as channel:
                    broker_pb2_grpc.MessageBrokerStub(channel).Ping(broker_pb2.PingRequest(), timeout=1)
                return
            except grpc.RpcError:
                time.sleep(0.2)
        self.stop()
        raise TimeoutError(f"broker did not answer Ping within {timeout}s, see {self.log_path}")

    def stop(self):
        if self._process is not None and self._process.poll() is None:
            self._process.kill()
            self._process.wait(timeout=10)
        if self._log is not None:
            self._log.close()
            self._log = None

    def restart(self):
        self.stop()
        self.start()

    def log_text(self):
        return self.log_path.read_text(encoding="utf-8", errors="replace")
```

- [ ] **Step 3: Creează `tests/e2e/oracle.py`**

Oracolul citește SQLite direct, cu SQL propriu. Nu importă nimic din `broker/` și nu apelează gRPC.

```python
import sqlite3
from contextlib import closing
from pathlib import Path

STATUSES = ("PENDING", "DELIVERED", "DEAD")


def _rows(db_path, sql, params=()):
    uri = f"{Path(db_path).resolve().as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=5)) as connection:
        return connection.execute(sql, params).fetchall()


def message_status(db_path, message_id):
    rows = _rows(db_path, "SELECT Status FROM Messages WHERE MessageId = ?", (message_id,))
    return rows[0][0] if rows else None


def messages(db_path):
    return _rows(db_path, "SELECT MessageId, Topic, Content, Status FROM Messages")


def delivery(db_path, message_id, subscriber_id):
    rows = _rows(
        db_path,
        "SELECT Status, RetryCount FROM Deliveries WHERE MessageId = ? AND SubscriberId = ?",
        (message_id, subscriber_id),
    )
    return rows[0] if rows else None


def dead_letters(db_path):
    return _rows(db_path, "SELECT MessageId, SubscriberId, Topic, RawPayload, Reason, RetryCount FROM DeadLetters ORDER BY Id")


def counts(db_path):
    return {
        "messages": dict(_rows(db_path, "SELECT Status, COUNT(*) FROM Messages GROUP BY Status")),
        "deliveries": dict(_rows(db_path, "SELECT Status, COUNT(*) FROM Deliveries GROUP BY Status")),
        "dead_letters_invalid": _rows(db_path, "SELECT COUNT(*) FROM DeadLetters WHERE MessageId IS NULL")[0][0],
        "dead_letters_delivery": _rows(db_path, "SELECT COUNT(*) FROM DeadLetters WHERE MessageId IS NOT NULL")[0][0],
    }


def _expected_message_status(delivery_statuses):
    if not delivery_statuses or "PENDING" in delivery_statuses:
        return "PENDING"
    if "DELIVERED" in delivery_statuses:
        return "DELIVERED"
    return "DEAD"


def conservation_violations(db_path):
    problems = []
    message_rows = _rows(db_path, "SELECT MessageId, Status FROM Messages")
    delivery_rows = _rows(db_path, "SELECT MessageId, SubscriberId, Status FROM Deliveries")
    dlq_pairs = sorted(_rows(db_path, "SELECT MessageId, SubscriberId FROM DeadLetters WHERE MessageId IS NOT NULL"))

    statuses_by_message = {}
    for message_id, _, status in delivery_rows:
        statuses_by_message.setdefault(message_id, []).append(status)

    message_buckets = {status: 0 for status in STATUSES}
    for message_id, status in message_rows:
        if status not in STATUSES:
            problems.append(f"message {message_id} has unknown status {status}")
            continue
        message_buckets[status] += 1
        expected = _expected_message_status(statuses_by_message.get(message_id, []))
        if status != expected:
            problems.append(f"message {message_id} is {status} but its deliveries imply {expected}")
    if sum(message_buckets.values()) != len(message_rows):
        problems.append(f"messages: {len(message_rows)} rows but buckets {message_buckets}")

    known_messages = {message_id for message_id, _ in message_rows}
    delivery_buckets = {status: 0 for status in STATUSES}
    for message_id, subscriber_id, status in delivery_rows:
        if message_id not in known_messages:
            problems.append(f"delivery {message_id}/{subscriber_id} references a missing message")
        if status not in STATUSES:
            problems.append(f"delivery {message_id}/{subscriber_id} has unknown status {status}")
        else:
            delivery_buckets[status] += 1
    if sum(delivery_buckets.values()) != len(delivery_rows):
        problems.append(f"deliveries: {len(delivery_rows)} rows but buckets {delivery_buckets}")

    dead_pairs = sorted((m, s) for m, s, status in delivery_rows if status == "DEAD")
    if dead_pairs != dlq_pairs:
        problems.append(f"DEAD deliveries {dead_pairs} do not match DLQ rows {dlq_pairs}")
    return problems
```

- [ ] **Step 4: Creează `tests/e2e/conftest.py`**

```python
import socket
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from broker_process import BrokerProcess

ROOT = Path(__file__).resolve().parents[2]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="session")
def broker_dll():
    subprocess.run(
        ["dotnet", "build", str(ROOT / "broker" / "Broker.csproj"), "-c", "Release", "--nologo", "-v", "q"],
        check=True,
    )
    return ROOT / "broker" / "bin" / "Release" / "net9.0" / "Broker.dll"


@pytest.fixture
def make_broker(broker_dll, tmp_path):
    started = []

    def factory(storage="persistent", db_path=None, port=None):
        broker = BrokerProcess(
            broker_dll,
            storage=storage,
            db_path=db_path or tmp_path / "broker.db",
            port=port or free_port(),
            work_dir=tmp_path,
        )
        broker.start()
        started.append(broker)
        return broker

    yield factory
    for broker in started:
        broker.stop()


@pytest.fixture(params=["persistent", "transient"])
def broker(request, make_broker):
    return make_broker(storage=request.param)


@pytest.fixture
def persistent_broker(make_broker):
    return make_broker(storage="persistent")


@pytest.fixture
def transient_broker(make_broker):
    return make_broker(storage="transient")
```

- [ ] **Step 5: Scrie scenariile — `tests/e2e/test_delivery.py`**

```python
import grpc
import pytest

import oracle
from clients import BrokerApi, ListeningSubscriber, ids, new_id, wait_until


def state_message(api, message_id):
    return next(m for m in api.state().messages if m.message_id == message_id)


def assert_oracle_clean(broker):
    if broker.storage == "persistent":
        assert oracle.conservation_violations(broker.db_path) == []


def test_online_subscriber_receives_and_acknowledges(broker):
    with BrokerApi(broker.address) as api:
        api.subscribe("sub-1", "sport")
        subscriber = ListeningSubscriber(broker.address, "sub-1")
        try:
            message_id = new_id()
            reply = api.publish(message_id, "sport", "goal")
            assert reply.subscribers == 1

            received = subscriber.wait_for(lambda ms: message_id in ids(ms))
            assert [(m.topic, m.content, m.attempt, m.max_attempts) for m in received] == [("sport", "goal", 1, 3)]
            wait_until(lambda: state_message(api, message_id).status == "DELIVERED", message="message DELIVERED")
            assert subscriber.ack_errors == []
        finally:
            subscriber.close()
    if broker.storage == "persistent":
        assert oracle.message_status(broker.db_path, message_id) == "DELIVERED"
        assert oracle.delivery(broker.db_path, message_id, "sub-1") == ("DELIVERED", 1)
    assert_oracle_clean(broker)


def test_message_without_subscribers_is_kept_and_delivered_as_backlog(broker):
    with BrokerApi(broker.address) as api:
        message_id = new_id()
        reply = api.publish(message_id, "news", "breaking")
        assert reply.subscribers == 0
        assert state_message(api, message_id).status == "PENDING"

        subscriber = ListeningSubscriber(broker.address, "late-sub")
        try:
            api.subscribe("late-sub", "news")
            received = subscriber.wait_for(lambda ms: message_id in ids(ms))
            assert received[0].content == "breaking"
            wait_until(lambda: state_message(api, message_id).status == "DELIVERED", message="backlog DELIVERED")
        finally:
            subscriber.close()
    assert_oracle_clean(broker)


def test_missing_ack_moves_delivery_to_dead_letter_queue(broker):
    with BrokerApi(broker.address) as api:
        subscriber = ListeningSubscriber(broker.address, "lazy", auto_ack=False)
        try:
            api.subscribe("lazy", "alerts")
            message_id = new_id()
            api.publish(message_id, "alerts", "fire")

            wait_until(lambda: state_message(api, message_id).status == "DEAD", timeout=60, message="message DEAD")

            attempts = [m.attempt for m in subscriber.messages() if m.message_id == message_id]
            assert attempts == [1, 2, 3]
            dead = [d for d in api.state().dead_letters if d.message_id == message_id]
            assert len(dead) == 1
            assert (dead[0].subscriber_id, dead[0].reason, dead[0].retry_count) == (
                "lazy", "ACK_NOT_RECEIVED_AFTER_MAX_RETRIES", 3)
        finally:
            subscriber.close()
    if broker.storage == "persistent":
        assert oracle.delivery(broker.db_path, message_id, "lazy") == ("DEAD", 3)
    assert_oracle_clean(broker)


def test_duplicate_ack_and_unknown_ack(broker):
    with BrokerApi(broker.address) as api:
        api.subscribe("sub-ack", "sport")
        subscriber = ListeningSubscriber(broker.address, "sub-ack")
        try:
            message_id = new_id()
            api.publish(message_id, "sport", "goal")
            wait_until(lambda: state_message(api, message_id).status == "DELIVERED", message="DELIVERED")

            reply = api.ack("sub-ack", message_id)
            assert (reply.message_id, reply.duplicate) == (message_id, True)

            with pytest.raises(grpc.RpcError) as error:
                api.ack("sub-ack", new_id())
            assert error.value.code() == grpc.StatusCode.NOT_FOUND
            assert error.value.details() == "DELIVERY_NOT_FOUND"
        finally:
            subscriber.close()
    assert_oracle_clean(broker)


def test_reconnecting_subscriber_gets_unacked_message_again(broker):
    with BrokerApi(broker.address) as api:
        api.subscribe("flaky", "orders")
        first = ListeningSubscriber(broker.address, "flaky", auto_ack=False)
        message_id = new_id()
        api.publish(message_id, "orders", "order-1")
        first.wait_for(lambda ms: message_id in ids(ms))
        first.close()

        second = ListeningSubscriber(broker.address, "flaky")
        try:
            received = second.wait_for(lambda ms: message_id in ids(ms), timeout=20)
            assert received[0].attempt == 2
            wait_until(lambda: state_message(api, message_id).status == "DELIVERED", message="DELIVERED after reconnect")
        finally:
            second.close()
    assert_oracle_clean(broker)


def test_pending_messages_survive_broker_restart(persistent_broker):
    with BrokerApi(persistent_broker.address) as api:
        api.subscribe("offline-sub", "orders")
        message_ids = [new_id() for _ in range(3)]
        for index, message_id in enumerate(message_ids):
            api.publish(message_id, "orders", f"order-{index}")

    persistent_broker.restart()
    assert [oracle.message_status(persistent_broker.db_path, m) for m in message_ids] == ["PENDING"] * 3

    subscriber = ListeningSubscriber(persistent_broker.address, "offline-sub")
    try:
        received = subscriber.wait_for(lambda ms: set(message_ids) <= set(ids(ms)))
        assert ids(received)[:3] == message_ids
        wait_until(
            lambda: all(oracle.message_status(persistent_broker.db_path, m) == "DELIVERED" for m in message_ids),
            message="all DELIVERED after restart")
    finally:
        subscriber.close()
    assert oracle.conservation_violations(persistent_broker.db_path) == []
```

- [ ] **Step 6: Rulează scenariile**

Run (din `laborator2-gRPC/`): `.\.venv\Scripts\python -m pytest tests/e2e/test_delivery.py -v`
Expected: 11 teste PASS (5 teste × 2 moduri + restart în persistent). Durată de aproximativ 1–2 minute, din cauza scenariului DLQ.

Dacă un test pică, citește log-ul brokerului din `tmp_path` (calea apare în mesajul de eroare) și aplică `superpowers:systematic-debugging` înainte de orice modificare.

- [ ] **Step 7: Checkpoint** — de comis: `tests/e2e/clients.py`, `broker_process.py`, `oracle.py`, `conftest.py`, `test_delivery.py`.

---

### Task 11: Scenarii E2E de validare, integritate, concurență, transient și rețea (4, 5, 8–13)

**Files:**
- Create: `tests/e2e/test_validation.py`
- Create: `tests/e2e/test_integrity.py`
- Create: `tests/e2e/test_concurrency.py`
- Create: `tests/e2e/test_transient.py`
- Create: `tests/e2e/test_network.py`

**Interfaces:**
- Consumes: tot din Task 10.

- [ ] **Step 1: Creează `tests/e2e/test_validation.py`**

```python
import grpc
import pytest

import oracle
from clients import BrokerApi, broker_pb2, new_id


@pytest.mark.parametrize(
    "message_id, topic, content, expected",
    [
        ("", "sport", "text", "MESSAGE_ID_REQUIRED"),
        (None, "", "text", "TOPIC_REQUIRED"),
        (None, "sport", "   ", "CONTENT_REQUIRED"),
        (None, "bad topic!", "text", "TOPIC_INVALID_CHARS"),
        (None, "t" * 65, "text", "TOPIC_TOO_LONG"),
        (None, "sport", "x" * 4097, "CONTENT_TOO_LONG"),
        (None, "", "", "TOPIC_REQUIRED,CONTENT_REQUIRED"),
    ],
)
def test_invalid_publish_is_rejected_and_dead_lettered(broker, message_id, topic, content, expected):
    with BrokerApi(broker.address) as api:
        with pytest.raises(grpc.RpcError) as error:
            api.publish(new_id() if message_id is None else message_id, topic, content)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert error.value.details() == expected

        state = api.state()
        assert [(d.message_id, d.reason) for d in state.dead_letters] == [("", expected)]
        assert len(state.messages) == 0

    if broker.storage == "persistent":
        rows = oracle.dead_letters(broker.db_path)
        assert len(rows) == 1
        stored_message_id, subscriber_id, _, raw_payload, reason, retry_count = rows[0]
        assert (stored_message_id, subscriber_id, reason, retry_count) == (None, None, expected, 0)
        assert '"topic"' in raw_payload and '"content"' in raw_payload
        assert oracle.counts(broker.db_path)["dead_letters_invalid"] == 1
        assert oracle.conservation_violations(broker.db_path) == []


def test_duplicate_message_id_is_rejected_without_overwriting(broker):
    with BrokerApi(broker.address) as api:
        message_id = new_id()
        api.publish(message_id, "sport", "first")
        with pytest.raises(grpc.RpcError) as error:
            api.publish(message_id, "sport", "second")
        assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS
        assert error.value.details() == "DUPLICATE_MESSAGE_ID"

        stored = [m for m in api.state().messages if m.message_id == message_id]
        assert [m.content for m in stored] == ["first"]
        assert len(api.state().dead_letters) == 0
    if broker.storage == "persistent":
        assert [row[2] for row in oracle.messages(broker.db_path)] == ["first"]


def test_invalid_subscribe_is_rejected_without_dead_letter(broker):
    with BrokerApi(broker.address) as api:
        with pytest.raises(grpc.RpcError) as error:
            api.subscribe("java pc", "sport")
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert error.value.details() == "SUBSCRIBER_ID_INVALID_CHARS"
        assert len(api.state().dead_letters) == 0
        assert len(api.state().subscriptions) == 0


def test_corrupted_and_oversized_payloads_do_not_crash_broker(persistent_broker):
    with grpc.insecure_channel(persistent_broker.address) as channel:
        raw_publish = channel.unary_unary("/pad.broker.v1.MessageBroker/Publish")

        with pytest.raises(grpc.RpcError):
            raw_publish(b"\x0a\x05ab", timeout=5)

        oversized = broker_pb2.PublishRequest(message_id=new_id(), topic="sport", content="x" * 70000)
        with pytest.raises(grpc.RpcError) as error:
            raw_publish(oversized.SerializeToString(), timeout=5)
        assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED

    with BrokerApi(persistent_broker.address) as api:
        assert api.ping().storage_mode == "persistent"
    assert oracle.messages(persistent_broker.db_path) == []
    assert oracle.dead_letters(persistent_broker.db_path) == []
```

- [ ] **Step 2: Creează `tests/e2e/test_integrity.py`**

```python
import oracle
from clients import BrokerApi, ListeningSubscriber, ids, new_id

SAMPLE = 'Ăâîșț ĂÂÎȘȚ „ghilimele” "quotes" \'apostrof\' \\backslash\nlinie nouă\ttab 😀🚀 中文 العربية '


def test_content_is_not_altered(persistent_broker):
    content = (SAMPLE * 200)[:4096]
    assert len(content) == 4096

    with BrokerApi(persistent_broker.address) as api:
        api.subscribe("integrity", "unicode")
        subscriber = ListeningSubscriber(persistent_broker.address, "integrity")
        try:
            message_id = new_id()
            api.publish(message_id, "unicode", content)
            received = subscriber.wait_for(lambda ms: message_id in ids(ms))
        finally:
            subscriber.close()

    assert received[0].content == content
    assert received[0].content.encode("utf-8") == content.encode("utf-8")
    stored = [row for row in oracle.messages(persistent_broker.db_path) if row[0] == message_id]
    assert stored[0][2] == content
```

- [ ] **Step 3: Creează `tests/e2e/test_concurrency.py`**

```python
import threading
from concurrent.futures import ThreadPoolExecutor

import oracle
from clients import BrokerApi, ListeningSubscriber, ids, new_id, wait_until

PUBLISHERS = 5
PER_PUBLISHER = 40
TOPICS_BY_SUBSCRIBER = {"sub-a": ["t1"], "sub-b": ["t1", "t2"], "sub-c": ["t2"]}


def test_parallel_publishers_deliver_each_message_exactly_once(broker):
    with BrokerApi(broker.address) as api:
        for subscriber_id, topics in TOPICS_BY_SUBSCRIBER.items():
            for topic in topics:
                api.subscribe(subscriber_id, topic)

    subscribers = {sid: ListeningSubscriber(broker.address, sid) for sid in TOPICS_BY_SUBSCRIBER}
    published = {"t1": [], "t2": []}
    lock = threading.Lock()

    def publish_batch(index):
        with BrokerApi(broker.address) as api:
            for n in range(PER_PUBLISHER):
                topic = "t1" if (index + n) % 2 == 0 else "t2"
                message_id = new_id()
                api.publish(message_id, topic, f"p{index}-m{n}")
                with lock:
                    published[topic].append(message_id)

    try:
        with ThreadPoolExecutor(PUBLISHERS) as pool:
            list(pool.map(publish_batch, range(PUBLISHERS)))

        total = PUBLISHERS * PER_PUBLISHER
        assert len(published["t1"]) + len(published["t2"]) == total

        for subscriber_id, topics in TOPICS_BY_SUBSCRIBER.items():
            expected = {m for topic in topics for m in published[topic]}
            received = subscribers[subscriber_id].wait_for(lambda ms, e=expected: e <= set(ids(ms)), timeout=60)
            assert set(ids(received)) == expected
            assert len(ids(received)) == len(expected), f"{subscriber_id} received duplicates"

        expected_deliveries = 2 * len(published["t1"]) + 2 * len(published["t2"])
        with BrokerApi(broker.address) as api:
            wait_until(
                lambda: sum(1 for d in api.state().deliveries if d.status == "DELIVERED") == expected_deliveries,
                timeout=30, message="all deliveries DELIVERED")
            state = api.state()
            assert len(state.messages) == total
            assert len(state.deliveries) == expected_deliveries
    finally:
        for subscriber in subscribers.values():
            subscriber.close()

    if broker.storage == "persistent":
        counts = oracle.counts(broker.db_path)
        assert counts["messages"] == {"DELIVERED": total}
        assert counts["deliveries"] == {"DELIVERED": expected_deliveries}
        assert oracle.conservation_violations(broker.db_path) == []
```

- [ ] **Step 4: Creează `tests/e2e/test_transient.py`**

```python
from clients import BrokerApi, new_id


def test_transient_state_is_lost_on_restart_and_no_file_is_written(transient_broker, tmp_path):
    with BrokerApi(transient_broker.address) as api:
        api.subscribe("sub-t", "sport")
        api.publish(new_id(), "sport", "goal")
        state = api.state()
        assert state.storage_mode == "transient"
        assert len(state.messages) == 1

    transient_broker.restart()

    with BrokerApi(transient_broker.address) as api:
        state = api.state()
        assert len(state.messages) == 0
        assert len(state.subscriptions) == 0
    assert not (tmp_path / "broker.db").exists()
```

- [ ] **Step 5: Creează `tests/e2e/test_network.py`**

```python
import socket

import grpc
import pytest

from clients import BrokerApi, ListeningSubscriber, ids, new_id, wait_until


def lan_ipv4():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            sock.connect(("10.255.255.255", 1))
            address = sock.getsockname()[0]
        except OSError:
            return None
    return None if address.startswith("127.") else address


def test_clients_reach_broker_through_lan_address(persistent_broker):
    ip = lan_ipv4()
    if ip is None:
        pytest.skip("no LAN IPv4 address on this machine")
    address = f"{ip}:{persistent_broker.port}"

    with BrokerApi(address) as api:
        api.subscribe("lan-sub", "lan")
        subscriber = ListeningSubscriber(address, "lan-sub")
        try:
            message_id = new_id()
            api.publish(message_id, "lan", "over the network")
            received = subscriber.wait_for(lambda ms: message_id in ids(ms))
            assert received[0].content == "over the network"
        finally:
            subscriber.close()

    log = persistent_broker.log_text()
    assert f"connect to {ip}:{persistent_broker.port}" in log
    assert ip in log.split("[ONLINE] lan-sub from", 1)[1].splitlines()[0]


def test_second_connection_with_same_id_replaces_first(persistent_broker):
    with BrokerApi(persistent_broker.address) as api:
        api.subscribe("dup-id", "dup")
        first = ListeningSubscriber(persistent_broker.address, "dup-id")
        wait_until(lambda: "dup-id" in api.state().online_subscribers, message="first online")

        second = ListeningSubscriber(persistent_broker.address, "dup-id")
        try:
            error = first.wait_until_closed(10)
            assert error is not None
            assert error.code() == grpc.StatusCode.ABORTED
            assert error.details() == "SESSION_REPLACED"

            message_id = new_id()
            api.publish(message_id, "dup", "for the newest connection")
            second.wait_for(lambda ms: message_id in ids(ms))
            assert message_id not in ids(first.messages())
        finally:
            second.close()
            first.close()
```

- [ ] **Step 6: Rulează toată suita E2E**

Run: `.\.venv\Scripts\python -m pytest tests/e2e -v`
Expected: toate testele PASS. `test_clients_reach_broker_through_lan_address` poate fi SKIPPED doar dacă mașina nu are rețea; motivul se raportează. Durată de aproximativ 3–5 minute.

- [ ] **Step 7: Checkpoint** — de comis: `tests/e2e/test_validation.py`, `test_integrity.py`, `test_concurrency.py`, `test_transient.py`, `test_network.py`.

---

### Task 12: Interoperabilitate pe procesele reale, README și verificarea finală

**Files:**
- Create: `tests/e2e/test_interop.py`
- Create: `README.md`

**Interfaces:**
- Consumes: `publisher/publisher.py` (Task 8), `subscriber/target/subscriber.jar` (Task 9), fixture-urile din Task 10.

- [ ] **Step 1: Creează `tests/e2e/test_interop.py`**

```python
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import oracle
from clients import wait_until

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def subscriber_jar():
    subscriber_dir = ROOT / "subscriber"
    subprocess.run(["cmd", "/c", str(subscriber_dir / "mvnw.cmd"), "-q", "package", "-DskipTests"],
                   cwd=subscriber_dir, check=True)
    return subscriber_dir / "target" / "subscriber.jar"


class JavaSubscriber:
    def __init__(self, jar, port):
        self.process = subprocess.Popen(
            ["java", "-jar", str(jar), "127.0.0.1", str(port)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1,
        )
        self._lines = queue.Queue()
        self.output = ""
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        for line in self.process.stdout:
            self._lines.put(line)

    def send(self, line):
        self.process.stdin.write(line + "\n")
        self.process.stdin.flush()

    def wait_for(self, text, timeout=30, count=1):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            while not self._lines.empty():
                self.output += self._lines.get()
            if self.output.count(text) >= count:
                return
            time.sleep(0.1)
        raise AssertionError(f"Java subscriber did not print {text!r} x{count}; output:\n{self.output}")

    def stop(self):
        if self.process.poll() is None:
            try:
                self.send("0")
                self.process.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                self.process.kill()


def run_publisher(port, script):
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    result = subprocess.run(
        [sys.executable, "-u", str(ROOT / "publisher" / "publisher.py"), "--port", str(port)],
        input=script, capture_output=True, text=True, encoding="utf-8", timeout=60, env=env,
    )
    return result.stdout


def test_python_publisher_and_java_subscriber_through_csharp_broker(persistent_broker, subscriber_jar):
    java = JavaSubscriber(subscriber_jar, persistent_broker.port)
    try:
        java.send("java-interop")
        java.send("1")
        java.send("interop")
        java.wait_for("[BROKER] Subscription confirmed: interop")

        output = run_publisher(persistent_broker.port, "1\ninterop\nSalut din Python\n0\n")
        assert "Subscribers found: 1" in output

        java.wait_for("Content    : Salut din Python")
        java.wait_for("[BROKER] ACK confirmed for:")

        java.send("5")
        java.wait_for("Auto-ACK is now OFF")
        output = run_publisher(persistent_broker.port, "1\ninterop\nFara ACK\n0\n")
        assert "Subscribers found: 1" in output

        def dead_message_ids():
            return [row[0] for row in oracle.messages(persistent_broker.db_path) if row[2] == "Fara ACK" and row[3] == "DEAD"]

        wait_until(lambda: len(dead_message_ids()) == 1, timeout=60, message="'Fara ACK' moved to DLQ")
        java.wait_for("Content    : Fara ACK", count=3)
        java.wait_for("[ACK disabled]", count=3)
    finally:
        java.stop()

    statuses = sorted(row[3] for row in oracle.messages(persistent_broker.db_path))
    assert statuses == ["DEAD", "DELIVERED"]
    assert oracle.conservation_violations(persistent_broker.db_path) == []
```

- [ ] **Step 2: Rulează testul de interoperabilitate**

Run: `.\.venv\Scripts\python -m pytest tests/e2e/test_interop.py -v`
Expected: PASS (aproximativ 40–60 s).

- [ ] **Step 3: Creează `README.md`**

````markdown
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
`clients on other computers can connect to 192.168.1.15:50051`.

### 2. Publisher (Python)

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r publisher/requirements.txt
.\.venv\Scripts\python publisher/publisher.py --host 192.168.1.15 --port 50051
```

### 3. Subscriber (Java)

```powershell
cd subscriber
.\mvnw.cmd package
java -jar target/subscriber.jar 192.168.1.15 50051
```

Pe Linux/macOS se folosește `./mvnw package`.

## Rulare pe mai multe calculatoare

1. Toate calculatoarele trebuie să fie în aceeași rețea (același Wi-Fi sau LAN).
2. Pe calculatorul cu brokerul, permite conexiunile venite pe port. PowerShell **ca administrator**:
   ```powershell
   New-NetFirewallRule -DisplayName "PAD gRPC Broker" -Direction Inbound -Protocol TCP -LocalPort 50051 -Action Allow
   ```
   Pentru ștergerea regulii: `Remove-NetFirewallRule -DisplayName "PAD gRPC Broker"`.
3. Pornește brokerul și notează adresa afișată (`192.168.x.x:50051`).
4. Pe celelalte calculatoare pornește publisher-ul și subscriber-ul cu acea adresă.
5. Fiecare subscriber are nevoie de un ID unic. Implicit, ID-ul este `java-<numele calculatorului>`.

Verificare rapidă de pe alt calculator: opțiunea `5 - Ping Broker` din publisher.

## Teste

```powershell
dotnet test broker.Tests
cd subscriber; .\mvnw.cmd test; cd ..
.\.venv\Scripts\python -m pip install -r tests/e2e/requirements.txt
.\.venv\Scripts\python -m pytest tests/e2e -v
```

Testele E2E pornesc brokerul real pe porturi și baze de date temporare. Verificarea finală citește
SQLite direct. Testul de rețea se conectează prin IP-ul LAN al mașinii, nu prin `127.0.0.1`.
````

- [ ] **Step 4: Verificarea finală — toate suitele, rulate proaspăt**

Folosește `superpowers:verification-before-completion`. Run:
```powershell
dotnet test broker.Tests --nologo; Push-Location subscriber; .\mvnw.cmd -q test; Pop-Location; .\.venv\Scripts\python -m pytest tests/e2e -v
```
Expected: 0 eșecuri în toate cele trei suite. Raportul către Anastasia include numărul exact de teste trecute din fiecare suită și orice test SKIPPED, cu motivul.

- [ ] **Step 5: Review final pe cerința originală**

Dispatch `feature-dev:code-reviewer` pe **opus**, cu textul integral al cerinței laboratorului (Partea 1 + Partea 2), cerința suplimentară de rulare pe mai multe calculatoare și instrucțiunea: „Specificația poate fi greșită: caută cerințe respectate în literă, dar ratate în scop.” Reviewerul primește lista de fișiere din `laborator2-gRPC/`, nu planul.

- [ ] **Step 6: Checkpoint** — de comis: `tests/e2e/test_interop.py`, `README.md`. Fără commit din partea agentului.
