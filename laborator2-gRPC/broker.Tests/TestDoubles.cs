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
