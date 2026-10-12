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
