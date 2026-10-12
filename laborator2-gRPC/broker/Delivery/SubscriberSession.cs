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
