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

    [Fact]
    public void CloseAll_closes_every_session_without_marking_them_replaced()
    {
        var registry = new SubscriberRegistry();
        SubscriberSession a = registry.Open("sub-a", "test");
        SubscriberSession b = registry.Open("sub-b", "test");

        registry.CloseAll();

        Assert.True(a.Closed.IsCancellationRequested);
        Assert.True(b.Closed.IsCancellationRequested);
        Assert.False(a.IsReplaced);
        Assert.False(b.IsReplaced);
        Assert.Empty(registry.OnlineSubscriberIds);
    }
}
