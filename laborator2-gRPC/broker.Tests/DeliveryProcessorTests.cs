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
