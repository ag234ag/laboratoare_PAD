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
