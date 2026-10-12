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
