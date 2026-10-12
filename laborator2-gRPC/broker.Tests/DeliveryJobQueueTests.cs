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
