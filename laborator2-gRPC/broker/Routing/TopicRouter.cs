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
