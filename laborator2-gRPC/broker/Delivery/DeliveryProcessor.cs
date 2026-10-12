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
