using Microsoft.Extensions.Hosting;

namespace Broker.Delivery;

public sealed class RetryScheduler(
    SubscriberRegistry registry,
    DeliveryJobQueue queue,
    BrokerOptions options,
    TimeProvider time) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.RetryInterval, time);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                foreach (string subscriberId in registry.OnlineSubscriberIds)
                    queue.Enqueue(subscriberId);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
