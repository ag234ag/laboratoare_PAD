using Broker.Logging;
using Microsoft.Extensions.Hosting;

namespace Broker.Delivery;

public sealed class DeliveryWorkerPool(
    DeliveryJobQueue queue,
    DeliveryProcessor processor,
    BrokerOptions options,
    IBrokerLog log) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.WhenAll(Enumerable.Range(1, options.Workers)
            .Select(workerId => Task.Run(() => RunWorkerAsync(workerId, stoppingToken), stoppingToken)));

    private async Task RunWorkerAsync(int workerId, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            string subscriberId;
            try
            {
                subscriberId = await queue.DequeueAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                await processor.ProcessAsync(subscriberId, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                log.Error($"[WORKER {workerId}] delivery to {subscriberId} failed: {ex.Message}");
            }
        }
    }
}
