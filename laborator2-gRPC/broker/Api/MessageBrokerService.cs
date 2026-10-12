using Broker.Contracts;
using Broker.Delivery;
using Broker.Logging;
using Broker.Routing;
using Broker.Storage;
using Broker.Validation;
using Grpc.Core;

namespace Broker.Api;

public sealed class MessageBrokerService(
    RequestValidator validator,
    TopicRouter router,
    IDeliveryStore deliveries,
    IDeadLetterStore deadLetters,
    IStateReader state,
    SubscriberRegistry registry,
    DeliveryJobQueue queue,
    BrokerOptions options,
    TimeProvider time,
    IBrokerLog log) : MessageBroker.MessageBrokerBase
{
    private const int DefaultStateLimit = 100;
    private const int MaxStateLimit = 1000;

    public override async Task<PublishReply> Publish(PublishRequest request, ServerCallContext context)
    {
        IReadOnlyList<string> errors = validator.ValidatePublish(request);
        if (errors.Count > 0)
        {
            string reason = string.Join(",", errors);
            await deadLetters.AddInvalidPayloadAsync(PayloadFormatter.Format(request), reason, time.GetUtcNow(), CancellationToken.None);
            log.Info($"[DLQ] invalid publish from {context.Peer}: {reason}");
            throw new RpcException(new Status(StatusCode.InvalidArgument, reason));
        }

        log.Info($"[RECEIVED from {context.Peer}] publish {request.MessageId} topic={request.Topic}");
        PublishResult result = await router.PublishAsync(
            new StoredMessage(request.MessageId, request.Topic, request.Content), context.CancellationToken);
        if (!result.Inserted)
        {
            log.Info($"[DUPLICATE] message {request.MessageId} already exists (from {context.Peer})");
            throw new RpcException(new Status(StatusCode.AlreadyExists, "DUPLICATE_MESSAGE_ID"));
        }

        return new PublishReply
        {
            MessageId = request.MessageId,
            Topic = request.Topic,
            Subscribers = result.Recipients.Count
        };
    }

    public override async Task<SubscriptionReply> Subscribe(SubscriptionRequest request, ServerCallContext context)
    {
        EnsureValid(validator.ValidateSubscription(request));
        log.Info($"[RECEIVED from {context.Peer}] subscribe {request.SubscriberId} -> {request.Topic}");
        await router.SubscribeAsync(request.SubscriberId, request.Topic, context.CancellationToken);
        return new SubscriptionReply { SubscriberId = request.SubscriberId, Topic = request.Topic };
    }

    public override async Task<SubscriptionReply> Unsubscribe(SubscriptionRequest request, ServerCallContext context)
    {
        EnsureValid(validator.ValidateSubscription(request));
        log.Info($"[RECEIVED from {context.Peer}] unsubscribe {request.SubscriberId} -> {request.Topic}");
        await router.UnsubscribeAsync(request.SubscriberId, request.Topic, context.CancellationToken);
        return new SubscriptionReply { SubscriberId = request.SubscriberId, Topic = request.Topic };
    }

    public override async Task Listen(ListenRequest request, IServerStreamWriter<DeliveredMessage> responseStream, ServerCallContext context)
    {
        EnsureValid(validator.ValidateListen(request));
        SubscriberSession session = registry.Open(request.SubscriberId, context.Peer);
        log.Info($"[ONLINE] {request.SubscriberId} from {context.Peer}");
        queue.Enqueue(request.SubscriberId);

        try
        {
            await foreach (DeliveredMessage message in session.Outbox.ReadAllAsync(context.CancellationToken))
                await responseStream.WriteAsync(message);
        }
        catch (Exception) when (context.CancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            registry.Close(session);
            log.Info(session.IsReplaced
                ? $"[REPLACED] {request.SubscriberId} ({context.Peer}) by a newer connection"
                : $"[OFFLINE] {request.SubscriberId} ({context.Peer})");
        }

        if (session.IsReplaced)
            throw new RpcException(new Status(StatusCode.Aborted, "SESSION_REPLACED"));
    }

    public override async Task<AckReply> Ack(AckRequest request, ServerCallContext context)
    {
        EnsureValid(validator.ValidateAck(request));
        AckOutcome outcome = await deliveries.AcknowledgeAsync(request.MessageId, request.SubscriberId, context.CancellationToken);
        switch (outcome)
        {
            case AckOutcome.Acked:
                log.Info($"[ACK] {request.MessageId} confirmed by {request.SubscriberId} (from {context.Peer})");
                return new AckReply { MessageId = request.MessageId };
            case AckOutcome.Duplicate:
                log.Info($"[DUPLICATE ACK] {request.MessageId} from {request.SubscriberId} ({context.Peer})");
                return new AckReply { MessageId = request.MessageId, Duplicate = true };
            default:
                throw new RpcException(new Status(StatusCode.NotFound, "DELIVERY_NOT_FOUND"));
        }
    }

    public override Task<PingReply> Ping(PingRequest request, ServerCallContext context) =>
        Task.FromResult(new PingReply
        {
            ServerTimeUtc = Timestamps.Format(time.GetUtcNow()),
            StorageMode = options.StorageName
        });

    public override async Task<BrokerState> GetState(StateRequest request, ServerCallContext context)
    {
        int limit = request.Limit <= 0 ? DefaultStateLimit : Math.Min(request.Limit, MaxStateLimit);
        BrokerSnapshot snapshot = await state.ReadSnapshotAsync(limit, context.CancellationToken);
        return StateMapper.ToProto(snapshot, options.StorageName, registry.OnlineSubscriberIds);
    }

    private static void EnsureValid(IReadOnlyList<string> errors)
    {
        if (errors.Count > 0)
            throw new RpcException(new Status(StatusCode.InvalidArgument, string.Join(",", errors)));
    }
}
