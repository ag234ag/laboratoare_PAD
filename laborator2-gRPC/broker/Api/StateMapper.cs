using Broker.Contracts;
using Broker.Storage;

namespace Broker.Api;

public static class StateMapper
{
    public static BrokerState ToProto(BrokerSnapshot snapshot, string storageMode, IEnumerable<string> onlineSubscribers)
    {
        var state = new BrokerState { StorageMode = storageMode };
        state.Messages.AddRange(snapshot.Messages.Select(m => new MessageRecord
        {
            MessageId = m.MessageId,
            Topic = m.Topic,
            Content = m.Content,
            Status = m.Status,
            CreatedAtUtc = m.CreatedAtUtc
        }));
        state.Deliveries.AddRange(snapshot.Deliveries.Select(d => new DeliveryRecord
        {
            MessageId = d.MessageId,
            SubscriberId = d.SubscriberId,
            Status = d.Status,
            RetryCount = d.RetryCount,
            LastError = d.LastError ?? ""
        }));
        state.DeadLetters.AddRange(snapshot.DeadLetters.Select(d => new DeadLetterRecord
        {
            Id = d.Id,
            MessageId = d.MessageId ?? "",
            SubscriberId = d.SubscriberId ?? "",
            Topic = d.Topic ?? "",
            RawPayload = d.RawPayload ?? "",
            Reason = d.Reason,
            RetryCount = d.RetryCount,
            CreatedAtUtc = d.CreatedAtUtc
        }));
        state.Subscriptions.AddRange(snapshot.Subscriptions.Select(s => new SubscriptionRecord
        {
            SubscriberId = s.SubscriberId,
            Topic = s.Topic
        }));
        state.OnlineSubscribers.AddRange(onlineSubscribers.Order());
        return state;
    }
}
