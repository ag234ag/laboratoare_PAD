using System.Collections.Concurrent;

namespace Broker.Storage;

public sealed class InMemoryStore : IBrokerStore
{
    private readonly object _gate = new();
    private readonly ConcurrentDictionary<string, MessageEntry> _messages = new();
    private readonly ConcurrentDictionary<(string SubscriberId, string Topic), long> _subscriptions = new();
    private readonly ConcurrentDictionary<(string MessageId, string SubscriberId), DeliveryEntry> _deliveries = new();
    private readonly ConcurrentQueue<DeadLetterRow> _deadLetters = new();
    private long _sequence;
    private long _deadLetterId;

    public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task<PublishResult> AddMessageAsync(StoredMessage message, DateTimeOffset createdAt, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_messages.TryAdd(message.MessageId, new MessageEntry(message, Timestamps.Format(createdAt), ++_sequence)))
                return Task.FromResult(new PublishResult(false, []));

            List<string> recipients = _subscriptions.Keys
                .Where(key => key.Topic == message.Topic)
                .Select(key => key.SubscriberId)
                .ToList();
            foreach (string subscriberId in recipients)
                AddDelivery(message.MessageId, subscriberId);
            return Task.FromResult(new PublishResult(true, recipients));
        }
    }

    public Task AddSubscriptionAsync(string subscriberId, string topic, DateTimeOffset createdAt, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            _subscriptions.TryAdd((subscriberId, topic), ++_sequence);
            foreach (MessageEntry message in _messages.Values.Where(m => m.Topic == topic))
            {
                AddDelivery(message.MessageId, subscriberId);
                RefreshStatus(message);
            }
        }
        return Task.CompletedTask;
    }

    public Task<bool> RemoveSubscriptionAsync(string subscriberId, string topic, CancellationToken cancellationToken = default)
    {
        lock (_gate)
            return Task.FromResult(_subscriptions.TryRemove((subscriberId, topic), out _));
    }

    public Task<IReadOnlyList<PendingDelivery>> GetDueDeliveriesAsync(string subscriberId, DateTimeOffset attemptedAtOrBefore, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            List<PendingDelivery> due = _deliveries.Values
                .Where(d => d.SubscriberId == subscriberId
                    && d.Status == Statuses.Pending
                    && (d.LastAttemptAt is null || d.LastAttemptAt <= attemptedAtOrBefore))
                .Select(d => (Delivery: d, Message: _messages[d.MessageId]))
                .Where(x => _subscriptions.ContainsKey((subscriberId, x.Message.Topic)))
                .OrderBy(x => x.Message.Sequence)
                .Select(x => new PendingDelivery(x.Message.MessageId, subscriberId, x.Message.Topic, x.Message.Content, x.Delivery.RetryCount))
                .ToList();
            return Task.FromResult<IReadOnlyList<PendingDelivery>>(due);
        }
    }

    public Task RecordAttemptAsync(string messageId, string subscriberId, DateTimeOffset attemptedAt, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_deliveries.TryGetValue((messageId, subscriberId), out DeliveryEntry? delivery) && delivery.Status == Statuses.Pending)
            {
                delivery.RetryCount++;
                delivery.LastAttemptAt = attemptedAt;
            }
        }
        return Task.CompletedTask;
    }

    public Task<AckOutcome> AcknowledgeAsync(string messageId, string subscriberId, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_deliveries.TryGetValue((messageId, subscriberId), out DeliveryEntry? delivery))
                return Task.FromResult(AckOutcome.NotFound);
            if (delivery.Status == Statuses.Delivered)
                return Task.FromResult(AckOutcome.Duplicate);
            if (delivery.Status != Statuses.Pending)
                return Task.FromResult(AckOutcome.NotFound);

            delivery.Status = Statuses.Delivered;
            delivery.LastError = null;
            RefreshStatus(_messages[messageId]);
            return Task.FromResult(AckOutcome.Acked);
        }
    }

    public Task<bool> MoveToDeadLetterAsync(PendingDelivery delivery, string reason, DateTimeOffset at, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_deliveries.TryGetValue((delivery.MessageId, delivery.SubscriberId), out DeliveryEntry? entry)
                || entry.Status != Statuses.Pending
                || entry.RetryCount != delivery.RetryCount)
                return Task.FromResult(false);

            entry.Status = Statuses.Dead;
            entry.LastError = reason;
            _deadLetters.Enqueue(new DeadLetterRow(
                ++_deadLetterId, delivery.MessageId, delivery.SubscriberId, delivery.Topic,
                delivery.Content, reason, delivery.RetryCount, Timestamps.Format(at)));
            RefreshStatus(_messages[delivery.MessageId]);
            return Task.FromResult(true);
        }
    }

    public Task AddInvalidPayloadAsync(string rawPayload, string reason, DateTimeOffset at, CancellationToken cancellationToken = default)
    {
        lock (_gate)
            _deadLetters.Enqueue(new DeadLetterRow(++_deadLetterId, null, null, null, rawPayload, reason, 0, Timestamps.Format(at)));
        return Task.CompletedTask;
    }

    public Task<BrokerSnapshot> ReadSnapshotAsync(int limit, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var snapshot = new BrokerSnapshot(
                _messages.Values.OrderByDescending(m => m.Sequence).Take(limit)
                    .Select(m => new MessageRow(m.MessageId, m.Topic, m.Content, m.Status, m.CreatedAtUtc)).ToList(),
                _deliveries.Values.OrderByDescending(d => d.Sequence).Take(limit)
                    .Select(d => new DeliveryRow(d.MessageId, d.SubscriberId, d.Status, d.RetryCount, d.LastError)).ToList(),
                _deadLetters.OrderByDescending(d => d.Id).Take(limit).ToList(),
                _subscriptions.OrderByDescending(s => s.Value).Take(limit)
                    .Select(s => new SubscriptionRow(s.Key.SubscriberId, s.Key.Topic)).ToList());
            return Task.FromResult(snapshot);
        }
    }

    private void AddDelivery(string messageId, string subscriberId)
    {
        if (!_deliveries.ContainsKey((messageId, subscriberId)))
            _deliveries[(messageId, subscriberId)] = new DeliveryEntry(messageId, subscriberId, ++_sequence);
    }

    private void RefreshStatus(MessageEntry message)
    {
        List<DeliveryEntry> deliveries = _deliveries.Values.Where(d => d.MessageId == message.MessageId).ToList();
        message.Status = Statuses.ForMessage(
            deliveries.Count,
            deliveries.Count(d => d.Status == Statuses.Pending),
            deliveries.Count(d => d.Status == Statuses.Delivered));
    }

    private sealed class MessageEntry(StoredMessage message, string createdAtUtc, long sequence)
    {
        public string MessageId { get; } = message.MessageId;
        public string Topic { get; } = message.Topic;
        public string Content { get; } = message.Content;
        public string CreatedAtUtc { get; } = createdAtUtc;
        public long Sequence { get; } = sequence;
        public string Status { get; set; } = Statuses.Pending;
    }

    private sealed class DeliveryEntry(string messageId, string subscriberId, long sequence)
    {
        public string MessageId { get; } = messageId;
        public string SubscriberId { get; } = subscriberId;
        public long Sequence { get; } = sequence;
        public string Status { get; set; } = Statuses.Pending;
        public int RetryCount { get; set; }
        public DateTimeOffset? LastAttemptAt { get; set; }
        public string? LastError { get; set; }
    }
}
