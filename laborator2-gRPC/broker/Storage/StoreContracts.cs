using System.Globalization;

namespace Broker.Storage;

public static class Statuses
{
    public const string Pending = "PENDING";
    public const string Delivered = "DELIVERED";
    public const string Dead = "DEAD";

    public static string ForMessage(int total, int pending, int delivered) =>
        total == 0 || pending > 0 ? Pending : delivered > 0 ? Delivered : Dead;
}

public static class Timestamps
{
    public static string Format(DateTimeOffset value) =>
        value.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
}

public sealed record StoredMessage(string MessageId, string Topic, string Content);

public sealed record PublishResult(bool Inserted, IReadOnlyList<string> Recipients);

public sealed record PendingDelivery(string MessageId, string SubscriberId, string Topic, string Content, int RetryCount);

public enum AckOutcome
{
    Acked,
    Duplicate,
    NotFound
}

public sealed record MessageRow(string MessageId, string Topic, string Content, string Status, string CreatedAtUtc);

public sealed record DeliveryRow(string MessageId, string SubscriberId, string Status, int RetryCount, string? LastError);

public sealed record DeadLetterRow(
    long Id,
    string? MessageId,
    string? SubscriberId,
    string? Topic,
    string? RawPayload,
    string Reason,
    int RetryCount,
    string CreatedAtUtc);

public sealed record SubscriptionRow(string SubscriberId, string Topic);

public sealed record BrokerSnapshot(
    IReadOnlyList<MessageRow> Messages,
    IReadOnlyList<DeliveryRow> Deliveries,
    IReadOnlyList<DeadLetterRow> DeadLetters,
    IReadOnlyList<SubscriptionRow> Subscriptions);

public interface IMessageStore
{
    Task<PublishResult> AddMessageAsync(StoredMessage message, DateTimeOffset createdAt, CancellationToken cancellationToken = default);
}

public interface ISubscriptionStore
{
    Task AddSubscriptionAsync(string subscriberId, string topic, DateTimeOffset createdAt, CancellationToken cancellationToken = default);
    Task<bool> RemoveSubscriptionAsync(string subscriberId, string topic, CancellationToken cancellationToken = default);
}

public interface IDeliveryStore
{
    Task<IReadOnlyList<PendingDelivery>> GetDueDeliveriesAsync(string subscriberId, DateTimeOffset attemptedAtOrBefore, CancellationToken cancellationToken = default);
    Task RecordAttemptAsync(string messageId, string subscriberId, DateTimeOffset attemptedAt, CancellationToken cancellationToken = default);
    Task<AckOutcome> AcknowledgeAsync(string messageId, string subscriberId, CancellationToken cancellationToken = default);
}

public interface IDeadLetterStore
{
    Task<bool> MoveToDeadLetterAsync(PendingDelivery delivery, string reason, DateTimeOffset at, CancellationToken cancellationToken = default);
    Task AddInvalidPayloadAsync(string rawPayload, string reason, DateTimeOffset at, CancellationToken cancellationToken = default);
}

public interface IStateReader
{
    Task<BrokerSnapshot> ReadSnapshotAsync(int limit, CancellationToken cancellationToken = default);
}

public interface IBrokerStore : IMessageStore, ISubscriptionStore, IDeliveryStore, IDeadLetterStore, IStateReader
{
    Task InitializeAsync(CancellationToken cancellationToken = default);
}
