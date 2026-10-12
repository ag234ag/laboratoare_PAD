using Microsoft.Data.Sqlite;

namespace Broker.Storage;

public sealed class SqliteStore : IBrokerStore
{
    private const string Schema = """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS Messages
        (
            MessageId TEXT PRIMARY KEY,
            Topic TEXT NOT NULL,
            Content TEXT NOT NULL,
            Status TEXT NOT NULL DEFAULT 'PENDING',
            CreatedAt TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS Subscriptions
        (
            SubscriberId TEXT NOT NULL,
            Topic TEXT NOT NULL,
            CreatedAt TEXT NOT NULL,
            PRIMARY KEY (SubscriberId, Topic)
        );
        CREATE TABLE IF NOT EXISTS Deliveries
        (
            MessageId TEXT NOT NULL,
            SubscriberId TEXT NOT NULL,
            Status TEXT NOT NULL DEFAULT 'PENDING',
            RetryCount INTEGER NOT NULL DEFAULT 0,
            LastAttemptAt TEXT,
            LastError TEXT,
            PRIMARY KEY (MessageId, SubscriberId)
        );
        CREATE TABLE IF NOT EXISTS DeadLetters
        (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            MessageId TEXT,
            SubscriberId TEXT,
            Topic TEXT,
            RawPayload TEXT,
            Reason TEXT NOT NULL,
            RetryCount INTEGER NOT NULL DEFAULT 0,
            CreatedAt TEXT NOT NULL
        );
        """;

    private const string DerivedMessageStatus = """
        (SELECT CASE
             WHEN COUNT(*) = 0 THEN 'PENDING'
             WHEN SUM(CASE WHEN d.Status = 'PENDING' THEN 1 ELSE 0 END) > 0 THEN 'PENDING'
             WHEN SUM(CASE WHEN d.Status = 'DELIVERED' THEN 1 ELSE 0 END) > 0 THEN 'DELIVERED'
             ELSE 'DEAD'
         END
         FROM Deliveries d
         WHERE d.MessageId = Messages.MessageId)
        """;

    private readonly string _connectionString;

    public SqliteStore(string databasePath)
    {
        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            DefaultTimeout = 5,
            Pooling = true
        }.ToString();
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await ExecuteAsync(connection, null, Schema, cancellationToken);
    }

    public async Task<PublishResult> AddMessageAsync(StoredMessage message, DateTimeOffset createdAt, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await using SqliteTransaction transaction = connection.BeginTransaction();

        int inserted = await ExecuteAsync(connection, transaction, """
            INSERT OR IGNORE INTO Messages (MessageId, Topic, Content, Status, CreatedAt)
            VALUES ($messageId, $topic, $content, 'PENDING', $createdAt);
            """, cancellationToken,
            ("$messageId", message.MessageId), ("$topic", message.Topic),
            ("$content", message.Content), ("$createdAt", Timestamps.Format(createdAt)));
        if (inserted == 0)
            return new PublishResult(false, []);

        await ExecuteAsync(connection, transaction, """
            INSERT OR IGNORE INTO Deliveries (MessageId, SubscriberId, Status, RetryCount)
            SELECT $messageId, SubscriberId, 'PENDING', 0
            FROM Subscriptions
            WHERE Topic = $topic;
            """, cancellationToken, ("$messageId", message.MessageId), ("$topic", message.Topic));

        List<string> recipients = await QueryAsync(connection, transaction,
            "SELECT SubscriberId FROM Deliveries WHERE MessageId = $messageId ORDER BY SubscriberId;",
            reader => reader.GetString(0), cancellationToken, ("$messageId", message.MessageId));

        await transaction.CommitAsync(cancellationToken);
        return new PublishResult(true, recipients);
    }

    public async Task AddSubscriptionAsync(string subscriberId, string topic, DateTimeOffset createdAt, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await using SqliteTransaction transaction = connection.BeginTransaction();

        await ExecuteAsync(connection, transaction, $"""
            INSERT OR IGNORE INTO Subscriptions (SubscriberId, Topic, CreatedAt)
            VALUES ($subscriberId, $topic, $createdAt);

            INSERT OR IGNORE INTO Deliveries (MessageId, SubscriberId, Status, RetryCount)
            SELECT MessageId, $subscriberId, 'PENDING', 0
            FROM Messages
            WHERE Topic = $topic;

            UPDATE Messages
            SET Status = {DerivedMessageStatus}
            WHERE Topic = $topic;
            """, cancellationToken,
            ("$subscriberId", subscriberId), ("$topic", topic), ("$createdAt", Timestamps.Format(createdAt)));

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<bool> RemoveSubscriptionAsync(string subscriberId, string topic, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        int removed = await ExecuteAsync(connection, null,
            "DELETE FROM Subscriptions WHERE SubscriberId = $subscriberId AND Topic = $topic;",
            cancellationToken, ("$subscriberId", subscriberId), ("$topic", topic));
        return removed == 1;
    }

    public async Task<IReadOnlyList<PendingDelivery>> GetDueDeliveriesAsync(string subscriberId, DateTimeOffset attemptedAtOrBefore, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        return await QueryAsync(connection, null, """
            SELECT d.MessageId, d.SubscriberId, m.Topic, m.Content, d.RetryCount
            FROM Deliveries d
            INNER JOIN Messages m ON m.MessageId = d.MessageId
            INNER JOIN Subscriptions s ON s.SubscriberId = d.SubscriberId AND s.Topic = m.Topic
            WHERE d.SubscriberId = $subscriberId
              AND d.Status = 'PENDING'
              AND (d.LastAttemptAt IS NULL OR d.LastAttemptAt <= $cutoff)
            ORDER BY m.CreatedAt, m.rowid;
            """,
            reader => new PendingDelivery(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt32(4)),
            cancellationToken, ("$subscriberId", subscriberId), ("$cutoff", Timestamps.Format(attemptedAtOrBefore)));
    }

    public async Task RecordAttemptAsync(string messageId, string subscriberId, DateTimeOffset attemptedAt, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await ExecuteAsync(connection, null, """
            UPDATE Deliveries
            SET RetryCount = RetryCount + 1, LastAttemptAt = $attemptedAt, LastError = NULL
            WHERE MessageId = $messageId AND SubscriberId = $subscriberId AND Status = 'PENDING';
            """, cancellationToken,
            ("$messageId", messageId), ("$subscriberId", subscriberId), ("$attemptedAt", Timestamps.Format(attemptedAt)));
    }

    public async Task<AckOutcome> AcknowledgeAsync(string messageId, string subscriberId, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await using SqliteTransaction transaction = connection.BeginTransaction();

        int updated = await ExecuteAsync(connection, transaction, """
            UPDATE Deliveries
            SET Status = 'DELIVERED', LastError = NULL
            WHERE MessageId = $messageId AND SubscriberId = $subscriberId AND Status = 'PENDING';
            """, cancellationToken, ("$messageId", messageId), ("$subscriberId", subscriberId));

        if (updated == 1)
        {
            await RefreshMessageStatusAsync(connection, transaction, messageId, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return AckOutcome.Acked;
        }

        List<string> statuses = await QueryAsync(connection, transaction,
            "SELECT Status FROM Deliveries WHERE MessageId = $messageId AND SubscriberId = $subscriberId;",
            reader => reader.GetString(0), cancellationToken, ("$messageId", messageId), ("$subscriberId", subscriberId));
        return statuses.SingleOrDefault() == Statuses.Delivered ? AckOutcome.Duplicate : AckOutcome.NotFound;
    }

    public async Task<bool> MoveToDeadLetterAsync(PendingDelivery delivery, string reason, DateTimeOffset at, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await using SqliteTransaction transaction = connection.BeginTransaction();

        int updated = await ExecuteAsync(connection, transaction, """
            UPDATE Deliveries
            SET Status = 'DEAD', LastError = $reason
            WHERE MessageId = $messageId AND SubscriberId = $subscriberId
              AND Status = 'PENDING' AND RetryCount = $retryCount;
            """, cancellationToken,
            ("$reason", reason), ("$messageId", delivery.MessageId),
            ("$subscriberId", delivery.SubscriberId), ("$retryCount", delivery.RetryCount));
        if (updated == 0)
            return false;

        await ExecuteAsync(connection, transaction, """
            INSERT INTO DeadLetters (MessageId, SubscriberId, Topic, RawPayload, Reason, RetryCount, CreatedAt)
            VALUES ($messageId, $subscriberId, $topic, $payload, $reason, $retryCount, $createdAt);
            """, cancellationToken,
            ("$messageId", delivery.MessageId), ("$subscriberId", delivery.SubscriberId), ("$topic", delivery.Topic),
            ("$payload", delivery.Content), ("$reason", reason), ("$retryCount", delivery.RetryCount),
            ("$createdAt", Timestamps.Format(at)));
        await RefreshMessageStatusAsync(connection, transaction, delivery.MessageId, cancellationToken);

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    public async Task AddInvalidPayloadAsync(string rawPayload, string reason, DateTimeOffset at, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        await ExecuteAsync(connection, null, """
            INSERT INTO DeadLetters (RawPayload, Reason, RetryCount, CreatedAt)
            VALUES ($payload, $reason, 0, $createdAt);
            """, cancellationToken, ("$payload", rawPayload), ("$reason", reason), ("$createdAt", Timestamps.Format(at)));
    }

    public async Task<BrokerSnapshot> ReadSnapshotAsync(int limit, CancellationToken cancellationToken = default)
    {
        await using SqliteConnection connection = await OpenAsync(cancellationToken);
        (string, object?) limitParameter = ("$limit", limit);

        List<MessageRow> messages = await QueryAsync(connection, null,
            "SELECT MessageId, Topic, Content, Status, CreatedAt FROM Messages ORDER BY CreatedAt DESC, rowid DESC LIMIT $limit;",
            r => new MessageRow(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetString(4)),
            cancellationToken, limitParameter);
        List<DeliveryRow> deliveries = await QueryAsync(connection, null,
            "SELECT MessageId, SubscriberId, Status, RetryCount, LastError FROM Deliveries ORDER BY rowid DESC LIMIT $limit;",
            r => new DeliveryRow(r.GetString(0), r.GetString(1), r.GetString(2), r.GetInt32(3), NullableString(r, 4)),
            cancellationToken, limitParameter);
        List<DeadLetterRow> deadLetters = await QueryAsync(connection, null,
            "SELECT Id, MessageId, SubscriberId, Topic, RawPayload, Reason, RetryCount, CreatedAt FROM DeadLetters ORDER BY Id DESC LIMIT $limit;",
            r => new DeadLetterRow(r.GetInt64(0), NullableString(r, 1), NullableString(r, 2), NullableString(r, 3),
                NullableString(r, 4), r.GetString(5), r.GetInt32(6), r.GetString(7)),
            cancellationToken, limitParameter);
        List<SubscriptionRow> subscriptions = await QueryAsync(connection, null,
            "SELECT SubscriberId, Topic FROM Subscriptions ORDER BY CreatedAt DESC, rowid DESC LIMIT $limit;",
            r => new SubscriptionRow(r.GetString(0), r.GetString(1)),
            cancellationToken, limitParameter);

        return new BrokerSnapshot(messages, deliveries, deadLetters, subscriptions);
    }

    private static Task RefreshMessageStatusAsync(SqliteConnection connection, SqliteTransaction transaction, string messageId, CancellationToken cancellationToken) =>
        ExecuteAsync(connection, transaction,
            $"UPDATE Messages SET Status = {DerivedMessageStatus} WHERE MessageId = $messageId;",
            cancellationToken, ("$messageId", messageId));

    private async Task<SqliteConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static SqliteCommand CreateCommand(SqliteConnection connection, SqliteTransaction? transaction, string sql, (string Name, object? Value)[] parameters)
    {
        SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        foreach ((string name, object? value) in parameters)
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        return command;
    }

    private static async Task<int> ExecuteAsync(SqliteConnection connection, SqliteTransaction? transaction, string sql, CancellationToken cancellationToken, params (string Name, object? Value)[] parameters)
    {
        await using SqliteCommand command = CreateCommand(connection, transaction, sql, parameters);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<List<T>> QueryAsync<T>(SqliteConnection connection, SqliteTransaction? transaction, string sql, Func<SqliteDataReader, T> map, CancellationToken cancellationToken, params (string Name, object? Value)[] parameters)
    {
        await using SqliteCommand command = CreateCommand(connection, transaction, sql, parameters);
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<T>();
        while (await reader.ReadAsync(cancellationToken))
            rows.Add(map(reader));
        return rows;
    }

    private static string? NullableString(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
}
