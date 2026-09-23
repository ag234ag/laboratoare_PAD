using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.Data.Sqlite;
class Program
{
    private const int PORT = 5000;
    private const int MAX_RETRIES = 3;
    private const int RETRY_INTERVAL_SECONDS = 5;
    private const int ACK_TIMEOUT_SECONDS = 5;
    private const string CONNECTION_STRING =
        "Data Source=broker.db";
    
    private static readonly ConcurrentDictionary<string, SubscriberConnection>
        ConnectedSubscribers = new();
    
    private static readonly ConcurrentDictionary<string, SemaphoreSlim>
        DeliveryLocks = new();

    private static readonly JsonSerializerOptions
        SerializerOptions = new()
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };
    static async Task Main()
    {
        Console.OutputEncoding = Encoding.UTF8;
        Console.WriteLine(" ------ MESSAGE BROKER - TCP / JSON ------     ");
        await InitializeDatabaseAsync();
        Console.WriteLine("The SQLite database is ready");
        TcpListener listener =
            new TcpListener(IPAddress.Loopback, PORT);
        listener.Start();
        Console.WriteLine(
            $"Broker started on 127.0.0.1:{PORT}"
        );
        Console.WriteLine("Waiting for connections...");
        Console.WriteLine();
       
        _ = Task.Run(RetryWorkerAsync);
        while (true)
        {
            TcpClient client =
                await listener.AcceptTcpClientAsync();
            Console.WriteLine(
                $"Client connected: {client.Client.RemoteEndPoint}"
            );
            // Each client is processed concurrently.
            _ = Task.Run(() => HandleClientAsync(client));
        }
    }
    // =========================================================
    // CLIENT CONNECTION
    // =========================================================
    private static async Task HandleClientAsync(
        TcpClient client)
    {
        SubscriberConnection? connection = null;
        try
        {
            NetworkStream stream =
                client.GetStream();
            connection =
                new SubscriberConnection(
                    client,
                    stream
                );
            while (client.Connected)
            {
                string? line =
                    await connection.Reader.ReadLineAsync();
                if (line == null)
                    break;
                if (string.IsNullOrWhiteSpace(line))
                    continue;
                Console.WriteLine();
                Console.WriteLine(
                    $"[RECEIVED] {line}"
                );
                await ProcessMessageAsync(
                    connection,
                    line
                );
            }
        }
        catch (IOException)
        {
            Console.WriteLine(
                "The client disconnected."
            );
        }
        catch (Exception ex)
        {
            Console.WriteLine(
                $"Client error: {ex.Message}"
            );
        }
        finally
        {
            if (connection?.SubscriberId != null)
            {
                string subscriberId =
                    connection.SubscriberId;
                if (ConnectedSubscribers.TryGetValue(
                    subscriberId,
                    out SubscriberConnection? existing
                ))
                {
                    if (ReferenceEquals(
                        existing,
                        connection
                    ))
                    {
                        ConnectedSubscribers.TryRemove(
                            subscriberId,
                            out _
                        );
                    }
                }
                Console.WriteLine(
                    $"Subscriber offline: {subscriberId}"
                );
            }
            try
            {
                client.Close();
            }
            catch
            {
            }
        }
    }
    // =========================================================
    // PROCESS JSON
    // =========================================================
    private static async Task ProcessMessageAsync(
        SubscriberConnection connection,
        string rawMessage)
    {
        JsonDocument document;
        try
        {
            document =
                JsonDocument.Parse(rawMessage);
        }
        catch (JsonException)
        {
            Console.WriteLine(
                "[INVALID JSON]"
            );
            await SaveRawDeadLetterAsync(
                rawMessage,
                "INVALID_JSON"
            );
            await SendJsonAsync(
                connection,
                new
                {
                    action = "error",
                    reason = "invalid_json",
                    message = "JSON invalid."
                }
            );
            return;
        }
        using (document)
        {
            JsonElement root =
                document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                Console.WriteLine(
                    "[INVALID MESSAGE FORMAT]"
                );
                await SaveRawDeadLetterAsync(
                    rawMessage,
                    "NOT_A_JSON_OBJECT"
                );
                await SendJsonAsync(
                    connection,
                    new
                    {
                        action = "error",
                        reason = "json_object_required"
                    }
                );
                return;
            }
            if (!root.TryGetProperty(
                "action",
                out JsonElement actionElement) ||
                actionElement.ValueKind != JsonValueKind.String)
            {
                await SaveRawDeadLetterAsync(
                    rawMessage,
                    "ACTION_MISSING"
                );
                await SendJsonAsync(
                    connection,
                    new
                    {
                        action = "error",
                        reason = "action_missing"
                    }
                );
                return;
            }
            string? action =
                actionElement.GetString();
            switch (action)
            {
                case "publish":
                    await HandlePublishAsync(
                        connection,
                        root,
                        rawMessage
                    );
                    break;
                case "subscribe":
                    await HandleSubscribeAsync(
                        connection,
                        root
                    );
                    break;
                case "unsubscribe":
                    await HandleUnsubscribeAsync(
                        connection,
                        root
                    );
                    break;
                case "ack":
                    await HandleAckAsync(
                        connection,
                        root
                    );
                    break;
                case "ping":
                    await SendJsonAsync(
                        connection,
                        new
                        {
                            action = "pong"
                        }
                    );
                    break;
                default:
                    await SaveRawDeadLetterAsync(
                        rawMessage,
                        "UNKNOWN_ACTION"
                    );
                    await SendJsonAsync(
                        connection,
                        new
                        {
                            action = "error",
                            reason = "unknown_action"
                        }
                    );
                    break;
            }
        }
    }
    // =========================================================
    // PUBLISH
    // =========================================================
    private static async Task HandlePublishAsync(
        SubscriberConnection publisher,
        JsonElement root,
        string rawMessage)
    {
        string? messageId =
            GetString(root, "messageId");
        string? topic =
            GetString(root, "topic");
        string? content =
            GetString(root, "content");
        if (string.IsNullOrWhiteSpace(messageId) ||
            string.IsNullOrWhiteSpace(topic) ||
            string.IsNullOrWhiteSpace(content))
        {
            Console.WriteLine(
                "[INVALID PUBLISH MESSAGE]"
            );
            await SaveRawDeadLetterAsync(
                rawMessage,
                "INVALID_PUBLISH_MESSAGE"
            );
            await SendJsonAsync(
                publisher,
                new
                {
                    action = "error",
                    reason =
                        "messageId_topic_content_required"
                }
            );
            return;
        }
        bool inserted =
            await SaveMessageAsync(
                messageId,
                topic,
                content
            );
        if (!inserted)
        {
            await SendJsonAsync(
                publisher,
                new
                {
                    action = "error",
                    reason =
                        "duplicate_message_id",
                    messageId
                }
            );
            return;
        }
        Console.WriteLine(
            $"[PERSISTED] {messageId}"
        );
        Console.WriteLine(
            $"Topic: {topic}"
        );
        // Create deliveries for subscribers
        // already subscribed to this topic.
        await CreateDeliveriesForTopicAsync(
            messageId,
            topic
        );
        int subscriptions =
            await CountSubscriptionsAsync(topic);
        await SendJsonAsync(
            publisher,
            new
            {
                action = "publish_accepted",
                messageId,
                topic,
                subscribers = subscriptions
            }
        );
        if (subscriptions == 0)
        {
            Console.WriteLine(
                $"[PENDING] Message {messageId} " +
                $"does not have any subscribers yet."
            );
            // IMPORTANT:
            // The message remains persisted in the database.
            return;
        }
        // Deliver immediately to online subscribers.
        foreach (var entry in ConnectedSubscribers)
        {
            SubscriberConnection subscriber =
                entry.Value;
            if (subscriber.Topics.ContainsKey(topic))
            {
                _ = DeliverMessageAsync(
                    entry.Key,
                    messageId
                );
            }
        }
    }
    // =========================================================
    // SUBSCRIBE
    // =========================================================
    private static async Task HandleSubscribeAsync(
        SubscriberConnection connection,
        JsonElement root)
    {
        string? subscriberId =
            GetString(root, "subscriberId");
        string? topic =
            GetString(root, "topic");
        if (string.IsNullOrWhiteSpace(subscriberId) ||
            string.IsNullOrWhiteSpace(topic))
        {
            await SendJsonAsync(
                connection,
                new
                {
                    action = "error",
                    reason =
                        "subscriberId_and_topic_required"
                }
            );
            return;
        }
        connection.SubscriberId =
            subscriberId;
        connection.Topics.TryAdd(
            topic,
            0
        );
        ConnectedSubscribers[
            subscriberId
        ] = connection;
        await SaveSubscriptionAsync(
            subscriberId,
            topic
        );
        // Create deliveries also for messages
        // published before the subscription.
        await CreateBacklogDeliveriesAsync(
            subscriberId,
            topic
        );
        Console.WriteLine(
            $"[SUBSCRIBE] {subscriberId} -> {topic}"
        );
        await SendJsonAsync(
            connection,
            new
            {
                action = "subscribed",
                subscriberId,
                topic
            }
        );
        // Deliver persisted / PENDING messages.
        _ = DeliverPendingMessagesAsync(
            subscriberId,
            topic
        );
    }
    // =========================================================
    // UNSUBSCRIBE
    // =========================================================
    private static async Task HandleUnsubscribeAsync(
        SubscriberConnection connection,
        JsonElement root)
    {
        string? topic =
            GetString(root, "topic");
        if (connection.SubscriberId == null ||
            string.IsNullOrWhiteSpace(topic))
        {
            await SendJsonAsync(
                connection,
                new
                {
                    action = "error",
                    reason =
                        "subscriber_not_registered"
                }
            );
            return;
        }
        connection.Topics.TryRemove(
            topic,
            out _
        );
        await DeleteSubscriptionAsync(
            connection.SubscriberId,
            topic
        );
        Console.WriteLine(
            $"[UNSUBSCRIBE] " +
            $"{connection.SubscriberId} -> {topic}"
        );
        await SendJsonAsync(
            connection,
            new
            {
                action = "unsubscribed",
                subscriberId =
                    connection.SubscriberId,
                topic
            }
        );
    }
    // =========================================================
    // ACK
    // =========================================================
    private static async Task HandleAckAsync(
        SubscriberConnection connection,
        JsonElement root)
    {
        if (connection.SubscriberId == null)
        {
            await SendJsonAsync(
                connection,
                new
                {
                    action = "error",
                    reason =
                        "subscriber_not_registered"
                }
            );
            return;
        }
        string? messageId =
            GetString(root, "messageId");
        if (string.IsNullOrWhiteSpace(messageId))
        {
            await SendJsonAsync(
                connection,
                new
                {
                    action = "error",
                    reason =
                        "messageId_required"
                }
            );
            return;
        }
        bool updated =
            await MarkDeliveryAsDeliveredAsync(
                messageId,
                connection.SubscriberId
            );
        if (!updated)
        {
            if (await IsDeliveryDeliveredAsync(
                messageId,
                connection.SubscriberId))
            {
                Console.WriteLine(
                    $"[DUPLICATE ACK] {messageId} from " +
                    connection.SubscriberId
                );
                await SendJsonAsync(
                    connection,
                    new
                    {
                        action = "ack_received",
                        messageId,
                        duplicate = true
                    }
                );
                return;
            }
            await SendJsonAsync(
                connection,
                new
                {
                    action = "error",
                    reason =
                        "delivery_not_found",
                    messageId
                }
            );
            return;
        }
        Console.WriteLine(
            $"[ACK] {messageId} confirmed by " +
            connection.SubscriberId
        );
        await UpdateMessageStatusAsync(
            messageId
        );
        await SendJsonAsync(
            connection,
            new
            {
                action = "ack_received",
                messageId
            }
        );
    }
    // =========================================================
    // DELIVERY
    // =========================================================
    private static async Task DeliverMessageAsync(
        string subscriberId,
        string messageId)
    {
        string lockKey =
            $"{subscriberId}:{messageId}";
        SemaphoreSlim deliveryLock =
            DeliveryLocks.GetOrAdd(
                lockKey,
                _ => new SemaphoreSlim(1, 1)
            );
        await deliveryLock.WaitAsync();
        try
        {
            if (!ConnectedSubscribers.TryGetValue(
                subscriberId,
                out SubscriberConnection? subscriber))
            {
                // Subscriber is offline.
                // Do not consume a retry attempt.
                return;
            }
            PendingDelivery? delivery =
                await GetPendingDeliveryAsync(
                    subscriberId,
                    messageId
                );
            if (delivery == null)
                return;
            if (delivery.RetryCount >= MAX_RETRIES)
            {
                await MoveToDeadLetterAsync(
                    delivery,
                    "ACK_NOT_RECEIVED_AFTER_MAX_RETRIES"
                );
                return;
            }
            try
            {
                int attempt =
                    delivery.RetryCount + 1;
                await SendJsonAsync(
                    subscriber,
                    new
                    {
                        action = "message",
                        messageId =
                            delivery.MessageId,
                        topic =
                            delivery.Topic,
                        content =
                            delivery.Content,
                        attempt
                    }
                );
                await IncrementRetryAsync(
                    delivery.MessageId,
                    subscriberId,
                    null
                );
                Console.WriteLine(
                    $"[DELIVERY] {delivery.MessageId} " +
                    $"-> {subscriberId} " +
                    $"(attempt {attempt}/{MAX_RETRIES})"
                );
            }
            catch (Exception ex)
            {
                await IncrementRetryAsync(
                    delivery.MessageId,
                    subscriberId,
                    ex.Message
                );
                Console.WriteLine(
                    $"[DELIVERY ERROR] {ex.Message}"
                );
            }
        }
        finally
        {
            deliveryLock.Release();
        }
    }
    private static async Task DeliverPendingMessagesAsync(
        string subscriberId,
        string topic)
    {
        List<string> messageIds =
            await GetPendingMessageIdsAsync(
                subscriberId,
                topic
            );
        foreach (string messageId in messageIds)
        {
            await DeliverMessageAsync(
                subscriberId,
                messageId
            );
        }
    }
    // =========================================================
    // RETRY WORKER
    // =========================================================
    private static async Task RetryWorkerAsync()
    {
        while (true)
        {
            await Task.Delay(
                TimeSpan.FromSeconds(
                    RETRY_INTERVAL_SECONDS
                )
            );
            try
            {
                foreach (
                    KeyValuePair<string, SubscriberConnection>
                    entry in ConnectedSubscribers
                )
                {
                    string subscriberId =
                        entry.Key;
                    SubscriberConnection subscriber =
                        entry.Value;
                    foreach (string topic
                        in subscriber.Topics.Keys)
                    {
                        await DeliverPendingMessagesAsync(
                            subscriberId,
                            topic
                        );
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"[RETRY WORKER ERROR] {ex.Message}"
                );
            }
        }
    }
    // =========================================================
    // DATABASE INITIALIZATION
    // =========================================================
    private static async Task InitializeDatabaseAsync()
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        string sql = """
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
                PRIMARY KEY
                (
                    SubscriberId,
                    Topic
                )
            );
            CREATE TABLE IF NOT EXISTS Deliveries
            (
                MessageId TEXT NOT NULL,
                SubscriberId TEXT NOT NULL,
                Status TEXT NOT NULL DEFAULT 'PENDING',
                RetryCount INTEGER NOT NULL DEFAULT 0,
                LastAttemptAt TEXT,
                LastError TEXT,
                PRIMARY KEY
                (
                    MessageId,
                    SubscriberId
                )
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
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }
    // =========================================================
    // DATABASE - MESSAGES
    // =========================================================
    private static async Task<bool> SaveMessageAsync(
        string messageId,
        string topic,
        string content)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            INSERT OR IGNORE INTO Messages
            (
                MessageId,
                Topic,
                Content,
                Status,
                CreatedAt
            )
            VALUES
            (
                $messageId,
                $topic,
                $content,
                'PENDING',
                $createdAt
            );
            """;
        command.Parameters.AddWithValue(
            "$messageId",
            messageId
        );
        command.Parameters.AddWithValue(
            "$topic",
            topic
        );
        command.Parameters.AddWithValue(
            "$content",
            content
        );
        command.Parameters.AddWithValue(
            "$createdAt",
            DateTime.UtcNow.ToString("O")
        );
        int affected =
            await command.ExecuteNonQueryAsync();
        return affected == 1;
    }
    // =========================================================
    // DATABASE - SUBSCRIPTIONS
    // =========================================================
    private static async Task SaveSubscriptionAsync(
        string subscriberId,
        string topic)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            INSERT OR IGNORE INTO Subscriptions
            (
                SubscriberId,
                Topic,
                CreatedAt
            )
            VALUES
            (
                $subscriberId,
                $topic,
                $createdAt
            );
            """;
        command.Parameters.AddWithValue(
            "$subscriberId",
            subscriberId
        );
        command.Parameters.AddWithValue(
            "$topic",
            topic
        );
        command.Parameters.AddWithValue(
            "$createdAt",
            DateTime.UtcNow.ToString("O")
        );
        await command.ExecuteNonQueryAsync();
    }
    private static async Task DeleteSubscriptionAsync(
        string subscriberId,
        string topic)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            DELETE FROM Subscriptions
            WHERE SubscriberId = $subscriberId
              AND Topic = $topic;
            """;
        command.Parameters.AddWithValue(
            "$subscriberId",
            subscriberId
        );
        command.Parameters.AddWithValue(
            "$topic",
            topic
        );
        await command.ExecuteNonQueryAsync();
    }
    private static async Task<int> CountSubscriptionsAsync(
        string topic)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*)
            FROM Subscriptions
            WHERE Topic = $topic;
            """;
        command.Parameters.AddWithValue(
            "$topic",
            topic
        );
        object? result =
            await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }
    // =========================================================
    // CREATE DELIVERIES
    // =========================================================
    private static async Task CreateDeliveriesForTopicAsync(
        string messageId,
        string topic)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            INSERT OR IGNORE INTO Deliveries
            (
                MessageId,
                SubscriberId,
                Status,
                RetryCount
            )
            SELECT
                $messageId,
                SubscriberId,
                'PENDING',
                0
            FROM Subscriptions
            WHERE Topic = $topic;
            """;
        command.Parameters.AddWithValue(
            "$messageId",
            messageId
        );
        command.Parameters.AddWithValue(
            "$topic",
            topic
        );
        await command.ExecuteNonQueryAsync();
    }
    private static async Task CreateBacklogDeliveriesAsync(
        string subscriberId,
        string topic)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            INSERT OR IGNORE INTO Deliveries
            (
                MessageId,
                SubscriberId,
                Status,
                RetryCount
            )
            SELECT
                MessageId,
                $subscriberId,
                'PENDING',
                0
            FROM Messages
            WHERE Topic = $topic;
            """;
        command.Parameters.AddWithValue(
            "$subscriberId",
            subscriberId
        );
        command.Parameters.AddWithValue(
            "$topic",
            topic
        );
        await command.ExecuteNonQueryAsync();
    }
    // =========================================================
    // GET PENDING DELIVERY
    // =========================================================
    private static async Task<PendingDelivery?>
        GetPendingDeliveryAsync(
            string subscriberId,
            string messageId)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            SELECT
                d.MessageId,
                d.SubscriberId,
                d.RetryCount,
                m.Topic,
                m.Content
            FROM Deliveries d
            INNER JOIN Messages m
                ON m.MessageId = d.MessageId
            WHERE d.MessageId = $messageId
              AND d.SubscriberId = $subscriberId
              AND d.Status = 'PENDING'
              AND (d.LastAttemptAt IS NULL
                   OR d.LastAttemptAt <= $cutoff);
            """;
        command.Parameters.AddWithValue(
            "$messageId",
            messageId
        );
        command.Parameters.AddWithValue(
            "$subscriberId",
            subscriberId
        );
        command.Parameters.AddWithValue(
            "$cutoff",
            GetAckTimeoutCutoff()
        );
        await using SqliteDataReader reader =
            await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;
        return new PendingDelivery
        {
            MessageId =
                reader.GetString(0),
            SubscriberId =
                reader.GetString(1),
            RetryCount =
                reader.GetInt32(2),
            Topic =
                reader.GetString(3),
            Content =
                reader.GetString(4)
        };
    }
    private static async Task<List<string>>
        GetPendingMessageIdsAsync(
            string subscriberId,
            string topic)
    {
        List<string> result = new();
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            SELECT d.MessageId
            FROM Deliveries d
            INNER JOIN Messages m
                ON m.MessageId = d.MessageId
            WHERE d.SubscriberId = $subscriberId
              AND m.Topic = $topic
              AND d.Status = 'PENDING'
              AND (d.LastAttemptAt IS NULL
                   OR d.LastAttemptAt <= $cutoff)
            ORDER BY m.CreatedAt;
            """;
        command.Parameters.AddWithValue(
            "$subscriberId",
            subscriberId
        );
        command.Parameters.AddWithValue(
            "$topic",
            topic
        );
        command.Parameters.AddWithValue(
            "$cutoff",
            GetAckTimeoutCutoff()
        );
        await using SqliteDataReader reader =
            await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            result.Add(
                reader.GetString(0)
            );
        }
        return result;
    }
    // =========================================================
    // RETRY
    // =========================================================
    private static async Task IncrementRetryAsync(
        string messageId,
        string subscriberId,
        string? error)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            UPDATE Deliveries
            SET
                RetryCount = RetryCount + 1,
                LastAttemptAt = $lastAttempt,
                LastError = $error
            WHERE MessageId = $messageId
              AND SubscriberId = $subscriberId
              AND Status = 'PENDING';
            """;
        command.Parameters.AddWithValue(
            "$messageId",
            messageId
        );
        command.Parameters.AddWithValue(
            "$subscriberId",
            subscriberId
        );
        command.Parameters.AddWithValue(
            "$lastAttempt",
            DateTime.UtcNow.ToString("O")
        );
        command.Parameters.AddWithValue(
            "$error",
            (object?)error ?? DBNull.Value
        );
        await command.ExecuteNonQueryAsync();
    }
    // =========================================================
    // ACK / DELIVERED
    // =========================================================
    private static async Task<bool>
        MarkDeliveryAsDeliveredAsync(
            string messageId,
            string subscriberId)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            UPDATE Deliveries
            SET
                Status = 'DELIVERED',
                LastError = NULL
            WHERE MessageId = $messageId
              AND SubscriberId = $subscriberId
              AND Status = 'PENDING';
            """;
        command.Parameters.AddWithValue(
            "$messageId",
            messageId
        );
        command.Parameters.AddWithValue(
            "$subscriberId",
            subscriberId
        );
        int affected =
            await command.ExecuteNonQueryAsync();
        return affected == 1;
    }
    private static async Task<bool>
        IsDeliveryDeliveredAsync(
            string messageId,
            string subscriberId)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*)
            FROM Deliveries
            WHERE MessageId = $messageId
              AND SubscriberId = $subscriberId
              AND Status = 'DELIVERED';
            """;
        command.Parameters.AddWithValue(
            "$messageId",
            messageId
        );
        command.Parameters.AddWithValue(
            "$subscriberId",
            subscriberId
        );
        object? result =
            await command.ExecuteScalarAsync();
        return Convert.ToInt32(result) > 0;
    }
    // =========================================================
    // UPDATE MESSAGE GLOBAL STATUS
    // =========================================================
    private static async Task UpdateMessageStatusAsync(
        string messageId)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            SELECT
                COUNT(*),
                SUM(CASE WHEN Status = 'PENDING'
                    THEN 1 ELSE 0 END),
                SUM(CASE WHEN Status = 'DELIVERED'
                    THEN 1 ELSE 0 END),
                SUM(CASE WHEN Status = 'DEAD'
                    THEN 1 ELSE 0 END)
            FROM Deliveries
            WHERE MessageId = $messageId;
            """;
        command.Parameters.AddWithValue(
            "$messageId",
            messageId
        );
        int total;
        int pending;
        int delivered;
        await using (
            SqliteDataReader reader =
                await command.ExecuteReaderAsync())
        {
            await reader.ReadAsync();
            total =
                reader.IsDBNull(0)
                    ? 0
                    : reader.GetInt32(0);
            pending =
                reader.IsDBNull(1)
                    ? 0
                    : reader.GetInt32(1);
            delivered =
                reader.IsDBNull(2)
                    ? 0
                    : reader.GetInt32(2);
        }
        string newStatus;
        if (total == 0)
        {
            newStatus = "PENDING";
        }
        else if (pending > 0)
        {
            newStatus = "PENDING";
        }
        else if (delivered > 0)
        {
            newStatus = "DELIVERED";
        }
        else
        {
            newStatus = "DEAD";
        }
        await using SqliteCommand update =
            connection.CreateCommand();
        update.CommandText = """
            UPDATE Messages
            SET Status = $status
            WHERE MessageId = $messageId;
            """;
        update.Parameters.AddWithValue(
            "$status",
            newStatus
        );
        update.Parameters.AddWithValue(
            "$messageId",
            messageId
        );
        await update.ExecuteNonQueryAsync();
    }
    // =========================================================
    // DEAD LETTER QUEUE
    // =========================================================
    private static async Task MoveToDeadLetterAsync(
        PendingDelivery delivery,
        string reason)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using var transaction =
        (SqliteTransaction)await connection.BeginTransactionAsync();
        await using SqliteCommand update =
            connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText = """
            UPDATE Deliveries
            SET
                Status = 'DEAD',
                LastError = $reason
            WHERE MessageId = $messageId
              AND SubscriberId = $subscriberId
              AND Status = 'PENDING'
              AND RetryCount >= $maxRetries;
            """;
        update.Parameters.AddWithValue(
            "$reason",
            reason
        );
        update.Parameters.AddWithValue(
            "$messageId",
            delivery.MessageId
        );
        update.Parameters.AddWithValue(
            "$subscriberId",
            delivery.SubscriberId
        );
        update.Parameters.AddWithValue(
            "$maxRetries",
            MAX_RETRIES
        );
        int affected =
            await update.ExecuteNonQueryAsync();
        if (affected == 1)
        {
            await using SqliteCommand insert =
                connection.CreateCommand();
           insert.Transaction = transaction;
            insert.CommandText = """
                INSERT INTO DeadLetters
                (
                    MessageId,
                    SubscriberId,
                    Topic,
                    RawPayload,
                    Reason,
                    RetryCount,
                    CreatedAt
                )
                VALUES
                (
                    $messageId,
                    $subscriberId,
                    $topic,
                    $payload,
                    $reason,
                    $retryCount,
                    $createdAt
                );
                """;
            insert.Parameters.AddWithValue(
                "$messageId",
                delivery.MessageId
            );
            insert.Parameters.AddWithValue(
                "$subscriberId",
                delivery.SubscriberId
            );
            insert.Parameters.AddWithValue(
                "$topic",
                delivery.Topic
            );
            insert.Parameters.AddWithValue(
                "$payload",
                delivery.Content
            );
            insert.Parameters.AddWithValue(
                "$reason",
                reason
            );
            insert.Parameters.AddWithValue(
                "$retryCount",
                delivery.RetryCount
            );
            insert.Parameters.AddWithValue(
                "$createdAt",
                DateTime.UtcNow.ToString("O")
            );
            await insert.ExecuteNonQueryAsync();
            Console.WriteLine(
                $"[DLQ] {delivery.MessageId} " +
                $"for {delivery.SubscriberId}"
            );
        }
        await transaction.CommitAsync();
        if (affected == 1)
        {
            await UpdateMessageStatusAsync(
                delivery.MessageId
            );
        }
    }
    private static async Task SaveRawDeadLetterAsync(
        string rawPayload,
        string reason)
    {
        await using SqliteConnection connection =
            new SqliteConnection(CONNECTION_STRING);
        await connection.OpenAsync();
        await using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = """
            INSERT INTO DeadLetters
            (
                RawPayload,
                Reason,
                RetryCount,
                CreatedAt
            )
            VALUES
            (
                $payload,
                $reason,
                0,
                $createdAt
            );
            """;
        command.Parameters.AddWithValue(
            "$payload",
            rawPayload
        );
        command.Parameters.AddWithValue(
            "$reason",
            reason
        );
        command.Parameters.AddWithValue(
            "$createdAt",
            DateTime.UtcNow.ToString("O")
        );
        await command.ExecuteNonQueryAsync();
    }
    // =========================================================
    // SEND JSON
    // =========================================================
    private static async Task SendJsonAsync(
        SubscriberConnection connection,
        object data)
    {
        string json =
            JsonSerializer.Serialize(
                data,
                SerializerOptions
            );
        await connection.SendLock.WaitAsync();
        try
        {
            await connection.Writer.WriteLineAsync(
                json
            );
            await connection.Writer.FlushAsync();
        }
        finally
        {
            connection.SendLock.Release();
        }
    }
    // =========================================================
    // HELPERS
    // =========================================================
    private static string GetAckTimeoutCutoff()
    {
        return DateTime.UtcNow
            .AddSeconds(-ACK_TIMEOUT_SECONDS)
            .ToString("O");
    }
    private static string? GetString(
        JsonElement element,
        string property)
    {
        if (!element.TryGetProperty(
            property,
            out JsonElement value))
        {
            return null;
        }
        if (value.ValueKind !=
            JsonValueKind.String)
        {
            return null;
        }
        return value.GetString();
    }
}
// =============================================================
// SUBSCRIBER CONNECTION
// =============================================================
class SubscriberConnection
{
    public TcpClient Client { get; }
    public StreamReader Reader { get; }
    public StreamWriter Writer { get; }
    public string? SubscriberId { get; set; }
    public ConcurrentDictionary<string, byte>
        Topics { get; } = new();
    public SemaphoreSlim SendLock { get; } =
        new(1, 1);
    public SubscriberConnection(
        TcpClient client,
        NetworkStream stream)
    {
        Client = client;
        Reader =
            new StreamReader(
                stream,
                Encoding.UTF8,
                false,
                4096,
                leaveOpen: true
            );
        Writer =
            new StreamWriter(
                stream,
                new UTF8Encoding(false),
                4096,
                leaveOpen: true
            )
            {
                AutoFlush = true,
                NewLine = "\n"
            };
    }
}
// =============================================================
// PENDING DELIVERY MODEL
// =============================================================
class PendingDelivery
{
    public string MessageId { get; set; } =
        string.Empty;
    public string SubscriberId { get; set; } =
        string.Empty;
    public string Topic { get; set; } =
        string.Empty;
    public string Content { get; set; } =
        string.Empty;
    public int RetryCount { get; set; }
}
