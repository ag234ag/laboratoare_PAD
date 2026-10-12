using System.Globalization;

namespace Broker;

public enum StorageMode
{
    Persistent,
    Transient
}

public sealed class BrokerOptions
{
    public const string Usage =
        "Usage: Broker [--port 50051] [--storage persistent|transient] [--db broker.db] [--workers 4]";

    public int Port { get; init; } = 50051;
    public StorageMode Storage { get; init; } = StorageMode.Persistent;
    public string DatabasePath { get; init; } = "broker.db";
    public int Workers { get; init; } = 4;
    public int MaxRetries { get; init; } = 3;
    public TimeSpan RetryInterval { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan AckTimeout { get; init; } = TimeSpan.FromSeconds(5);

    public string StorageName => Storage == StorageMode.Persistent ? "persistent" : "transient";

    public static BrokerOptions Parse(IReadOnlyList<string> args)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < args.Count; i += 2)
        {
            string key = args[i];
            if (!key.StartsWith("--", StringComparison.Ordinal) || i + 1 >= args.Count)
                throw new ArgumentException($"Invalid argument: {key}");
            values[key[2..]] = args[i + 1];
        }

        var defaults = new BrokerOptions();
        var options = new BrokerOptions
        {
            Port = values.Remove("port", out string? port) ? ParseRange(port, "port", 1, 65535) : defaults.Port,
            Storage = values.Remove("storage", out string? storage) ? ParseStorage(storage) : defaults.Storage,
            DatabasePath = values.Remove("db", out string? db) ? RequireValue(db, "db") : defaults.DatabasePath,
            Workers = values.Remove("workers", out string? workers) ? ParseRange(workers, "workers", 1, 64) : defaults.Workers
        };

        if (values.Count > 0)
            throw new ArgumentException($"Unknown argument: --{values.Keys.First()}");
        return options;
    }

    private static int ParseRange(string value, string name, int min, int max)
    {
        if (int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out int number)
            && number >= min && number <= max)
            return number;
        throw new ArgumentException($"--{name} must be an integer between {min} and {max}");
    }

    private static StorageMode ParseStorage(string value) => value.ToLowerInvariant() switch
    {
        "persistent" => StorageMode.Persistent,
        "transient" => StorageMode.Transient,
        _ => throw new ArgumentException("--storage must be 'persistent' or 'transient'")
    };

    private static string RequireValue(string value, string name) =>
        string.IsNullOrWhiteSpace(value) ? throw new ArgumentException($"--{name} cannot be empty") : value;
}
