namespace Broker.Tests;

public sealed class BrokerOptionsTests
{
    [Fact]
    public void Defaults_when_no_arguments()
    {
        BrokerOptions options = BrokerOptions.Parse([]);

        Assert.Equal(50051, options.Port);
        Assert.Equal(StorageMode.Persistent, options.Storage);
        Assert.Equal("broker.db", options.DatabasePath);
        Assert.Equal(4, options.Workers);
        Assert.Equal(3, options.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(1), options.RetryInterval);
        Assert.Equal(TimeSpan.FromSeconds(5), options.AckTimeout);
        Assert.Equal("persistent", options.StorageName);
    }

    [Fact]
    public void Parses_all_arguments()
    {
        BrokerOptions options = BrokerOptions.Parse(
            ["--port", "6000", "--storage", "Transient", "--db", "data/x.db", "--workers", "8"]);

        Assert.Equal(6000, options.Port);
        Assert.Equal(StorageMode.Transient, options.Storage);
        Assert.Equal("data/x.db", options.DatabasePath);
        Assert.Equal(8, options.Workers);
        Assert.Equal("transient", options.StorageName);
    }

    [Theory]
    [InlineData("--port", "0")]
    [InlineData("--port", "70000")]
    [InlineData("--port", "abc")]
    [InlineData("--storage", "memory")]
    [InlineData("--workers", "0")]
    [InlineData("--workers", "65")]
    [InlineData("--db", " ")]
    [InlineData("--unknown", "1")]
    [InlineData("port", "50051")]
    public void Rejects_invalid_arguments(string key, string value) =>
        Assert.Throws<ArgumentException>(() => BrokerOptions.Parse([key, value]));

    [Fact]
    public void Rejects_argument_without_value() =>
        Assert.Throws<ArgumentException>(() => BrokerOptions.Parse(["--port"]));
}
