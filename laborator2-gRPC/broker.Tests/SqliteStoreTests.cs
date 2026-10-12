using Broker.Storage;
using Microsoft.Data.Sqlite;

namespace Broker.Tests;

public sealed class SqliteStoreTests : StoreContractTests
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"broker-test-{Guid.NewGuid():N}.db");

    protected override IBrokerStore CreateStore() => new SqliteStore(_path);

    [Fact]
    public async Task Data_survives_a_new_store_instance_on_the_same_file()
    {
        await Store.AddSubscriptionAsync("sub-a", "sport", T0);
        await Store.AddMessageAsync(new StoredMessage("m1", "sport", "goal"), T0);

        var reopened = new SqliteStore(_path);
        await reopened.InitializeAsync();

        Assert.Equal("m1", Assert.Single(await reopened.GetDueDeliveriesAsync("sub-a", T0)).MessageId);
    }

    public override void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (string suffix in new[] { "", "-wal", "-shm" })
            File.Delete(_path + suffix);
    }
}
