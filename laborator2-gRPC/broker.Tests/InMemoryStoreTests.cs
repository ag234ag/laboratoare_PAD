using Broker.Storage;

namespace Broker.Tests;

public sealed class InMemoryStoreTests : StoreContractTests
{
    protected override IBrokerStore CreateStore() => new InMemoryStore();
}
