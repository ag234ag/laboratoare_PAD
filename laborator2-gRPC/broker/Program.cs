using System.Text;
using Broker;
using Broker.Api;
using Broker.Delivery;
using Broker.Hosting;
using Broker.Logging;
using Broker.Routing;
using Broker.Storage;
using Broker.Validation;
using Microsoft.AspNetCore.Server.Kestrel.Core;

BrokerOptions options;
try
{
    options = BrokerOptions.Parse(args);
}
catch (ArgumentException ex)
{
    Console.Error.WriteLine(ex.Message);
    Console.Error.WriteLine(BrokerOptions.Usage);
    return 1;
}

Console.OutputEncoding = Encoding.UTF8;

IBrokerStore store = options.Storage == StorageMode.Persistent
    ? new SqliteStore(options.DatabasePath)
    : new InMemoryStore();
await store.InitializeAsync();

WebApplicationBuilder builder = WebApplication.CreateBuilder();
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole();
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.Extensions.Hosting.Internal.Host", LogLevel.None);
builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.ListenAnyIP(options.Port, listen => listen.Protocols = HttpProtocols.Http2);
    kestrel.Limits.Http2.KeepAlivePingDelay = TimeSpan.FromSeconds(30);
    kestrel.Limits.Http2.KeepAlivePingTimeout = TimeSpan.FromSeconds(10);
});

builder.Services.AddSingleton(options);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<IBrokerLog, ConsoleBrokerLog>();
builder.Services.AddSingleton(store);
builder.Services.AddSingleton<IMessageStore>(store);
builder.Services.AddSingleton<ISubscriptionStore>(store);
builder.Services.AddSingleton<IDeliveryStore>(store);
builder.Services.AddSingleton<IDeadLetterStore>(store);
builder.Services.AddSingleton<IStateReader>(store);
builder.Services.AddSingleton<RequestValidator>();
builder.Services.AddSingleton<SubscriberRegistry>();
builder.Services.AddSingleton<DeliveryJobQueue>();
builder.Services.AddSingleton<DeliveryProcessor>();
builder.Services.AddSingleton<TopicRouter>();
builder.Services.AddHostedService<DeliveryWorkerPool>();
builder.Services.AddHostedService<RetryScheduler>();
builder.Services.AddGrpc(grpc =>
{
    grpc.Interceptors.Add<ExceptionInterceptor>();
    grpc.MaxReceiveMessageSize = 64 * 1024;
});
builder.Services.AddGrpcReflection();

WebApplication app = builder.Build();
app.MapGrpcService<MessageBrokerService>();
app.MapGrpcReflectionService();
app.Lifetime.ApplicationStopping.Register(() => app.Services.GetRequiredService<SubscriberRegistry>().CloseAll());

try
{
    await app.StartAsync();
}
catch (Exception ex) when (IsAddressInUse(ex))
{
    Console.Error.WriteLine($"Port {options.Port} is already in use. Stop the other broker or choose another --port.");
    return 1;
}

Console.WriteLine(" ------ MESSAGE BROKER - gRPC / Protobuf ------");
Console.WriteLine(options.Storage == StorageMode.Persistent
    ? $"Storage: persistent (SQLite: {Path.GetFullPath(options.DatabasePath)})"
    : "Storage: transient (in-memory, lost on restart)");
Console.WriteLine($"Listening on 0.0.0.0:{options.Port} (HTTP/2 over TCP), {options.Workers} delivery workers");
foreach (LocalAddress address in NetworkAddresses.LocalIPv4())
    Console.WriteLine($"  clients on other computers can connect to {address.Address}:{options.Port}  ({address.AdapterName}{(address.HasGateway ? ", default gateway" : "")})");
Console.WriteLine();

await app.WaitForShutdownAsync();
return 0;

static bool IsAddressInUse(Exception ex)
{
    for (Exception? e = ex; e is not null; e = e.InnerException)
    {
        if (e is System.Net.Sockets.SocketException { SocketErrorCode: System.Net.Sockets.SocketError.AddressAlreadyInUse })
            return true;
    }
    return false;
}
