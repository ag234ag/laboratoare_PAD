using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace Broker.Hosting;

public sealed record LocalAddress(string Address, string AdapterName, bool HasGateway);

public static class NetworkAddresses
{
    public static IReadOnlyList<LocalAddress> LocalIPv4() =>
        NetworkInterface.GetAllNetworkInterfaces()
            .Where(n => n.OperationalStatus == OperationalStatus.Up && n.NetworkInterfaceType != NetworkInterfaceType.Loopback)
            .SelectMany(ToLocalAddresses)
            .DistinctBy(a => a.Address)
            .OrderByDescending(a => a.HasGateway)
            .ToList();

    private static IEnumerable<LocalAddress> ToLocalAddresses(NetworkInterface adapter)
    {
        IPInterfaceProperties properties = adapter.GetIPProperties();
        bool hasGateway = properties.GatewayAddresses.Any(g =>
            g.Address.AddressFamily == AddressFamily.InterNetwork && !g.Address.Equals(System.Net.IPAddress.Any));
        return properties.UnicastAddresses
            .Where(a => a.Address.AddressFamily == AddressFamily.InterNetwork)
            .Select(a => new LocalAddress(a.Address.ToString(), adapter.Name, hasGateway));
    }
}
