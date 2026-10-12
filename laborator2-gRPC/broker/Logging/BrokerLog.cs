namespace Broker.Logging;

public interface IBrokerLog
{
    void Info(string message);
    void Error(string message);
}

public sealed class ConsoleBrokerLog : IBrokerLog
{
    public void Info(string message) => Console.WriteLine($"{DateTime.Now:HH:mm:ss} {message}");

    public void Error(string message) => Console.WriteLine($"{DateTime.Now:HH:mm:ss} [ERROR] {message}");
}
