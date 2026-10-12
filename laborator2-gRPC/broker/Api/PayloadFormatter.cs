using Google.Protobuf;

namespace Broker.Api;

public static class PayloadFormatter
{
    private static readonly JsonFormatter Formatter = new(JsonFormatter.Settings.Default.WithFormatDefaultValues(true));

    public static string Format(IMessage message) => Formatter.Format(message);
}
