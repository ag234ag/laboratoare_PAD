using System.Text.RegularExpressions;
using Broker.Contracts;

namespace Broker.Validation;

public sealed partial class RequestValidator
{
    public const int MaxNameLength = 64;
    public const int MaxContentLength = 4096;

    public IReadOnlyList<string> ValidatePublish(PublishRequest request)
    {
        var errors = new List<string>();
        CheckName(request.MessageId, "MESSAGE_ID", errors);
        CheckName(request.Topic, "TOPIC", errors);
        CheckContent(request.Content, errors);
        return errors;
    }

    public IReadOnlyList<string> ValidateSubscription(SubscriptionRequest request)
    {
        var errors = new List<string>();
        CheckName(request.SubscriberId, "SUBSCRIBER_ID", errors);
        CheckName(request.Topic, "TOPIC", errors);
        return errors;
    }

    public IReadOnlyList<string> ValidateListen(ListenRequest request)
    {
        var errors = new List<string>();
        CheckName(request.SubscriberId, "SUBSCRIBER_ID", errors);
        return errors;
    }

    public IReadOnlyList<string> ValidateAck(AckRequest request)
    {
        var errors = new List<string>();
        CheckName(request.SubscriberId, "SUBSCRIBER_ID", errors);
        CheckName(request.MessageId, "MESSAGE_ID", errors);
        return errors;
    }

    private static void CheckName(string value, string field, List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(value))
            errors.Add($"{field}_REQUIRED");
        else if (CountCodePoints(value) > MaxNameLength)
            errors.Add($"{field}_TOO_LONG");
        else if (!NamePattern().IsMatch(value))
            errors.Add($"{field}_INVALID_CHARS");
    }

    private static void CheckContent(string value, List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(value))
            errors.Add("CONTENT_REQUIRED");
        else if (CountCodePoints(value) > MaxContentLength)
            errors.Add("CONTENT_TOO_LONG");
    }

    private static int CountCodePoints(string value) => value.EnumerateRunes().Count();

    [GeneratedRegex(@"^[\p{L}\p{N}._-]+\z")]
    private static partial Regex NamePattern();
}
