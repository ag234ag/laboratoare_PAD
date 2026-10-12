using Broker.Contracts;
using Broker.Validation;

namespace Broker.Tests;

public sealed class RequestValidatorTests
{
    private readonly RequestValidator _validator = new();

    private static PublishRequest Publish(
        string id = "3f0c2a1e-9b7d-4c55-8e0f-1a2b3c4d5e6f",
        string topic = "sport",
        string content = "goal") =>
        new() { MessageId = id, Topic = topic, Content = content };

    [Fact]
    public void Valid_publish_has_no_errors() =>
        Assert.Empty(_validator.ValidatePublish(Publish()));

    [Fact]
    public void Missing_fields_report_every_error_in_field_order() =>
        Assert.Equal(
            new[] { "MESSAGE_ID_REQUIRED", "TOPIC_REQUIRED", "CONTENT_REQUIRED" },
            _validator.ValidatePublish(Publish("", "", "")));

    [Theory]
    [InlineData("   ")]
    [InlineData("\t\n")]
    public void Whitespace_only_content_is_required_error(string content) =>
        Assert.Equal(new[] { "CONTENT_REQUIRED" }, _validator.ValidatePublish(Publish(content: content)));

    [Theory]
    [InlineData(64, null)]
    [InlineData(65, "TOPIC_TOO_LONG")]
    public void Topic_length_limit_is_64(int length, string? expected) =>
        AssertSingleOrNone(expected, _validator.ValidatePublish(Publish(topic: new string('t', length))));

    [Theory]
    [InlineData(64, null)]
    [InlineData(65, "MESSAGE_ID_TOO_LONG")]
    public void Message_id_length_limit_is_64(int length, string? expected) =>
        AssertSingleOrNone(expected, _validator.ValidatePublish(Publish(id: new string('m', length))));

    [Theory]
    [InlineData(4096, null)]
    [InlineData(4097, "CONTENT_TOO_LONG")]
    public void Content_length_limit_is_4096(int length, string? expected) =>
        AssertSingleOrNone(expected, _validator.ValidatePublish(Publish(content: new string('c', length))));

    [Fact]
    public void Content_length_counts_unicode_code_points_not_utf16_units()
    {
        string content = string.Concat(Enumerable.Repeat("😀", 4096));

        Assert.Empty(_validator.ValidatePublish(Publish(content: content)));
        Assert.Equal(new[] { "CONTENT_TOO_LONG" }, _validator.ValidatePublish(Publish(content: content + "😀")));
    }

    [Theory]
    [InlineData("sport")]
    [InlineData("știri.locale")]
    [InlineData("a_b-c.1")]
    [InlineData("ĂÎÂȘȚ")]
    public void Topic_accepts_letters_digits_dot_underscore_dash(string topic) =>
        Assert.Empty(_validator.ValidatePublish(Publish(topic: topic)));

    [Theory]
    [InlineData("bad topic")]
    [InlineData("sport!")]
    [InlineData("sport\n")]
    [InlineData("a/b")]
    public void Topic_rejects_other_characters(string topic) =>
        Assert.Equal(new[] { "TOPIC_INVALID_CHARS" }, _validator.ValidatePublish(Publish(topic: topic)));

    [Fact]
    public void Subscription_requires_subscriber_and_topic() =>
        Assert.Equal(
            new[] { "SUBSCRIBER_ID_REQUIRED", "TOPIC_REQUIRED" },
            _validator.ValidateSubscription(new SubscriptionRequest()));

    [Fact]
    public void Listen_rejects_subscriber_id_with_spaces() =>
        Assert.Equal(
            new[] { "SUBSCRIBER_ID_INVALID_CHARS" },
            _validator.ValidateListen(new ListenRequest { SubscriberId = "java pc" }));

    [Fact]
    public void Ack_requires_subscriber_and_message_id() =>
        Assert.Equal(
            new[] { "SUBSCRIBER_ID_REQUIRED", "MESSAGE_ID_REQUIRED" },
            _validator.ValidateAck(new AckRequest()));

    private static void AssertSingleOrNone(string? expected, IReadOnlyList<string> errors)
    {
        if (expected is null)
            Assert.Empty(errors);
        else
            Assert.Equal(new[] { expected }, errors);
    }
}
