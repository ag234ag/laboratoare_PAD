package md.utm.pad.subscriber;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import md.utm.pad.broker.grpc.DeliveredMessage;

public final class MessageProcessor {

    private final Set<String> processedMessageIds = ConcurrentHashMap.newKeySet();
    private final AckSender ackSender;
    private final Consumer<String> output;
    private volatile boolean autoAck = true;

    public MessageProcessor(AckSender ackSender, Consumer<String> output) {
        this.ackSender = ackSender;
        this.output = output;
    }

    public void handle(DeliveredMessage message) {
        String attempt = message.getAttempt() + "/" + message.getMaxAttempts();

        if (!autoAck) {
            output.accept(describe(message, attempt) + System.lineSeparator()
                    + "[ACK disabled] No ACK sent; the Broker will retry, then move it to the DLQ.");
            return;
        }

        if (!processedMessageIds.add(message.getMessageId())) {
            output.accept("[DUPLICATE] Message " + message.getMessageId()
                    + " already processed (attempt " + attempt + "), re-sending ACK.");
            ackSender.sendAck(message.getMessageId());
            return;
        }

        output.accept(describe(message, attempt));
        ackSender.sendAck(message.getMessageId());
    }

    public boolean toggleAutoAck() {
        autoAck = !autoAck;
        return autoAck;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    private static String describe(DeliveredMessage message, String attempt) {
        return String.join(System.lineSeparator(),
                "==========================================",
                "NEW MESSAGE",
                "Message ID : " + message.getMessageId(),
                "Topic      : " + message.getTopic(),
                "Content    : " + message.getContent(),
                "Attempt    : " + attempt,
                "==========================================");
    }
}
