package md.utm.pad.subscriber;

@FunctionalInterface
public interface AckSender {
    void sendAck(String messageId);
}
