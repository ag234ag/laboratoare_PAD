package md.utm.pad.subscriber;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import md.utm.pad.broker.grpc.DeliveredMessage;
import org.junit.jupiter.api.Test;

class MessageProcessorTest {

    private final List<String> acks = new ArrayList<>();
    private final List<String> output = new ArrayList<>();
    private final MessageProcessor processor = new MessageProcessor(acks::add, output::add);

    private static DeliveredMessage message(String id, int attempt) {
        return DeliveredMessage.newBuilder()
                .setMessageId(id)
                .setTopic("sport")
                .setContent("goal")
                .setAttempt(attempt)
                .setMaxAttempts(3)
                .build();
    }

    @Test
    void acknowledgesNewMessage() {
        processor.handle(message("m1", 1));

        assertEquals(List.of("m1"), acks);
        assertTrue(output.get(0).contains("NEW MESSAGE"));
        assertTrue(output.get(0).contains("Content    : goal"));
        assertTrue(output.get(0).contains("Attempt    : 1/3"));
    }

    @Test
    void duplicateIsNotShownAgainButIsAcknowledgedAgain() {
        processor.handle(message("m1", 1));
        processor.handle(message("m1", 2));

        assertEquals(List.of("m1", "m1"), acks);
        assertTrue(output.get(1).startsWith("[DUPLICATE]"));
    }

    @Test
    void autoAckOffDisplaysWithoutAcknowledging() {
        processor.toggleAutoAck();

        processor.handle(message("m1", 1));

        assertTrue(acks.isEmpty());
        assertTrue(output.get(0).contains("[ACK disabled]"));
    }

    @Test
    void messageSeenWithAckOffIsProcessedNormallyAfterAckIsTurnedOn() {
        processor.toggleAutoAck();
        processor.handle(message("m1", 1));
        processor.toggleAutoAck();

        processor.handle(message("m1", 2));

        assertEquals(List.of("m1"), acks);
        assertTrue(output.get(1).contains("NEW MESSAGE"));
    }
}
