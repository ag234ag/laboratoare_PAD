package md.utm.pad.subscriber;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import md.utm.pad.broker.grpc.AckReply;
import md.utm.pad.broker.grpc.AckRequest;
import md.utm.pad.broker.grpc.DeliveredMessage;
import md.utm.pad.broker.grpc.ListenRequest;
import md.utm.pad.broker.grpc.MessageBrokerGrpc;
import md.utm.pad.broker.grpc.PingReply;
import md.utm.pad.broker.grpc.PingRequest;
import md.utm.pad.broker.grpc.SubscriptionReply;
import md.utm.pad.broker.grpc.SubscriptionRequest;

public final class BrokerClient implements AutoCloseable {

    private static final long CALL_DEADLINE_SECONDS = 5;
    private static final long RECONNECT_DELAY_SECONDS = 3;
    private static final long KEEPALIVE_SECONDS = 30;
    private static final long KEEPALIVE_TIMEOUT_SECONDS = 10;

    private final ManagedChannel channel;
    private final MessageBrokerGrpc.MessageBrokerBlockingStub blockingStub;
    private final MessageBrokerGrpc.MessageBrokerStub asyncStub;
    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "listen-reconnect");
        thread.setDaemon(true);
        return thread;
    });
    private final Consumer<String> output;
    private volatile boolean running = true;

    public BrokerClient(String host, int port, Consumer<String> output) {
        this.output = output;
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .keepAliveTime(KEEPALIVE_SECONDS, TimeUnit.SECONDS)
                .keepAliveTimeout(KEEPALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .build();
        this.blockingStub = MessageBrokerGrpc.newBlockingStub(channel);
        this.asyncStub = MessageBrokerGrpc.newStub(channel);
    }

    public SubscriptionReply subscribe(String subscriberId, String topic) {
        return blocking().subscribe(subscription(subscriberId, topic));
    }

    public SubscriptionReply unsubscribe(String subscriberId, String topic) {
        return blocking().unsubscribe(subscription(subscriberId, topic));
    }

    public PingReply ping() {
        return blocking().ping(PingRequest.getDefaultInstance());
    }

    public void ack(String subscriberId, String messageId) {
        AckRequest request = AckRequest.newBuilder()
                .setSubscriberId(subscriberId)
                .setMessageId(messageId)
                .build();
        output.accept("[ACK] Sent for message: " + messageId);
        asyncStub.withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS).ack(request, new StreamObserver<AckReply>() {
            @Override
            public void onNext(AckReply reply) {
                output.accept(reply.getDuplicate()
                        ? "[BROKER] Duplicate ACK ignored for: " + reply.getMessageId()
                        : "[BROKER] ACK confirmed for: " + reply.getMessageId());
            }

            @Override
            public void onError(Throwable error) {
                output.accept("[ACK ERROR] " + messageId + ": " + describe(error));
            }

            @Override
            public void onCompleted() {
            }
        });
    }

    public void listen(String subscriberId, Consumer<DeliveredMessage> onMessage) {
        if (!running) {
            return;
        }
        ListenRequest request = ListenRequest.newBuilder().setSubscriberId(subscriberId).build();
        asyncStub.listen(request, new StreamObserver<DeliveredMessage>() {
            @Override
            public void onNext(DeliveredMessage message) {
                onMessage.accept(message);
            }

            @Override
            public void onError(Throwable error) {
                if (!running) {
                    return;
                }
                Status status = Status.fromThrowable(error);
                if (status.getCode() == Status.Code.ABORTED && "SESSION_REPLACED".equals(status.getDescription())) {
                    output.accept("[LISTEN] Another subscriber connected with the same ID. Listening stopped.");
                    return;
                }
                if (status.getCode() == Status.Code.INVALID_ARGUMENT) {
                    output.accept("[LISTEN] Rejected by Broker: " + status.getDescription());
                    return;
                }
                output.accept("[LISTEN] Connection to Broker lost (" + status.getCode()
                        + "). Reconnecting in " + RECONNECT_DELAY_SECONDS + "s...");
                scheduleReconnect(subscriberId, onMessage);
            }

            @Override
            public void onCompleted() {
                if (running) {
                    output.accept("[LISTEN] Stream closed by Broker. Reconnecting in " + RECONNECT_DELAY_SECONDS + "s...");
                    scheduleReconnect(subscriberId, onMessage);
                }
            }
        });
    }

    public static String describe(Throwable error) {
        Status status = Status.fromThrowable(error);
        if (status.getCode() == Status.Code.UNAVAILABLE) {
            return "Broker unavailable (check host/IP, port, firewall and that the Broker is running)";
        }
        return status.getCode() + " - " + status.getDescription();
    }

    @Override
    public void close() {
        running = false;
        reconnectScheduler.shutdownNow();
        channel.shutdownNow();
        try {
            channel.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void scheduleReconnect(String subscriberId, Consumer<DeliveredMessage> onMessage) {
        try {
            reconnectScheduler.schedule(() -> listen(subscriberId, onMessage), RECONNECT_DELAY_SECONDS, TimeUnit.SECONDS);
        } catch (RejectedExecutionException ignored) {
        }
    }

    private MessageBrokerGrpc.MessageBrokerBlockingStub blocking() {
        return blockingStub.withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS);
    }

    private static SubscriptionRequest subscription(String subscriberId, String topic) {
        return SubscriptionRequest.newBuilder().setSubscriberId(subscriberId).setTopic(topic).build();
    }
}
