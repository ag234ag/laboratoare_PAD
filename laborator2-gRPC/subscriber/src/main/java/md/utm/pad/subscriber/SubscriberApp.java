package md.utm.pad.subscriber;

import io.grpc.StatusRuntimeException;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import md.utm.pad.broker.grpc.PingReply;
import md.utm.pad.broker.grpc.SubscriptionReply;

public final class SubscriberApp {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 50051;
    private static final int MAX_ID_LENGTH = 64;

    private final BrokerClient client;
    private final MessageProcessor processor;
    private final String subscriberId;
    private final Scanner scanner;
    private final Set<String> topics = ConcurrentHashMap.newKeySet();

    private SubscriberApp(BrokerClient client, MessageProcessor processor, String subscriberId, Scanner scanner) {
        this.client = client;
        this.processor = processor;
        this.subscriberId = subscriberId;
        this.scanner = scanner;
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, "UTF-8"));
        String host = args.length >= 1 ? args[0] : DEFAULT_HOST;
        int port = parsePort(args);

        System.out.println(" ------ SUBSCRIBER - JAVA (gRPC) ----");
        System.out.println("Broker: " + host + ":" + port);

        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
        String defaultId = defaultSubscriberId();
        System.out.print("Subscriber ID (Enter = " + defaultId + "): ");
        String typed = scanner.hasNextLine() ? scanner.nextLine().trim() : "";
        String subscriberId = typed.isEmpty() ? defaultId : typed;

        try (BrokerClient client = new BrokerClient(host, port, SubscriberApp::print)) {
            MessageProcessor processor = new MessageProcessor(
                    messageId -> client.ack(subscriberId, messageId), SubscriberApp::print);
            new SubscriberApp(client, processor, subscriberId, scanner).run();
        }
        System.out.println();
        System.out.println("Subscriber closed.");
    }

    private void run() {
        print("[LISTEN] Listening for messages as '" + subscriberId + "'");
        client.listen(subscriberId, processor::handle);

        while (true) {
            printMenu();
            if (!scanner.hasNextLine()) {
                return;
            }
            String option = scanner.nextLine().trim();
            switch (option) {
                case "1":
                    subscribe();
                    break;
                case "2":
                    unsubscribe();
                    break;
                case "3":
                    showTopics();
                    break;
                case "4":
                    ping();
                    break;
                case "5":
                    toggleAutoAck();
                    break;
                case "0":
                    return;
                default:
                    print("Invalid option.");
            }
        }
    }

    private void printMenu() {
        System.out.println();
        System.out.println("------------------------------------------");
        System.out.println("1 - Subscribe to a topic");
        System.out.println("2 - Unsubscribe from a topic");
        System.out.println("3 - Show subscribed topics");
        System.out.println("4 - Ping Broker");
        System.out.println("5 - Auto-ACK on/off (now: " + (processor.isAutoAck() ? "ON" : "OFF") + ")");
        System.out.println("0 - Exit");
        System.out.println("------------------------------------------");
        System.out.print("Choose an option: ");
    }

    private void subscribe() {
        String topic = readTopic();
        if (topic == null) {
            return;
        }
        try {
            SubscriptionReply reply = client.subscribe(subscriberId, topic);
            topics.add(topic);
            print("[BROKER] Subscription confirmed: " + reply.getTopic());
        } catch (StatusRuntimeException e) {
            print("Subscribe error: " + BrokerClient.describe(e));
        }
    }

    private void unsubscribe() {
        String topic = readTopic();
        if (topic == null) {
            return;
        }
        try {
            SubscriptionReply reply = client.unsubscribe(subscriberId, topic);
            topics.remove(topic);
            print("[BROKER] Unsubscription confirmed: " + reply.getTopic());
        } catch (StatusRuntimeException e) {
            print("Unsubscribe error: " + BrokerClient.describe(e));
        }
    }

    private void showTopics() {
        if (topics.isEmpty()) {
            print("You are not subscribed to any topic in this session.");
            return;
        }
        StringBuilder text = new StringBuilder("Topics:");
        for (String topic : topics) {
            text.append(System.lineSeparator()).append(" - ").append(topic);
        }
        print(text.toString());
    }

    private void ping() {
        try {
            PingReply reply = client.ping();
            print("[BROKER] PONG (broker time " + reply.getServerTimeUtc() + ", storage " + reply.getStorageMode() + ")");
        } catch (StatusRuntimeException e) {
            print("Ping error: " + BrokerClient.describe(e));
        }
    }

    private void toggleAutoAck() {
        boolean enabled = processor.toggleAutoAck();
        print(enabled
                ? "Auto-ACK is now ON"
                : "Auto-ACK is now OFF (messages are not acknowledged; the Broker retries, then moves them to the DLQ)");
    }

    private String readTopic() {
        System.out.print("Topic: ");
        if (!scanner.hasNextLine()) {
            return null;
        }
        String topic = scanner.nextLine().trim();
        if (topic.isEmpty()) {
            print("Topic cannot be empty.");
            return null;
        }
        return topic;
    }

    private static synchronized void print(String text) {
        System.out.println();
        System.out.println(text);
    }

    private static int parsePort(String[] args) {
        if (args.length < 2) {
            return DEFAULT_PORT;
        }
        try {
            return Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid port '" + args[1] + "', using default " + DEFAULT_PORT);
            return DEFAULT_PORT;
        }
    }

    private static String defaultSubscriberId() {
        try {
            String host = InetAddress.getLocalHost().getHostName()
                    .toLowerCase(Locale.ROOT)
                    .replaceAll("[^a-z0-9._-]", "-");
            String id = "java-" + host;
            return id.length() > MAX_ID_LENGTH ? id.substring(0, MAX_ID_LENGTH) : id;
        } catch (UnknownHostException e) {
            return "java-subscriber-1";
        }
    }
}
