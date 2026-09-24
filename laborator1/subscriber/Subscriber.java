import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Subscriber {

    private static String HOST = "127.0.0.1";
    private static int PORT = 5000;

    private static Socket socket;
    private static BufferedReader reader;
    private static BufferedWriter writer;

    private static final Set<String> topics =
            ConcurrentHashMap.newKeySet();

    private static final Set<String> processedMessageIds =
            ConcurrentHashMap.newKeySet();

    private static String subscriberId;

    private static volatile boolean running = true;

    public static void main(String[] args) {

        if (args.length >= 1) {
            HOST = args[0];
        }

        if (args.length >= 2) {
            try {
                PORT = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid port '" + args[1] + "', using default " + PORT);
            }
        }

        System.out.println(" ------ SUBSCRIBER - JAVA ----");

        Scanner scanner = new Scanner(System.in);

        try {
            // =====================================================
            // SUBSCRIBER ID
            // =====================================================

            System.out.print("Subscriber ID: ");

            subscriberId = scanner.nextLine().trim();

            if (subscriberId.isEmpty()) {
                subscriberId = "java-subscriber-1";
            }

            // =====================================================
            // CONNECT TO BROKER
            // =====================================================

            socket = new Socket(HOST, PORT);

            reader = new BufferedReader(
                    new InputStreamReader(
                            socket.getInputStream(),
                            StandardCharsets.UTF_8
                    )
            );

            writer = new BufferedWriter(
                    new OutputStreamWriter(
                            socket.getOutputStream(),
                            StandardCharsets.UTF_8
                    )
            );

            System.out.println();
            System.out.println(
                    "Connected to Broker: "
                            + HOST
                            + ":"
                            + PORT
            );

            // =====================================================
            // MESSAGE RECEIVER THREAD
            // =====================================================

            Thread receiverThread =
                    new Thread(Subscriber::receiveMessages);

            receiverThread.setDaemon(true);
            receiverThread.start();

            // =====================================================
            // COMMAND LOOP
            // =====================================================

            while (running) {
                printMenu();

                String option = scanner.nextLine().trim();

                switch (option) {
                    case "1":
                        subscribe(scanner);
                        break;

                    case "2":
                        unsubscribe(scanner);
                        break;

                    case "3":
                        showTopics();
                        break;

                    case "4":
                        sendPing();
                        break;

                    case "0":
                        running = false;
                        break;

                    default:
                        System.out.println("Invalid option.");
                }
            }

        } catch (ConnectException e) {
            System.out.println();
            System.out.println(
                    "Unable to connect to the Broker."
            );

            System.out.println(
                    "Check whether the Broker is running on "
                            + HOST
                            + ":"
                            + PORT
            );

        } catch (IOException e) {
            System.out.println(
                    "Network error: "
                            + e.getMessage()
            );

        } finally {
            closeConnection();
            scanner.close();
        }
    }

    // =============================================================
    // MENU
    // =============================================================

    private static void printMenu() {

        System.out.println();
        System.out.println("------------------------------------------");
        System.out.println("1 - Subscribe to a topic");
        System.out.println("2 - Unsubscribe from a topic");
        System.out.println("3 - Show subscribed topics");
        System.out.println("4 - Ping Broker");
        System.out.println("0 - Exit");
        System.out.println("------------------------------------------");
        System.out.print("Choose an option: ");
    }

    // =============================================================
    // SUBSCRIBE
    // =============================================================

    private static void subscribe(Scanner scanner) {

        try {
            System.out.print("Topic: ");

            String topic = scanner.nextLine().trim();

            if (topic.isEmpty()) {
                System.out.println(
                        "Topic cannot be empty."
                );
                return;
            }

            String json =
                    "{"
                            + "\"action\":\"subscribe\","
                            + "\"subscriberId\":\""
                            + escapeJson(subscriberId)
                            + "\","
                            + "\"topic\":\""
                            + escapeJson(topic)
                            + "\""
                            + "}";

            sendJson(json);

            topics.add(topic);

            System.out.println(
                    "SUBSCRIBE request sent: "
                            + topic
            );

        } catch (IOException e) {
            System.out.println(
                    "Subscribe error: "
                            + e.getMessage()
            );
        }
    }

    // =============================================================
    // UNSUBSCRIBE
    // =============================================================

    private static void unsubscribe(Scanner scanner) {

        try {
            System.out.print("Topic: ");

            String topic = scanner.nextLine().trim();

            if (topic.isEmpty()) {
                System.out.println(
                        "Topic cannot be empty."
                );
                return;
            }

            String json =
                    "{"
                            + "\"action\":\"unsubscribe\","
                            + "\"subscriberId\":\""
                            + escapeJson(subscriberId)
                            + "\","
                            + "\"topic\":\""
                            + escapeJson(topic)
                            + "\""
                            + "}";

            sendJson(json);

            topics.remove(topic);

            System.out.println(
                    "UNSUBSCRIBE request sent: "
                            + topic
            );

        } catch (IOException e) {
            System.out.println(
                    "Unsubscribe error: "
                            + e.getMessage()
            );
        }
    }

    // =============================================================
    // SHOW TOPICS
    // =============================================================

    private static void showTopics() {

        System.out.println();

        if (topics.isEmpty()) {
            System.out.println(
                    "You are not subscribed to any topic."
            );
            return;
        }

        System.out.println("Topics:");

        for (String topic : topics) {
            System.out.println(
                    " - " + topic
            );
        }
    }

    // =============================================================
    // PING
    // =============================================================

    private static void sendPing() {

        try {
            sendJson(
                    "{\"action\":\"ping\"}"
            );

            System.out.println(
                    "PING sent."
            );

        } catch (IOException e) {
            System.out.println(
                    "PING error: "
                            + e.getMessage()
            );
        }
    }

    // =============================================================
    // RECEIVE LOOP
    // =============================================================

    private static void receiveMessages() {

        try {
            String line;

            while (
                    running
                            &&
                    (line = reader.readLine()) != null
            ) {
                handleBrokerMessage(line);
            }

        } catch (IOException e) {
            if (running) {
                System.out.println();
                System.out.println(
                        "The connection to the Broker "
                                + "was interrupted."
                );
            }

        } finally {
            running = false;
        }
    }

    // =============================================================
    // HANDLE MESSAGE FROM BROKER
    // =============================================================

    private static void handleBrokerMessage(String json) {

        String action =
                getJsonString(
                        json,
                        "action"
                );

        if (action == null) {
            System.out.println();
            System.out.println(
                    "[UNKNOWN MESSAGE]"
            );
            System.out.println(json);
            return;
        }

        switch (action) {

            // =====================================================
            // MESSAGE
            // =====================================================

            case "message":

                String messageId =
                        getJsonString(
                                json,
                                "messageId"
                        );

                String topic =
                        getJsonString(
                                json,
                                "topic"
                        );

                String content =
                        getJsonString(
                                json,
                                "content"
                        );

                String attempt =
                        getJsonNumber(
                                json,
                                "attempt"
                        );

                if (messageId != null
                        && !processedMessageIds.add(messageId)) {
                    System.out.println();
                    System.out.println(
                            "[DUPLICATE] Message "
                                    + messageId
                                    + " already processed (attempt "
                                    + attempt
                                    + "), re-sending ACK."
                    );
                    sendAck(messageId);
                    break;
                }

                System.out.println();
                System.out.println(
                        "=========================================="
                );
                System.out.println(
                        "NEW MESSAGE"
                );
                System.out.println(
                        "Message ID : "
                                + messageId
                );
                System.out.println(
                        "Topic      : "
                                + topic
                );
                System.out.println(
                        "Content    : "
                                + content
                );
                System.out.println(
                        "Attempt    : "
                                + attempt
                );
                System.out.println(
                        "=========================================="
                );

                // After the message has been processed,
                // send the ACK.
                if (messageId != null) {
                    sendAck(messageId);
                }

                break;

            // =====================================================
            // SUBSCRIBED
            // =====================================================

            case "subscribed":

                String subscribedTopic =
                        getJsonString(
                                json,
                                "topic"
                        );

                System.out.println();
                System.out.println(
                        "[BROKER] Subscription confirmed: "
                                + subscribedTopic
                );

                break;

            // =====================================================
            // UNSUBSCRIBED
            // =====================================================

            case "unsubscribed":

                String unsubscribedTopic =
                        getJsonString(
                                json,
                                "topic"
                        );

                System.out.println();
                System.out.println(
                        "[BROKER] Unsubscription confirmed: "
                                + unsubscribedTopic
                );

                break;

            // =====================================================
            // ACK RECEIVED
            // =====================================================

            case "ack_received":

                String ackId =
                        getJsonString(
                                json,
                                "messageId"
                        );

                System.out.println();
                System.out.println(
                        (json.contains("\"duplicate\":true")
                                ? "[BROKER] Duplicate ACK ignored for: "
                                : "[BROKER] ACK confirmed for: ")
                                + ackId
                );

                break;

            // =====================================================
            // PONG
            // =====================================================

            case "pong":

                System.out.println();
                System.out.println(
                        "[BROKER] PONG"
                );

                break;

            // =====================================================
            // ERROR
            // =====================================================

            case "error":

                String reason =
                        getJsonString(
                                json,
                                "reason"
                        );

                System.out.println();
                System.out.println(
                        "[BROKER ERROR] "
                                + reason
                );

                break;

            default:

                System.out.println();
                System.out.println(
                        "[BROKER] " + json
                );

                break;
        }
    }

    // =============================================================
    // ACK
    // =============================================================

    private static void sendAck(String messageId) {

        try {
            String json =
                    "{"
                            + "\"action\":\"ack\","
                            + "\"messageId\":\""
                            + escapeJson(messageId)
                            + "\""
                            + "}";

            sendJson(json);

            System.out.println(
                    "[ACK] Sent for message: "
                            + messageId
            );

        } catch (IOException e) {
            System.out.println(
                    "Error sending ACK: "
                            + e.getMessage()
            );
        }
    }

    // =============================================================
    // SEND JSON
    // =============================================================

    private static synchronized void sendJson(
            String json
    ) throws IOException {

        writer.write(json);
        writer.write("\n");
        writer.flush();
    }

    // =============================================================
    // SIMPLE JSON STRING EXTRACTION
    // WITHOUT EXTERNAL LIBRARIES
    // =============================================================

    private static String getJsonString(
            String json,
            String key
    ) {

        String search =
                "\"" + key + "\"";

        int keyIndex =
                json.indexOf(search);

        if (keyIndex == -1) {
            return null;
        }

        int colon =
                json.indexOf(
                        ':',
                        keyIndex + search.length()
                );

        if (colon == -1) {
            return null;
        }

        int firstQuote =
                json.indexOf(
                        '"',
                        colon + 1
                );

        if (firstQuote == -1) {
            return null;
        }

        StringBuilder result =
                new StringBuilder();

        boolean escaped = false;

        for (
                int i = firstQuote + 1;
                i < json.length();
                i++
        ) {

            char c =
                    json.charAt(i);

            if (escaped) {

                switch (c) {
                    case 'n':
                        result.append('\n');
                        break;

                    case 'r':
                        result.append('\r');
                        break;

                    case 't':
                        result.append('\t');
                        break;

                    case '"':
                        result.append('"');
                        break;

                    case '\\':
                        result.append('\\');
                        break;

                    case '/':
                        result.append('/');
                        break;

                    case 'b':
                        result.append('\b');
                        break;

                    case 'f':
                        result.append('\f');
                        break;

                    case 'u':
                        if (i + 4 >= json.length()) {
                            return null;
                        }

                        try {
                            result.append(
                                    (char) Integer.parseInt(
                                            json.substring(i + 1, i + 5),
                                            16
                                    )
                            );
                        } catch (NumberFormatException e) {
                            return null;
                        }

                        i += 4;
                        break;

                    default:
                        result.append(c);
                        break;
                }

                escaped = false;

            } else if (c == '\\') {
                escaped = true;

            } else if (c == '"') {
                return result.toString();

            } else {
                result.append(c);
            }
        }

        return null;
    }

    // =============================================================
    // NUMBER EXTRACTION
    // =============================================================

    private static String getJsonNumber(
            String json,
            String key
    ) {

        String search =
                "\"" + key + "\"";

        int keyIndex =
                json.indexOf(search);

        if (keyIndex == -1) {
            return null;
        }

        int colon =
                json.indexOf(
                        ':',
                        keyIndex + search.length()
                );

        if (colon == -1) {
            return null;
        }

        int start =
                colon + 1;

        while (
                start < json.length()
                        &&
                Character.isWhitespace(
                        json.charAt(start)
                )
        ) {
            start++;
        }

        int end =
                start;

        while (
                end < json.length()
                        &&
                (
                        Character.isDigit(
                                json.charAt(end)
                        )
                                ||
                        json.charAt(end) == '-'
                )
        ) {
            end++;
        }

        if (start == end) {
            return null;
        }

        return json.substring(
                start,
                end
        );
    }

    // =============================================================
    // JSON ESCAPING
    // =============================================================

    private static String escapeJson(
            String value
    ) {

        return value
                .replace(
                        "\\",
                        "\\\\"
                )
                .replace(
                        "\"",
                        "\\\""
                )
                .replace(
                        "\n",
                        "\\n"
                )
                .replace(
                        "\r",
                        "\\r"
                )
                .replace(
                        "\t",
                        "\\t"
                );
    }

    // =============================================================
    // CLOSE CONNECTION
    // =============================================================

    private static void closeConnection() {

        running = false;

        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException ignored) {
        }

        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException ignored) {
        }

        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
        }

        System.out.println();
        System.out.println(
                "Subscriber closed."
        );
    }
}