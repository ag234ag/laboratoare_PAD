import argparse
import socket
import json
import uuid


def parse_args():
    parser = argparse.ArgumentParser(description="Publisher client")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host/IP (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=5000, help="Broker port (default: 5000)")
    return parser.parse_args()


def send_json(writer, data):
    """
    Serializes the Python object to JSON and sends it to the Broker.
    Each message ends with \n because the protocol uses JSON Lines.
    """
    message = json.dumps(data, ensure_ascii=False)
    writer.write(message + "\n")
    writer.flush()

def main():
    args = parse_args()
    HOST = args.host
    PORT = args.port

    print("=" * 50)
    print("          PUBLISHER - PYTHON")
    print("=" * 50)

    try:
        # Create a TCP socket
        client_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        # Connect to the Broker
        client_socket.connect((HOST, PORT))

        print(f"Connected to Broker: {HOST}:{PORT}")
        print()

        # Use makefile so we can easily work
        # with messages terminated by \n
        reader = client_socket.makefile(
            "r",
            encoding="utf-8"
        )

        writer = client_socket.makefile(
            "w",
            encoding="utf-8"
        )

        while True:
            print()
            print("------------------------------------------")
            print("1 - Publish message")
            print("2 - Send invalid JSON (DLQ test)")
            print("0 - Exit")
            print("------------------------------------------")

            option = input("Choose an option: ").strip()

            # =====================================================
            # PUBLISH
            # =====================================================

            if option == "1":
                topic = input("Topic: ").strip()
                content = input("Message: ").strip()

                if not topic:
                    print("Error: topic cannot be empty.")
                    continue

                if not content:
                    print("Error: message cannot be empty.")
                    continue

                # Unique ID for each message.
                message_id = str(uuid.uuid4())

                message = {
                    "action": "publish",
                    "messageId": message_id,
                    "topic": topic,
                    "content": content
                }

                print()
                print("Sending to Broker:")
                print(
                    json.dumps(
                        message,
                        ensure_ascii=False,
                        indent=4
                    )
                )

                send_json(writer, message)

                # Wait for the Broker response.
                response_line = reader.readline()

                if not response_line:
                    print("The Broker closed the connection.")
                    break

                try:
                    response = json.loads(response_line)

                    print()
                    print("Broker response:")
                    print(
                        json.dumps(
                            response,
                            ensure_ascii=False,
                            indent=4
                        )
                    )

                    if response.get("action") == "publish_accepted":
                        subscribers = response.get("subscribers", 0)

                        print()
                        print(
                            f"Message {message_id} "
                            "was accepted."
                        )

                        if subscribers == 0:
                            print(
                                "There are currently no subscribers "
                                "for this topic."
                            )
                            print(
                                "The message remains persistent "
                                "in the Broker."
                            )
                        else:
                            print(
                                f"Subscribers found: {subscribers}"
                            )

                except json.JSONDecodeError:
                    print(
                        "Invalid response received "
                        "from the Broker:"
                    )
                    print(response_line)

            # =====================================================
            # INVALID JSON TEST
            # =====================================================

            elif option == "2":
                invalid_message = (
                    '{"action":"publish",'
                    '"topic":"sport",'
                    '"content":'
                )

                print()
                print("Intentionally sending invalid JSON:")
                print(invalid_message)

                writer.write(invalid_message + "\n")
                writer.flush()

                response_line = reader.readline()

                if not response_line:
                    print("The Broker closed the connection.")
                    break

                print()
                print("Broker response:")
                print(response_line.strip())

            # =====================================================
            # EXIT
            # =====================================================

            elif option == "0":
                print("Publisher closed.")
                break

            else:
                print("Invalid option.")

        writer.close()
        reader.close()
        client_socket.close()

    except ConnectionRefusedError:
        print()
        print(
            "Unable to connect to the Broker."
        )
        print(
            "Check whether the Broker is running "
            f"on {HOST}:{PORT}."
        )

    except Exception as error:
        print()
        print(
            f"Publisher error: {error}"
        )

if __name__ == "__main__":
    main()