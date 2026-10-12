import argparse
import uuid

import grpc

import stubs

broker_pb2, broker_pb2_grpc = stubs.load()

CALL_TIMEOUT_SECONDS = 5
MAX_NAME_LENGTH = 64
MAX_CONTENT_LENGTH = 4096

MENU = """
------------------------------------------
1 - Publish message
2 - DLQ test: publish without topic
3 - DLQ test: publish without content
4 - Idempotency test: re-send last message
5 - Ping Broker
0 - Exit
------------------------------------------"""


def parse_args():
    parser = argparse.ArgumentParser(description="Publisher client (gRPC)")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host/IP (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=50051, help="Broker port (default: 50051)")
    return parser.parse_args()


def validate_topic(topic):
    if not topic:
        return "topic cannot be empty"
    if len(topic) > MAX_NAME_LENGTH:
        return f"topic is longer than {MAX_NAME_LENGTH} characters"
    if not all(ch.isalnum() or ch in "._-" for ch in topic):
        return "topic may contain only letters, digits, '.', '_' and '-'"
    return None


def validate_content(content):
    if not content.strip():
        return "message cannot be empty"
    if len(content) > MAX_CONTENT_LENGTH:
        return f"message is longer than {MAX_CONTENT_LENGTH} characters"
    return None


class BrokerGateway:
    def __init__(self, host, port):
        self._channel = grpc.insecure_channel(f"{host}:{port}")
        self._stub = broker_pb2_grpc.MessageBrokerStub(self._channel)

    def publish(self, message_id, topic, content):
        request = broker_pb2.PublishRequest(message_id=message_id, topic=topic, content=content)
        return self._stub.Publish(request, timeout=CALL_TIMEOUT_SECONDS)

    def ping(self):
        return self._stub.Ping(broker_pb2.PingRequest(), timeout=CALL_TIMEOUT_SECONDS)

    def close(self):
        self._channel.close()


def print_rpc_error(error):
    if error.code() == grpc.StatusCode.UNAVAILABLE:
        print("Broker unavailable. Check the host/IP, the port, the firewall and that the Broker is running.")
    else:
        print(f"Broker error: {error.code().name} - {error.details()}")


def send(gateway, message_id, topic, content):
    print()
    print(f"Sending: message_id={message_id!r} topic={topic!r} content={content!r}")
    try:
        reply = gateway.publish(message_id, topic, content)
    except grpc.RpcError as error:
        print_rpc_error(error)
        return
    print(f"Accepted: {reply.message_id} on topic '{reply.topic}'")
    if reply.subscribers == 0:
        print("There are currently no subscribers for this topic.")
        print("The message remains stored in the Broker.")
    else:
        print(f"Subscribers found: {reply.subscribers}")


def publish_interactive(gateway):
    topic = input("Topic: ").strip()
    content = input("Message: ").strip()
    error = validate_topic(topic) or validate_content(content)
    if error:
        print(f"Error: {error}.")
        return None
    message = (str(uuid.uuid4()), topic, content)
    send(gateway, *message)
    return message


def ping(gateway):
    try:
        reply = gateway.ping()
    except grpc.RpcError as error:
        print_rpc_error(error)
        return
    print(f"PONG - broker time {reply.server_time_utc}, storage {reply.storage_mode}")


def main():
    args = parse_args()
    print("=" * 50)
    print("        PUBLISHER - PYTHON (gRPC)")
    print("=" * 50)
    print(f"Broker: {args.host}:{args.port}")

    gateway = BrokerGateway(args.host, args.port)
    last_message = None
    try:
        while True:
            print(MENU)
            option = input("Choose an option: ").strip()
            if option == "1":
                last_message = publish_interactive(gateway) or last_message
            elif option == "2":
                send(gateway, str(uuid.uuid4()), "", "message without topic")
            elif option == "3":
                send(gateway, str(uuid.uuid4()), "dlq-test", "")
            elif option == "4":
                if last_message is None:
                    print("Publish a message first (option 1).")
                else:
                    send(gateway, *last_message)
            elif option == "5":
                ping(gateway)
            elif option == "0":
                print("Publisher closed.")
                break
            else:
                print("Invalid option.")
    except (EOFError, KeyboardInterrupt):
        print()
        print("Publisher closed.")
    finally:
        gateway.close()


if __name__ == "__main__":
    main()
