import grpc

from core import stubs

broker_pb2, broker_pb2_grpc = stubs.load()

CALL_TIMEOUT_SECONDS = 5
DEFAULT_STATE_LIMIT = 100
UNAVAILABLE_TEXT = "Broker indisponibil. Verifica host/IP, portul, firewall-ul si ca brokerul ruleaza."
DEADLINE_TEXT = "Brokerul nu a raspuns in 5 secunde. Verifica host/IP, portul, firewall-ul si ca brokerul ruleaza."


def format_target(host, port):
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"{host}:{port}"


def describe_error(error):
    if not isinstance(error, grpc.RpcError):
        return str(error)
    if error.code() == grpc.StatusCode.UNAVAILABLE:
        return UNAVAILABLE_TEXT
    if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        return DEADLINE_TEXT
    return f"{error.code().name} - {error.details()}"


class BrokerGateway:
    def __init__(self, host, port):
        self._channel = grpc.insecure_channel(format_target(host, port))
        self._stub = broker_pb2_grpc.MessageBrokerStub(self._channel)

    def publish(self, message_id, topic, content):
        request = broker_pb2.PublishRequest(message_id=message_id, topic=topic, content=content)
        return self._stub.Publish(request, timeout=CALL_TIMEOUT_SECONDS)

    def subscribe(self, subscriber_id, topic):
        request = broker_pb2.SubscriptionRequest(subscriber_id=subscriber_id, topic=topic)
        return self._stub.Subscribe(request, timeout=CALL_TIMEOUT_SECONDS)

    def unsubscribe(self, subscriber_id, topic):
        request = broker_pb2.SubscriptionRequest(subscriber_id=subscriber_id, topic=topic)
        return self._stub.Unsubscribe(request, timeout=CALL_TIMEOUT_SECONDS)

    def ack(self, subscriber_id, message_id):
        request = broker_pb2.AckRequest(subscriber_id=subscriber_id, message_id=message_id)
        return self._stub.Ack(request, timeout=CALL_TIMEOUT_SECONDS)

    def ping(self):
        return self._stub.Ping(broker_pb2.PingRequest(), timeout=CALL_TIMEOUT_SECONDS)

    def get_state(self, limit=DEFAULT_STATE_LIMIT):
        return self._stub.GetState(broker_pb2.StateRequest(limit=limit), timeout=CALL_TIMEOUT_SECONDS)

    def listen(self, subscriber_id):
        return self._stub.Listen(broker_pb2.ListenRequest(subscriber_id=subscriber_id))

    def close(self):
        self._channel.close()
