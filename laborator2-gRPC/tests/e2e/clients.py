import sys
import threading
import time
import uuid
from pathlib import Path

import grpc

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "publisher"))

import stubs

broker_pb2, broker_pb2_grpc = stubs.load()

CALL_TIMEOUT = 5


def new_id():
    return str(uuid.uuid4())


def ids(messages):
    return [message.message_id for message in messages]


def wait_until(predicate, timeout=15, message="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError(f"{message} not met within {timeout}s")


class BrokerApi:
    def __init__(self, address):
        self._channel = grpc.insecure_channel(address)
        self.stub = broker_pb2_grpc.MessageBrokerStub(self._channel)

    def publish(self, message_id, topic, content):
        request = broker_pb2.PublishRequest(message_id=message_id, topic=topic, content=content)
        return self.stub.Publish(request, timeout=CALL_TIMEOUT)

    def subscribe(self, subscriber_id, topic):
        request = broker_pb2.SubscriptionRequest(subscriber_id=subscriber_id, topic=topic)
        return self.stub.Subscribe(request, timeout=CALL_TIMEOUT)

    def unsubscribe(self, subscriber_id, topic):
        request = broker_pb2.SubscriptionRequest(subscriber_id=subscriber_id, topic=topic)
        return self.stub.Unsubscribe(request, timeout=CALL_TIMEOUT)

    def ack(self, subscriber_id, message_id):
        request = broker_pb2.AckRequest(subscriber_id=subscriber_id, message_id=message_id)
        return self.stub.Ack(request, timeout=CALL_TIMEOUT)

    def ping(self):
        return self.stub.Ping(broker_pb2.PingRequest(), timeout=CALL_TIMEOUT)

    def state(self, limit=1000):
        return self.stub.GetState(broker_pb2.StateRequest(limit=limit), timeout=CALL_TIMEOUT)

    def close(self):
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


class ListeningSubscriber:
    def __init__(self, address, subscriber_id, auto_ack=True):
        self.subscriber_id = subscriber_id
        self.auto_ack = auto_ack
        self.api = BrokerApi(address)
        self.error = None
        self.ack_errors = []
        self._received = []
        self._lock = threading.Lock()
        self._call = self.api.stub.Listen(broker_pb2.ListenRequest(subscriber_id=subscriber_id))
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        try:
            for message in self._call:
                with self._lock:
                    self._received.append((message, time.monotonic()))
                if self.auto_ack:
                    try:
                        self.api.ack(self.subscriber_id, message.message_id)
                    except grpc.RpcError as error:
                        self.ack_errors.append(error)
        except grpc.RpcError as error:
            self.error = error

    def messages(self):
        with self._lock:
            return [message for message, _ in self._received]

    def receive_times(self, message_id):
        with self._lock:
            return [received_at for message, received_at in self._received if message.message_id == message_id]

    def wait_for(self, predicate, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            current = self.messages()
            if predicate(current):
                return current
            time.sleep(0.1)
        raise AssertionError(
            f"{self.subscriber_id}: condition not met within {timeout}s; received {ids(self.messages())}")

    def wait_until_closed(self, timeout=10):
        self._thread.join(timeout)
        return self.error

    def close(self):
        self._call.cancel()
        self._thread.join(5)
        self.api.close()
