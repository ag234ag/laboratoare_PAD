import threading
import time
from concurrent import futures

import grpc

from core.gateway import broker_pb2, broker_pb2_grpc


class FakeBroker(broker_pb2_grpc.MessageBrokerServicer):
    def __init__(self):
        self.published = []
        self.acks = []
        self.subscriptions = []
        self.listen_requests = []
        self.listen_messages = []
        self.reject_with = None
        self.reject_subscribe_with = None
        self.abort_listen_with = None
        self.finish_listen = threading.Event()
        self.state = broker_pb2.BrokerState(storage_mode="transient")

    def Ping(self, request, context):
        return broker_pb2.PingReply(server_time_utc="2026-10-15T00:00:00Z", storage_mode="transient")

    def Publish(self, request, context):
        if self.reject_with is not None:
            context.abort(*self.reject_with)
        self.published.append(request)
        return broker_pb2.PublishReply(message_id=request.message_id, topic=request.topic, subscribers=2)

    def Subscribe(self, request, context):
        if self.reject_subscribe_with is not None:
            context.abort(*self.reject_subscribe_with)
        self.subscriptions.append(("subscribe", request.subscriber_id, request.topic))
        return broker_pb2.SubscriptionReply(subscriber_id=request.subscriber_id, topic=request.topic)

    def Unsubscribe(self, request, context):
        self.subscriptions.append(("unsubscribe", request.subscriber_id, request.topic))
        return broker_pb2.SubscriptionReply(subscriber_id=request.subscriber_id, topic=request.topic)

    def Ack(self, request, context):
        self.acks.append((request.subscriber_id, request.message_id))
        return broker_pb2.AckReply(message_id=request.message_id)

    def GetState(self, request, context):
        return self.state

    def Listen(self, request, context):
        self.listen_requests.append(request.subscriber_id)
        for message in self.listen_messages:
            yield message
        if self.abort_listen_with is not None:
            context.abort(*self.abort_listen_with)
        while context.is_active() and not self.finish_listen.is_set():
            time.sleep(0.02)


class RunningFakeBroker:
    def __init__(self):
        self.servicer = FakeBroker()
        self.port = None
        self._server = None

    def __enter__(self):
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        broker_pb2_grpc.add_MessageBrokerServicer_to_server(self.servicer, self._server)
        self.port = self._server.add_insecure_port("127.0.0.1:0")
        self._server.start()
        return self

    def kill(self):
        self.servicer.finish_listen.set()
        self._server.stop(0)

    def __exit__(self, *exc_info):
        self.kill()
