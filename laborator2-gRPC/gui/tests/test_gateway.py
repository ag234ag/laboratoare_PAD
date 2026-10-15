import unittest

import grpc

from core.gateway import UNAVAILABLE_TEXT, BrokerGateway, broker_pb2, describe_error
from tests.fake_broker import RunningFakeBroker
from tests.support import FakeRpcError, unused_port


class GatewayTest(unittest.TestCase):
    def setUp(self):
        self.fake = RunningFakeBroker()
        self.fake.__enter__()
        self.addCleanup(self.fake.__exit__)
        self.gateway = BrokerGateway("127.0.0.1", self.fake.port)
        self.addCleanup(self.gateway.close)

    def test_ping_returns_storage_mode(self):
        self.assertEqual("transient", self.gateway.ping().storage_mode)

    def test_publish_sends_the_request_and_returns_the_reply(self):
        reply = self.gateway.publish("m1", "sport", "salut")
        self.assertEqual(2, reply.subscribers)
        self.assertEqual(("m1", "sport", "salut"), tuple(
            getattr(self.fake.servicer.published[0], field) for field in ("message_id", "topic", "content")))

    def test_publish_rejected_raises_rpc_error(self):
        self.fake.servicer.reject_with = (grpc.StatusCode.INVALID_ARGUMENT, "topic")
        with self.assertRaises(grpc.RpcError) as caught:
            self.gateway.publish("m1", "", "x")
        self.assertEqual(grpc.StatusCode.INVALID_ARGUMENT, caught.exception.code())

    def test_subscribe_unsubscribe_and_ack_reach_the_broker(self):
        self.gateway.subscribe("s1", "sport")
        self.gateway.unsubscribe("s1", "sport")
        reply = self.gateway.ack("s1", "m1")
        self.assertEqual("m1", reply.message_id)
        self.assertEqual([("subscribe", "s1", "sport"), ("unsubscribe", "s1", "sport")], self.fake.servicer.subscriptions)
        self.assertEqual([("s1", "m1")], self.fake.servicer.acks)

    def test_get_state_returns_the_snapshot(self):
        self.fake.servicer.state = broker_pb2.BrokerState(storage_mode="persistent")
        self.assertEqual("persistent", self.gateway.get_state().storage_mode)

    def test_unavailable_broker_raises_unavailable(self):
        gateway = BrokerGateway("127.0.0.1", unused_port())
        self.addCleanup(gateway.close)
        with self.assertRaises(grpc.RpcError) as caught:
            gateway.ping()
        self.assertEqual(grpc.StatusCode.UNAVAILABLE, caught.exception.code())


class DescribeErrorTest(unittest.TestCase):
    def test_unavailable_has_the_help_text(self):
        self.assertEqual(UNAVAILABLE_TEXT, describe_error(FakeRpcError(grpc.StatusCode.UNAVAILABLE)))

    def test_other_codes_show_name_and_details(self):
        self.assertEqual("NOT_FOUND - DELIVERY_NOT_FOUND",
                         describe_error(FakeRpcError(grpc.StatusCode.NOT_FOUND, "DELIVERY_NOT_FOUND")))

    def test_non_grpc_errors_use_their_message(self):
        self.assertEqual("boom", describe_error(RuntimeError("boom")))


if __name__ == "__main__":
    unittest.main()
