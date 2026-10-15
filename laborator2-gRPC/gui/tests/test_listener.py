import time
import unittest

import grpc

from core.events import EventBus
from core.gateway import UNAVAILABLE_TEXT, BrokerGateway, broker_pb2
from core.listener import ListenClosed, Listener
from tests.fake_broker import RunningFakeBroker
from tests.support import unused_port


def delivered(message_id):
    return broker_pb2.DeliveredMessage(message_id=message_id, topic="t", content="c", attempt=1, max_attempts=3)


def wait_for(bus, kind, count=1, timeout=3):
    items = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and len([k for k, _ in items if k == kind]) < count:
        items.extend(bus.drain())
        time.sleep(0.01)
    return items


class ListenerTest(unittest.TestCase):
    def setUp(self):
        self.fake = RunningFakeBroker()
        self.fake.__enter__()
        self.addCleanup(self.fake.__exit__)
        self.gateway = BrokerGateway("127.0.0.1", self.fake.port)
        self.addCleanup(self.gateway.close)
        self.bus = EventBus()
        self.listener = Listener(self.gateway, self.bus)
        self.addCleanup(self.listener.stop)

    def test_messages_are_posted_in_order(self):
        self.fake.servicer.listen_messages = [delivered("m1"), delivered("m2")]
        self.listener.start("s1")
        events = wait_for(self.bus, "message", count=2)
        self.assertEqual(["m1", "m2"], [payload.message_id for kind, payload in events if kind == "message"])
        self.assertEqual(["s1"], self.fake.servicer.listen_requests)

    def test_stream_closed_by_broker_posts_ok_closure(self):
        self.listener.start("s1")
        self.fake.servicer.finish_listen.set()
        events = wait_for(self.bus, "listen_closed")
        closed = [payload for kind, payload in events if kind == "listen_closed"][0]
        self.assertEqual("OK", closed.code)

    def test_replaced_session_is_detected(self):
        self.fake.servicer.abort_listen_with = (grpc.StatusCode.ABORTED, "SESSION_REPLACED")
        self.listener.start("s1")
        events = wait_for(self.bus, "listen_closed")
        closed = [payload for kind, payload in events if kind == "listen_closed"][0]
        self.assertTrue(closed.replaced)
        self.assertIn("inlocuita", closed.text())

    def test_stop_does_not_post_a_closure(self):
        self.listener.start("s1")
        time.sleep(0.3)
        self.listener.stop()
        time.sleep(0.5)
        self.assertEqual([], [kind for kind, _ in self.bus.drain() if kind == "listen_closed"])

    def test_restart_silences_the_previous_stream(self):
        self.fake.servicer.listen_messages = [delivered("m1")]
        self.listener.start("s1")
        wait_for(self.bus, "message")
        self.listener.start("s2")
        time.sleep(0.5)
        kinds = [kind for kind, _ in self.bus.drain()]
        self.assertEqual(1, kinds.count("message"))
        self.assertNotIn("listen_closed", kinds)

    def test_unavailable_broker_posts_unavailable_closure(self):
        gateway = BrokerGateway("127.0.0.1", unused_port())
        self.addCleanup(gateway.close)
        listener = Listener(gateway, self.bus)
        self.addCleanup(listener.stop)
        listener.start("s1")
        events = wait_for(self.bus, "listen_closed", timeout=8)
        closed = [payload for kind, payload in events if kind == "listen_closed"][0]
        self.assertEqual("UNAVAILABLE", closed.code)
        self.assertEqual(UNAVAILABLE_TEXT, closed.text())


class ListenClosedTest(unittest.TestCase):
    def test_text_for_other_codes(self):
        self.assertEqual("Listen inchis: INTERNAL - x", ListenClosed("INTERNAL", "x").text())


if __name__ == "__main__":
    unittest.main()
