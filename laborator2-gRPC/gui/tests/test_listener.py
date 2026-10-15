import queue
import threading
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


END = object()


class FakeCall:
    def __init__(self):
        self.items = queue.Queue()
        self.cancelled = threading.Event()
        self.advanced = threading.Event()
        self._yielded = False

    def push(self, item):
        self.items.put(item)

    def __iter__(self):
        return self

    def __next__(self):
        if self._yielded:
            self.advanced.set()
        item = self.items.get(timeout=5)
        if item is END:
            raise StopIteration
        if isinstance(item, Exception):
            raise item
        self._yielded = True
        return item

    def cancel(self):
        self.cancelled.set()


class FakeGateway:
    def __init__(self, *calls):
        self.calls = list(calls)
        self.requests = []

    def listen(self, subscriber_id):
        self.requests.append(subscriber_id)
        return self.calls.pop(0)


class GatedBus(EventBus):
    def __init__(self):
        super().__init__()
        self.entered = threading.Event()
        self.gate = threading.Event()

    def post(self, kind, payload=None):
        self.entered.set()
        self.gate.wait(5)
        super().post(kind, payload)


class ListenerRaceTest(unittest.TestCase):
    def make(self, *calls):
        for call in calls:
            self.addCleanup(call.push, END)
        gateway = FakeGateway(*calls)
        bus = GatedBus()
        bus.gate.set()
        listener = Listener(gateway, bus)
        self.addCleanup(listener.stop)
        return gateway, bus, listener

    def test_stop_waits_for_a_post_already_in_flight(self):
        call = FakeCall()
        _, bus, listener = self.make(call)
        bus.gate.clear()
        listener.start("s1")
        call.push(delivered("m1"))
        self.assertTrue(bus.entered.wait(3))
        stopper = threading.Thread(target=listener.stop)
        stopper.start()
        stopper.join(0.3)
        self.assertTrue(stopper.is_alive())
        bus.gate.set()
        stopper.join(3)
        self.assertFalse(stopper.is_alive())
        self.assertTrue(call.cancelled.is_set())
        self.assertEqual(["m1"], [payload.message_id for _, payload in bus.drain()])

    def test_restart_waits_for_a_post_already_in_flight(self):
        call1, call2 = FakeCall(), FakeCall()
        _, bus, listener = self.make(call1, call2)
        bus.gate.clear()
        listener.start("s1")
        call1.push(delivered("m1"))
        self.assertTrue(bus.entered.wait(3))
        restarter = threading.Thread(target=listener.start, args=("s2",))
        restarter.start()
        restarter.join(0.3)
        self.assertTrue(restarter.is_alive())
        bus.gate.set()
        restarter.join(3)
        self.assertFalse(restarter.is_alive())

    def test_old_stream_ending_does_not_wipe_the_new_stream(self):
        call1, call2 = FakeCall(), FakeCall()
        gateway, bus, listener = self.make(call1, call2)
        listener.start("s1")
        listener.start("s2")
        self.assertTrue(call1.cancelled.is_set())
        call1.push(END)
        call2.push(delivered("m2"))
        events = wait_for(bus, "message")
        self.assertEqual(["m2"], [payload.message_id for kind, payload in events if kind == "message"])
        listener.stop()
        self.assertTrue(call2.cancelled.is_set())

    def test_old_stream_messages_are_dropped(self):
        call1, call2 = FakeCall(), FakeCall()
        gateway, bus, listener = self.make(call1, call2)
        before = set(threading.enumerate())
        listener.start("s1")
        old_threads = set(threading.enumerate()) - before
        listener.start("s2")
        call1.push(delivered("stale"))
        for thread in old_threads:
            thread.join(3)
            self.assertFalse(thread.is_alive())
        call2.push(delivered("fresh"))
        events = wait_for(bus, "message")
        self.assertEqual(["s1", "s2"], gateway.requests)
        self.assertEqual(["fresh"], [payload.message_id for kind, payload in events if kind == "message"])

    def test_unexpected_exception_posts_internal_closure(self):
        call = FakeCall()
        _, bus, listener = self.make(call)
        listener.start("s1")
        call.push(RuntimeError("boom"))
        events = wait_for(bus, "listen_closed")
        closed = [payload for kind, payload in events if kind == "listen_closed"][0]
        self.assertEqual(ListenClosed("INTERNAL", "boom"), closed)


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
        deadline = time.monotonic() + 3
        while len(self.fake.servicer.listen_requests) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.5)
        self.assertEqual(["s1", "s2"], self.fake.servicer.listen_requests)
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

    def test_replaced_requires_aborted_code_and_details(self):
        self.assertTrue(ListenClosed("ABORTED", "SESSION_REPLACED").replaced)
        self.assertFalse(ListenClosed("INTERNAL", "SESSION_REPLACED").replaced)
        self.assertFalse(ListenClosed("ABORTED", "other").replaced)


if __name__ == "__main__":
    unittest.main()
