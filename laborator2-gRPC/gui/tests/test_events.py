import time
import unittest

from core.events import BackgroundCalls, EventBus, Outcome, TkPump


def collect(bus, count, timeout=2):
    items = []
    deadline = time.monotonic() + timeout
    while len(items) < count and time.monotonic() < deadline:
        items.extend(bus.drain())
        time.sleep(0.01)
    return items


class FakeRoot:
    def __init__(self):
        self.scheduled = []

    def after(self, milliseconds, callback):
        self.scheduled.append((milliseconds, callback))


class EventBusTest(unittest.TestCase):
    def test_drain_returns_events_in_order_and_empties_the_queue(self):
        bus = EventBus()
        bus.post("a", 1)
        bus.post("b")
        self.assertEqual([("a", 1), ("b", None)], bus.drain())
        self.assertEqual([], bus.drain())


class BackgroundCallsTest(unittest.TestCase):
    def test_success_posts_outcome_with_value_and_context(self):
        bus = EventBus()
        BackgroundCalls(bus).submit("sum", lambda a, b: a + b, 2, 3, context="ctx")
        [(kind, outcome)] = collect(bus, 1)
        self.assertEqual("sum", kind)
        self.assertTrue(outcome.ok)
        self.assertEqual(5, outcome.value)
        self.assertEqual("ctx", outcome.context)

    def test_failure_posts_outcome_with_error(self):
        bus = EventBus()

        def boom():
            raise ValueError("nope")

        BackgroundCalls(bus).submit("boom", boom, context=7)
        [(kind, outcome)] = collect(bus, 1)
        self.assertFalse(outcome.ok)
        self.assertIsInstance(outcome.error, ValueError)
        self.assertEqual(7, outcome.context)


class TkPumpTest(unittest.TestCase):
    def test_delivers_events_and_reschedules(self):
        root, bus, seen = FakeRoot(), EventBus(), []
        bus.post("x", 1)
        TkPump(root, bus, lambda kind, payload: seen.append((kind, payload)), interval_ms=100).start()
        self.assertEqual([("x", 1)], seen)
        self.assertEqual(100, root.scheduled[0][0])

    def test_reschedules_even_when_handler_raises(self):
        root, bus = FakeRoot(), EventBus()
        bus.post("x")

        def handler(kind, payload):
            raise RuntimeError("bad")

        with self.assertRaises(RuntimeError):
            TkPump(root, bus, handler).start()
        self.assertEqual(1, len(root.scheduled))


if __name__ == "__main__":
    unittest.main()
