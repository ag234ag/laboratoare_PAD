import time
import types
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
        self.cancelled = []
        self.bindings = []

    def after(self, milliseconds, callback):
        self.scheduled.append((milliseconds, callback))
        return f"after#{len(self.scheduled)}"

    def after_cancel(self, after_id):
        self.cancelled.append(after_id)

    def bind(self, sequence, callback, add=None):
        self.bindings.append((sequence, callback, add))

    def destroy_event(self, widget=None):
        event = types.SimpleNamespace(widget=self if widget is None else widget)
        for _, callback, _ in self.bindings:
            callback(event)


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

    def test_stop_before_start_is_harmless(self):
        root = FakeRoot()
        pump = TkPump(root, EventBus(), lambda kind, payload: None)
        pump.stop()
        pump.start()
        self.assertEqual([], root.scheduled)

    def test_double_stop_is_harmless(self):
        root = FakeRoot()
        pump = TkPump(root, EventBus(), lambda kind, payload: None)
        pump.start()
        pump.stop()
        pump.stop()
        self.assertEqual(1, len(root.cancelled))

    def test_stop_cancels_the_pending_after(self):
        root = FakeRoot()
        pump = TkPump(root, EventBus(), lambda kind, payload: None)
        pump.start()
        pump.stop()
        self.assertEqual(["after#1"], root.cancelled)

    def test_no_rescheduling_after_stop(self):
        root = FakeRoot()
        pump = TkPump(root, EventBus(), lambda kind, payload: None)
        pump.start()
        pump.stop()
        root.scheduled[0][1]()
        self.assertEqual(1, len(root.scheduled))

    def test_stop_from_inside_the_handler_prevents_rescheduling(self):
        root, bus = FakeRoot(), EventBus()
        bus.post("x")
        pumps = []
        pumps.append(TkPump(root, bus, lambda kind, payload: pumps[0].stop()))
        pumps[0].start()
        self.assertEqual([], root.scheduled)

    def test_start_binds_destroy_without_replacing_other_bindings(self):
        root = FakeRoot()
        TkPump(root, EventBus(), lambda kind, payload: None).start()
        [(sequence, _, add)] = root.bindings
        self.assertEqual("<Destroy>", sequence)
        self.assertEqual("+", add)

    def test_destroy_of_the_root_stops_the_pump(self):
        root = FakeRoot()
        TkPump(root, EventBus(), lambda kind, payload: None).start()
        root.destroy_event()
        self.assertEqual(["after#1"], root.cancelled)
        root.scheduled[0][1]()
        self.assertEqual(1, len(root.scheduled))

    def test_destroy_of_another_widget_does_not_stop_the_pump(self):
        root = FakeRoot()
        TkPump(root, EventBus(), lambda kind, payload: None).start()
        root.destroy_event(widget=object())
        self.assertEqual([], root.cancelled)
        root.scheduled[0][1]()
        self.assertEqual(2, len(root.scheduled))


if __name__ == "__main__":
    unittest.main()
