import threading
import time
import unittest

from core.events import EventBus
from core.gateway import BrokerGateway, broker_pb2
from core.state_poller import StatePoller
from tests.fake_broker import RunningFakeBroker
from tests.support import unused_port


def wait_for(bus, kind, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for item in bus.drain():
            if item[0] == kind:
                return item[1]
        time.sleep(0.01)
    raise AssertionError(f"niciun eveniment {kind}")


class GatedBus(EventBus):
    def __init__(self):
        super().__init__()
        self.entered = threading.Event()
        self.gate = threading.Event()

    def post(self, kind, payload=None):
        self.entered.set()
        self.gate.wait(5)
        super().post(kind, payload)


class StaticGateway:
    def get_state(self, limit):
        return broker_pb2.BrokerState(storage_mode="transient")


class StatePollerRaceTest(unittest.TestCase):
    def test_stop_waits_for_a_post_already_in_flight_and_nothing_follows(self):
        bus = GatedBus()
        poller = StatePoller(StaticGateway(), bus, interval=0.01)
        self.addCleanup(poller.stop)
        poller.start()
        self.assertTrue(bus.entered.wait(3))
        stopper = threading.Thread(target=poller.stop)
        stopper.start()
        stopper.join(0.3)
        self.assertTrue(stopper.is_alive())
        bus.gate.set()
        stopper.join(3)
        self.assertFalse(stopper.is_alive())
        events = bus.drain()
        time.sleep(0.2)
        self.assertEqual(1, len(events))
        self.assertEqual([], bus.drain())


class StatePollerTest(unittest.TestCase):
    def start_fake(self):
        fake = RunningFakeBroker()
        fake.__enter__()
        self.addCleanup(fake.__exit__)
        return fake

    def test_posts_state_repeatedly(self):
        fake = self.start_fake()
        fake.servicer.state = broker_pb2.BrokerState(storage_mode="persistent")
        gateway = BrokerGateway("127.0.0.1", fake.port)
        self.addCleanup(gateway.close)
        bus = EventBus()
        poller = StatePoller(gateway, bus, interval=0.05)
        self.addCleanup(poller.stop)
        poller.start()
        self.assertEqual("persistent", wait_for(bus, "state").storage_mode)
        self.assertEqual("persistent", wait_for(bus, "state").storage_mode)

    def test_errors_are_posted_as_text(self):
        gateway = BrokerGateway("127.0.0.1", unused_port())
        self.addCleanup(gateway.close)
        bus = EventBus()
        poller = StatePoller(gateway, bus, interval=0.05)
        self.addCleanup(poller.stop)
        poller.start()
        self.assertIn("indisponibil", wait_for(bus, "state_error", timeout=8))

    def test_stop_ends_the_events(self):
        fake = self.start_fake()
        gateway = BrokerGateway("127.0.0.1", fake.port)
        self.addCleanup(gateway.close)
        bus = EventBus()
        poller = StatePoller(gateway, bus, interval=0.05)
        poller.start()
        wait_for(bus, "state")
        poller.stop()
        time.sleep(0.4)
        bus.drain()
        time.sleep(0.4)
        self.assertEqual([], bus.drain())


if __name__ == "__main__":
    unittest.main()
