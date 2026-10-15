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
