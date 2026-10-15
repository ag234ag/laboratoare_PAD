import time
import tkinter as tk
import unittest
from pathlib import Path

from broker_app import BrokerApp
from core.broker_process import PortInUseError
from core.gateway import broker_pb2
from tests.fake_broker import RunningFakeBroker
from tests.support import make_root, pump, release_on_cleanup


class FakeProcess:
    def __init__(self, bus):
        self.bus = bus
        self.started_with = []
        self.stop_calls = 0
        self.running = False
        self.start_error = None
        self.stop_error = None

    def start(self, options):
        if self.start_error is not None:
            raise self.start_error
        self.started_with.append(options)
        self.running = True
        self.bus.post("console", "Listening on 0.0.0.0")

    def stop(self):
        self.stop_calls += 1
        if self.stop_error is not None:
            raise self.stop_error
        if self.running:
            self.running = False
            self.bus.post("exited", 0)


class BrokerAppTest(unittest.TestCase):
    def setUp(self):
        release_on_cleanup(self, "app", "root")
        self.fake = RunningFakeBroker()
        self.fake.__enter__()
        self.addCleanup(self.fake.__exit__)
        self.fake.servicer.state = broker_pb2.BrokerState(
            storage_mode="transient",
            messages=[broker_pb2.MessageRecord(message_id="m1", topic="t", content="ă", status="PENDING", created_at_utc="x")],
            deliveries=[broker_pb2.DeliveryRecord(message_id="m1", subscriber_id="s1", status="DEAD", retry_count=3, last_error="e")],
            dead_letters=[broker_pb2.DeadLetterRecord(id=1, message_id="m1", subscriber_id="s1", topic="t", reason="r", retry_count=3)],
            subscriptions=[broker_pb2.SubscriptionRecord(subscriber_id="s1", topic="t")],
            online_subscribers=["s1"],
        )
        self.root = make_root(self)
        self.process = None

        def factory(bus):
            self.process = FakeProcess(bus)
            return self.process

        self.app = BrokerApp(self.root, Path("broker"), Path("."), process_factory=factory)
        self.app.port.set(str(self.fake.port))
        self.addCleanup(self.stop_quietly)

    def stop_quietly(self):
        self.process.stop_error = None
        self.app.stop()

    def start_and_wait(self):
        self.app.start()
        pump(self.root, lambda: self.app.status.get() == "Running", message="Running")

    def settle(self, seconds=0.4):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.root.update()
            time.sleep(0.02)

    def test_start_passes_the_fields_to_the_process(self):
        self.app.storage.set("transient")
        self.app.db.set("x.db")
        self.app.workers.set("2")
        self.start_and_wait()
        options = self.process.started_with[0]
        self.assertEqual((str(self.fake.port), "transient", "x.db", "2"),
                         (options.port, options.storage, options.db, options.workers))

    def test_console_lines_are_shown(self):
        self.start_and_wait()
        pump(self.root, lambda: "Listening on 0.0.0.0" in self.app.console.contents(), message="consola")

    def test_state_fills_all_tables_and_the_online_label(self):
        self.start_and_wait()
        pump(self.root, lambda: self.app.messages_table.rows() == [("m1", "t", "ă", "PENDING", "x")], message="mesaje")
        pump(self.root, lambda: self.app.deliveries_table.rows() == [("m1", "s1", "DEAD", "3", "e")], message="livrari")
        pump(self.root, lambda: len(self.app.dead_letters_table.rows()) == 1, message="dlq")
        pump(self.root, lambda: self.app.subscriptions_table.rows() == [("s1", "t")], message="abonamente")
        self.assertEqual("Abonati online: s1", self.app.online.get())

    def test_invalid_fields_do_not_start_the_process(self):
        self.app.workers.set("0")
        self.app.start()
        self.assertEqual([], self.process.started_with)
        self.assertIn("workers", self.app.console.contents())

    def test_second_start_while_running_is_refused(self):
        self.start_and_wait()
        self.app.start()
        self.assertEqual(1, len(self.process.started_with))
        self.assertIn("ruleaza deja", self.app.console.contents())

    def test_port_in_use_is_reported_and_status_stays_stopped(self):
        self.process.start_error = PortInUseError(50051)
        self.app.start()
        self.assertIn("Portul 50051 este ocupat", self.app.console.contents())
        self.assertEqual("Oprit", self.app.status.get())

    def test_missing_dotnet_is_reported(self):
        self.process.start_error = FileNotFoundError("dotnet")
        self.app.start()
        self.assertIn("Nu pot porni", self.app.console.contents())
        self.assertEqual("Oprit", self.app.status.get())

    def test_stop_stops_the_process_and_shows_the_exit_code(self):
        self.start_and_wait()
        self.app.stop()
        pump(self.root, lambda: self.app.status.get() == "Oprit (cod 0)", message="oprit")
        self.assertEqual("normal", str(self.app.start_button.cget("state")))

    def test_close_while_starting_stops_the_process(self):
        self.app.start()
        self.app.close()
        self.assertGreaterEqual(self.process.stop_calls, 1)

    def test_exit_before_ready_is_shown_as_failure(self):
        self.app.port.set("50000")
        self.app.start()
        self.process.running = False
        self.process.bus.post("exited", 1)
        pump(self.root, lambda: self.app.status.get().startswith("Esuat"), timeout=10, message="esuat")
        self.assertEqual("Esuat (cod 1)", self.app.status.get())

    def test_state_error_is_logged_once(self):
        self.start_and_wait()
        self.fake.kill()
        pump(self.root, lambda: "indisponibil" in self.app.console.contents(), timeout=8, message="eroare stare")
        before = self.app.console.contents().count("indisponibil")
        for _ in range(40):
            self.root.update()
        self.assertEqual(before, self.app.console.contents().count("indisponibil"))

    def test_stop_failure_is_logged_and_does_not_raise(self):
        self.start_and_wait()
        self.process.stop_error = RuntimeError("taskkill a esuat cu codul 5: acces refuzat")
        self.app.stop()
        self.assertIn("Eroare la oprire: taskkill a esuat cu codul 5", self.app.console.contents())

    def test_close_destroys_the_window_even_if_stop_fails(self):
        self.start_and_wait()
        self.process.stop_error = RuntimeError("taskkill a esuat")
        self.app.close()
        with self.assertRaises(tk.TclError):
            self.root.winfo_exists()

    def test_no_state_error_is_logged_after_stop(self):
        self.start_and_wait()
        self.app.stop()
        self.fake.kill()
        self.settle(1.5)
        self.assertEqual("Oprit (cod 0)", self.app.status.get())
        self.assertNotIn("Stare:", self.app.console.contents())

    def test_state_event_of_a_dropped_gateway_is_ignored(self):
        self.start_and_wait()
        pump(self.root, lambda: len(self.app.messages_table.rows()) == 1, message="mesaje")
        stale = object()
        self.app._bus.post("state", (stale, broker_pb2.BrokerState(online_subscribers=["x"])))
        self.app._bus.post("state_error", (stale, "vechi"))
        self.settle()
        self.assertEqual("Abonati online: s1", self.app.online.get())
        self.assertNotIn("vechi", self.app.console.contents())

    def test_state_events_after_exit_are_ignored(self):
        self.start_and_wait()
        gateway = self.app._gateway
        self.app.stop()
        pump(self.root, lambda: self.app.status.get() == "Oprit (cod 0)", message="oprit")
        self.app._bus.post("state", (gateway, broker_pb2.BrokerState(online_subscribers=["x"])))
        self.app._bus.post("state_error", (gateway, "tarziu"))
        self.settle()
        self.assertNotIn("tarziu", self.app.console.contents())
        self.assertNotIn("Abonati online: x", self.app.online.get())


if __name__ == "__main__":
    unittest.main()
