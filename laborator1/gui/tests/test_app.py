import tkinter as tk
import unittest

from app import App
from support import FakeClient


class AppTest(unittest.TestCase):
    def setUp(self):
        self.root = tk.Tk()
        self.root.withdraw()
        self.clients = []

        def factory(events):
            client = FakeClient(events=events)
            self.clients.append(client)
            return client

        self.app = App(self.root, db_path="missing.db", client_factory=factory)
        self.addCleanup(self._destroy_root)

    def _destroy_root(self):
        try:
            self.root.destroy()
        except tk.TclError:
            pass

    def test_events_are_dispatched_to_their_own_panel(self):
        publisher_client, subscriber_client = self.clients
        publisher_client.events.put(("message", {"action": "publish_accepted"}))
        subscriber_client.events.put(("message", {"action": "message", "messageId": "m-1",
                                                  "topic": "t", "content": "c", "attempt": 1}))
        self.app.poll()
        self.assertEqual(len(self.app.subscriber.table.get_children()), 1)
        self.assertIn("[Publisher] << ", self.app.log.contents())
        self.assertIn("publish_accepted", self.app.log.contents())

    def test_closed_event_only_affects_its_role(self):
        publisher_client, subscriber_client = self.clients
        self.app.publisher.bar.status.set("conectat")
        self.app.subscriber.bar.status.set("conectat")
        subscriber_client.events.put(("closed", None))
        self.app.poll()
        self.assertEqual(self.app.publisher.bar.status.get(), "conectat")
        self.assertEqual(self.app.subscriber.bar.status.get(), "deconectat")

    def test_close_disconnects_both_clients(self):
        self.app.close()
        self.assertEqual([client.disconnects for client in self.clients], [1, 1])

    def test_close_unsubscribes_the_subscriber_before_disconnecting(self):
        subscriber_client = self.clients[1]
        self.app.subscriber.subscriber_id.set("S1")
        self.app.subscriber.topic.set("sport")
        self.app.subscriber.subscribe()
        subscriber_client.sent.clear()
        self.app.close()
        self.assertEqual([p["action"] for p in subscriber_client.sent], ["unsubscribe"])
        self.assertEqual(subscriber_client.disconnects, 1)

    def test_missing_database_does_not_break_startup(self):
        self.assertEqual(self.app.state.status.get(), "broker.db negasit")


if __name__ == "__main__":
    unittest.main()
