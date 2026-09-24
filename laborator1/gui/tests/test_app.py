import tkinter as tk
import unittest

from app import App
from support import FakeClient, scroll_height


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

    def subscribe(self, panel, subscriber_id, topic):
        panel.subscriber_id.set(subscriber_id)
        panel.topic.set(topic)
        panel.subscribe()

    def test_starts_with_one_publisher_and_two_subscribers(self):
        self.assertEqual(len(self.app.publishers), 1)
        self.assertEqual(len(self.app.subscribers), 2)
        self.assertEqual(len(self.clients), 3)

    def test_events_are_dispatched_to_their_own_panel(self):
        self.clients[0].events.put(("message", {"action": "publish_accepted"}))
        self.clients[2].events.put(("message", {"action": "message", "messageId": "m-1",
                                                "topic": "t", "content": "c", "attempt": 1}))
        self.app.poll()
        self.assertEqual(len(self.app.subscribers[0].table.get_children()), 0)
        self.assertEqual(len(self.app.subscribers[1].table.get_children()), 1)
        self.assertIn("[Publisher 1] << ", self.app.log.contents())
        self.assertIn("[Subscriber 2] << ", self.app.log.contents())

    def test_closed_event_only_affects_its_own_panel(self):
        for panel in self.app.publishers + self.app.subscribers:
            panel.bar.status.set("conectat")
        self.clients[1].events.put(("closed", None))
        self.app.poll()
        self.assertEqual(self.app.subscribers[0].bar.status.get(), "deconectat")
        self.assertEqual(self.app.subscribers[1].bar.status.get(), "conectat")
        self.assertEqual(self.app.publishers[0].bar.status.get(), "conectat")

    def test_connect_all_connects_every_panel(self):
        self.app.connect_all()
        self.assertEqual([client.connected for client in self.clients], [True, True, True])
        for panel in self.app.publishers + self.app.subscribers:
            self.assertEqual(panel.bar.status.get(), "conectat")

    def test_add_subscriber_and_publisher_get_own_clients_and_names(self):
        subscriber = self.app.add_subscriber()
        publisher = self.app.add_publisher()
        self.assertEqual(len(self.clients), 5)
        self.assertEqual(subscriber.cget("text"), "Subscriber 3")
        self.assertEqual(publisher.cget("text"), "Publisher 2")
        self.assertEqual(len(self.app.subscribers), 3)
        self.assertEqual(len(self.app.publishers), 2)

    def test_added_subscribers_get_distinct_default_ids(self):
        ids = {panel.subscriber_id.get() for panel in self.app.subscribers}
        ids.add(self.app.add_subscriber().subscriber_id.get())
        self.assertEqual(len(ids), 3)

    def test_remove_subscriber_unsubscribes_disconnects_and_forgets_it(self):
        target = self.app.subscribers[0]
        self.subscribe(target, "S1", "sport")
        self.clients[1].sent.clear()
        self.app.remove(target)
        self.assertEqual([p["action"] for p in self.clients[1].sent], ["unsubscribe"])
        self.assertEqual(self.clients[1].disconnects, 1)
        self.assertNotIn(target, self.app.subscribers)
        self.assertEqual(len(self.app.subscribers), 1)
        self.assertEqual(self.clients[2].disconnects, 0)

    def test_remove_publisher_disconnects_and_forgets_it(self):
        extra = self.app.add_publisher()
        self.app.remove(extra)
        self.assertEqual(self.clients[3].disconnects, 1)
        self.assertEqual(len(self.app.publishers), 1)

    def test_events_of_a_removed_panel_are_ignored(self):
        target = self.app.subscribers[0]
        self.app.remove(target)
        self.clients[1].events.put(("message", {"action": "pong"}))
        self.app.poll()
        self.assertNotIn("pong", self.app.log.contents())

    def test_remove_button_calls_back_with_its_panel(self):
        target = self.app.subscribers[0]
        target.remove_button.invoke()
        self.assertNotIn(target, self.app.subscribers)
        self.assertEqual(self.clients[1].disconnects, 1)

    def test_close_disconnects_every_client_once(self):
        self.app.close()
        self.assertEqual([client.disconnects for client in self.clients], [1, 1, 1])

    def test_close_unsubscribes_every_subscribed_subscriber(self):
        self.subscribe(self.app.subscribers[0], "S1", "sport")
        self.subscribe(self.app.subscribers[1], "S2", "sport")
        self.clients[1].sent.clear()
        self.clients[2].sent.clear()
        self.app.close()
        self.assertEqual([p["action"] for p in self.clients[1].sent], ["unsubscribe"])
        self.assertEqual([p["action"] for p in self.clients[2].sent], ["unsubscribe"])

    def test_scroll_areas_grow_with_more_instances_and_shrink_after_remove(self):
        before = scroll_height(self.app.subscriber_scroll)
        added = [self.app.add_subscriber() for _ in range(6)]
        after_add = scroll_height(self.app.subscriber_scroll)
        self.assertGreater(after_add, before)
        for panel in added:
            self.app.remove(panel)
        self.assertLess(scroll_height(self.app.subscriber_scroll), after_add)
        publisher_before = scroll_height(self.app.publisher_scroll)
        for _ in range(4):
            self.app.add_publisher()
        self.assertGreater(scroll_height(self.app.publisher_scroll), publisher_before)

    def test_second_subscriber_cannot_take_an_id_already_in_use(self):
        first, second = self.app.subscribers
        self.subscribe(first, "SAME", "sport")
        self.clients[2].sent.clear()
        self.subscribe(second, "SAME", "sport")
        self.assertEqual(self.clients[2].sent, [])
        self.assertIn("Subscriber 1", self.app.log.contents())

    def test_id_is_free_again_after_its_owner_is_removed(self):
        first, second = self.app.subscribers
        self.subscribe(first, "SAME", "sport")
        self.app.remove(first)
        self.clients[2].sent.clear()
        self.subscribe(second, "SAME", "sport")
        self.assertEqual([p["action"] for p in self.clients[2].sent], ["subscribe"])

    def test_same_id_typed_but_not_yet_subscribed_does_not_block_the_other(self):
        first, second = self.app.subscribers
        first.subscriber_id.set("SAME")
        self.clients[2].sent.clear()
        self.subscribe(second, "SAME", "sport")
        self.assertEqual(len(self.clients[2].sent), 1)

    def test_missing_database_does_not_break_startup(self):
        self.assertEqual(self.app.state.status.get(), "broker.db negasit")


if __name__ == "__main__":
    unittest.main()
