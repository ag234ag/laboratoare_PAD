from panels import SubscriberPanel
from support import FakeClient, TkTestCase

MESSAGE = {"action": "message", "messageId": "m-1", "topic": "sport", "content": "goal", "attempt": 1}


class SubscriberPanelTest(TkTestCase):
    def setUp(self):
        super().setUp()
        self.client = FakeClient()
        self.panel = SubscriberPanel(self.root, self.client, self.log)

    def rows(self):
        table = self.panel.table
        return [tuple(str(v) for v in table.item(i, "values")) for i in table.get_children()]

    def test_subscribe_sends_action_with_id_and_topic(self):
        self.panel.subscriber_id.set("S1")
        self.panel.topic.set("sport")
        self.panel.subscribe()
        self.assertEqual(self.client.sent, [{"action": "subscribe", "subscriberId": "S1", "topic": "sport"}])

    def test_unsubscribe_sends_action(self):
        self.panel.subscriber_id.set("S1")
        self.panel.topic.set("sport")
        self.panel.unsubscribe()
        self.assertEqual(self.client.sent[0]["action"], "unsubscribe")

    def test_blank_topic_or_subscriber_id_sends_nothing(self):
        for subscriber_id, topic in (("S1", ""), ("S1", "  "), ("", "sport"), ("  ", "sport")):
            self.panel.subscriber_id.set(subscriber_id)
            self.panel.topic.set(topic)
            self.panel.subscribe()
        self.assertEqual(self.client.sent, [])
        self.assertEqual(self.log.contents().count("ERR"), 4)

    def test_message_is_listed_and_auto_acked(self):
        self.panel.handle(("message", dict(MESSAGE)))
        self.assertEqual(self.rows(), [("m-1", "sport", "goal", "1")])
        self.assertEqual(self.client.sent, [{"action": "ack", "messageId": "m-1"}])

    def test_retransmission_updates_attempt_in_place_and_reacks(self):
        self.panel.handle(("message", dict(MESSAGE)))
        self.panel.handle(("message", dict(MESSAGE, attempt=2)))
        self.assertEqual(self.rows(), [("m-1", "sport", "goal", "2")])
        self.assertEqual(len(self.client.sent), 2)

    def test_auto_ack_off_sends_nothing_until_manual_ack(self):
        self.panel.auto_ack.set(False)
        self.panel.handle(("message", dict(MESSAGE)))
        self.assertEqual(self.client.sent, [])
        self.panel.table.selection_set(self.panel.table.get_children()[0])
        self.panel.ack_selected()
        self.assertEqual(self.client.sent, [{"action": "ack", "messageId": "m-1"}])

    def test_manual_ack_without_selection_logs_error(self):
        self.panel.ack_selected()
        self.assertEqual(self.client.sent, [])
        self.assertIn("ERR", self.log.contents())

    def test_other_broker_actions_do_not_create_rows(self):
        self.panel.handle(("message", {"action": "subscribed", "subscriberId": "S1", "topic": "sport"}))
        self.panel.handle(("message", {"action": "error", "reason": "invalid_json"}))
        self.assertEqual(self.rows(), [])
        self.assertIn("invalid_json", self.log.contents())

    def test_content_with_braces_backslash_quotes_unicode_is_shown_intact(self):
        content = 'a {b} \\ "c" ăț 😀'
        self.panel.handle(("message", dict(MESSAGE, content=content)))
        self.assertEqual(self.rows()[0][2], content)

    def test_ack_send_failure_is_logged_not_raised(self):
        client = FakeClient(fail_with=ConnectionError("not connected"))
        panel = SubscriberPanel(self.root, client, self.log)
        panel.handle(("message", dict(MESSAGE)))
        self.assertIn("not connected", self.log.contents())

    def test_newest_message_is_listed_first(self):
        self.panel.handle(("message", dict(MESSAGE)))
        self.panel.handle(("message", dict(MESSAGE, messageId="m-2")))
        self.assertEqual([row[0] for row in self.rows()], ["m-2", "m-1"])

    def test_each_panel_gets_its_own_default_subscriber_id(self):
        other = SubscriberPanel(self.root, FakeClient(), self.log)
        self.assertNotEqual(self.panel.subscriber_id.get(), other.subscriber_id.get())

    def test_shutdown_unsubscribes_every_subscribed_topic_then_disconnects(self):
        self.panel.subscriber_id.set("S1")
        for topic in ("sport", "news"):
            self.panel.topic.set(topic)
            self.panel.subscribe()
        self.client.sent.clear()
        self.panel.shutdown()
        unsubscribed = sorted(p["topic"] for p in self.client.sent if p["action"] == "unsubscribe")
        self.assertEqual(unsubscribed, ["news", "sport"])
        self.assertEqual(self.client.disconnects, 1)

    def test_unsubscribed_topic_is_not_unsubscribed_again_on_shutdown(self):
        self.panel.subscriber_id.set("S1")
        self.panel.topic.set("sport")
        self.panel.subscribe()
        self.panel.unsubscribe()
        self.client.sent.clear()
        self.panel.shutdown()
        self.assertEqual(self.client.sent, [])

    def test_shutdown_without_subscriptions_only_disconnects(self):
        self.panel.shutdown()
        self.assertEqual(self.client.sent, [])
        self.assertEqual(self.client.disconnects, 1)

    def test_shutdown_survives_a_dead_connection(self):
        client = FakeClient()
        panel = SubscriberPanel(self.root, client, self.log)
        panel.subscriber_id.set("S1")
        panel.topic.set("sport")
        panel.subscribe()
        client.fail_with = ConnectionError("not connected")
        panel.shutdown()
        self.assertEqual(client.disconnects, 1)

    def test_failed_subscribe_is_not_remembered_for_shutdown(self):
        client = FakeClient(fail_with=ConnectionError("not connected"))
        panel = SubscriberPanel(self.root, client, self.log)
        panel.subscriber_id.set("S1")
        panel.topic.set("sport")
        panel.subscribe()
        client.fail_with = None
        panel.shutdown()
        self.assertEqual(client.sent, [])

    def test_panel_uses_given_name_and_remove_button_calls_back(self):
        removed = []
        panel = SubscriberPanel(self.root, FakeClient(), self.log, name="Subscriber 4", on_remove=removed.append)
        self.assertEqual(panel.cget("text"), "Subscriber 4")
        panel.remove_button.invoke()
        self.assertEqual(removed, [panel])
