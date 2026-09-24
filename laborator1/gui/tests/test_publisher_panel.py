import json

from panels import PublisherPanel
from support import FakeClient, TkTestCase


class PublisherPanelTest(TkTestCase):
    def setUp(self):
        super().setUp()
        self.client = FakeClient()
        self.panel = PublisherPanel(self.root, self.client, self.log)

    def test_publish_sends_publish_action_with_uuid(self):
        self.panel.topic.set("sport")
        self.panel.content.set("goal")
        self.panel.publish()
        sent = self.client.sent[0]
        self.assertEqual((sent["action"], sent["topic"], sent["content"]), ("publish", "sport", "goal"))
        self.assertEqual(len(sent["messageId"]), 36)

    def test_every_publish_gets_a_new_message_id(self):
        self.panel.topic.set("t")
        self.panel.content.set("c")
        self.panel.publish()
        self.panel.publish()
        self.assertNotEqual(self.client.sent[0]["messageId"], self.client.sent[1]["messageId"])

    def test_blank_topic_or_content_sends_nothing_and_logs_error(self):
        for topic, content in (("", "c"), ("t", ""), ("   ", "c"), ("t", "   ")):
            self.panel.topic.set(topic)
            self.panel.content.set(content)
            self.panel.publish()
        self.assertEqual(self.client.sent, [])
        self.assertEqual(self.log.contents().count("ERR"), 4)

    def test_invalid_json_button_sends_unparsable_line(self):
        self.panel.send_invalid()
        self.assertEqual(len(self.client.raw), 1)
        with self.assertRaises(json.JSONDecodeError):
            json.loads(self.client.raw[0])

    def test_send_failure_is_logged_not_raised(self):
        client = FakeClient(fail_with=ConnectionError("not connected"))
        panel = PublisherPanel(self.root, client, self.log)
        panel.topic.set("t")
        panel.content.set("c")
        panel.publish()
        self.assertIn("not connected", self.log.contents())

    def test_incoming_message_and_raw_lines_are_logged(self):
        self.panel.handle(("message", {"action": "publish_accepted"}))
        self.panel.handle(("raw", "garbage line"))
        text = self.log.contents()
        self.assertIn("publish_accepted", text)
        self.assertIn("garbage line", text)

    def test_closed_event_marks_disconnected_and_logs(self):
        self.panel.bar.status.set("conectat")
        self.panel.handle(("closed", None))
        self.assertEqual(self.panel.bar.status.get(), "deconectat")
        self.assertIn("inchisa", self.log.contents())

    def test_connect_failure_is_logged_and_status_stays_disconnected(self):
        client = FakeClient(connect_error=ConnectionRefusedError("refused"))
        panel = PublisherPanel(self.root, client, self.log)
        panel.bar.connect()
        self.assertEqual(panel.bar.status.get(), "deconectat")
        self.assertIn("refused", self.log.contents())

    def test_connect_with_bad_port_is_logged(self):
        self.panel.bar._port.set("abc")
        self.panel.bar.connect()
        self.assertIn("ERR", self.log.contents())
        self.assertFalse(self.client.connected)

    def test_connect_and_disconnect_update_status(self):
        self.panel.bar.connect()
        self.assertEqual(self.panel.bar.status.get(), "conectat")
        self.panel.bar.disconnect()
        self.assertEqual(self.panel.bar.status.get(), "deconectat")
        self.assertEqual(self.client.disconnects, 1)

    def test_panel_uses_given_name_and_remove_button_calls_back(self):
        removed = []
        panel = PublisherPanel(self.root, FakeClient(), self.log, name="Publisher 7", on_remove=removed.append)
        self.assertEqual(panel.cget("text"), "Publisher 7")
        panel.remove_button.invoke()
        self.assertEqual(removed, [panel])

    def test_shutdown_disconnects_the_client(self):
        self.panel.shutdown()
        self.assertEqual(self.client.disconnects, 1)
        self.assertEqual(self.panel.bar.status.get(), "deconectat")
