import unittest

import grpc

from core.gateway import broker_pb2
from receiver_app import ReceiverApp
from tests.fake_broker import RunningFakeBroker
from tests.support import make_root, pump, release_on_cleanup


def delivered(message_id, content="salut", attempt=1):
    return broker_pb2.DeliveredMessage(message_id=message_id, topic="sport", content=content, attempt=attempt, max_attempts=3)


class ReceiverAppTest(unittest.TestCase):
    def setUp(self):
        release_on_cleanup(self, "app", "root")
        self.fake = RunningFakeBroker()
        self.fake.__enter__()
        self.addCleanup(self.fake.__exit__)
        self.root = make_root(self)
        self.app = ReceiverApp(self.root, "127.0.0.1", self.fake.port)
        self.app.subscriber_id.set("gui-test")
        self.addCleanup(self.app.disconnect)

    def connect(self):
        self.app.connect()
        pump(self.root, lambda: self.app.bar.status.get().startswith("Listen"), message="listen")

    def test_connect_opens_listen_with_the_subscriber_id(self):
        self.connect()
        pump(self.root, lambda: self.fake.servicer.listen_requests == ["gui-test"], message="listen request")

    def test_auto_ack_confirms_every_message(self):
        self.fake.servicer.listen_messages = [delivered("m1"), delivered("m2")]
        self.connect()
        pump(self.root, lambda: len(self.fake.servicer.acks) == 2, message="ack-uri")
        pump(self.root, lambda: all(row[4] == "ACK trimis" for row in self.app.table.rows())
             and len(self.app.table.rows()) == 2, message="stari")

    def test_manual_ack_waits_until_the_button(self):
        self.app.auto_ack.set(False)
        self.fake.servicer.listen_messages = [delivered("m1")]
        self.connect()
        pump(self.root, lambda: self.app.table.rows() == [("m1", "sport", "salut", "1/3", "asteapta ACK")], message="primit")
        self.assertEqual([], self.fake.servicer.acks)
        self.app.ack_selected()
        self.assertIn("Selecteaza", self.app.log.contents())
        self.app.table.select("m1")
        self.app.ack_selected()
        pump(self.root, lambda: self.fake.servicer.acks == [("gui-test", "m1")], message="ack manual")
        pump(self.root, lambda: self.app.table.rows()[0][4] == "ACK trimis", message="stare")

    def test_retransmission_updates_the_same_row(self):
        self.app.auto_ack.set(False)
        self.fake.servicer.listen_messages = [delivered("m1", attempt=1), delivered("m1", attempt=2)]
        self.connect()
        pump(self.root, lambda: self.app.table.rows() == [("m1", "sport", "salut", "2/3", "asteapta ACK, retransmisie")],
             message="retransmisie")

    def test_unicode_newline_and_numeric_content_are_preserved(self):
        self.app.auto_ack.set(False)
        self.fake.servicer.listen_messages = [delivered("m1", content="ăîșț\nlinia 2"), delivered("m2", content="007")]
        self.connect()
        pump(self.root, lambda: len(self.app.table.rows()) == 2, message="doua mesaje")
        contents = {row[0]: row[2] for row in self.app.table.rows()}
        self.assertEqual({"m1": "ăîșț\nlinia 2", "m2": "007"}, contents)

    def test_subscribe_and_unsubscribe_update_the_topic_list(self):
        self.connect()
        self.app.topic.set("sport")
        self.app.subscribe()
        pump(self.root, lambda: "sport" in self.app.topics_text.get(), message="abonat")
        self.app.unsubscribe()
        pump(self.root, lambda: "sport" not in self.app.topics_text.get(), message="dezabonat")
        self.assertEqual([("subscribe", "gui-test", "sport"), ("unsubscribe", "gui-test", "sport")],
                         self.fake.servicer.subscriptions)

    def test_subscribe_error_is_logged_and_topic_is_not_added(self):
        self.fake.servicer.reject_subscribe_with = (grpc.StatusCode.INVALID_ARGUMENT, "topic invalid")
        self.connect()
        self.app.topic.set("sport")
        self.app.subscribe()
        pump(self.root, lambda: "INVALID_ARGUMENT" in self.app.log.contents(), message="eroare")
        self.assertNotIn("sport", self.app.topics_text.get())

    def test_empty_or_invalid_topic_is_not_sent(self):
        self.connect()
        for topic in ("  ", "topic invalid"):
            with self.subTest(topic=topic):
                self.app.topic.set(topic)
                self.app.subscribe()
        self.assertIn("Eroare: topicul nu poate fi gol", self.app.log.contents())
        self.assertIn("Eroare: topicul poate contine doar", self.app.log.contents())
        self.assertEqual([], self.fake.servicer.subscriptions)

    def test_invalid_subscriber_id_blocks_connect(self):
        for value in ("", "a b"):
            with self.subTest(value=value):
                self.app.subscriber_id.set(value)
                self.app.connect()
                self.assertEqual("Deconectat", self.app.bar.status.get())
        self.assertIn("Eroare: Subscriber ID nu poate fi gol", self.app.log.contents())
        self.assertIn("Eroare: Subscriber ID poate contine doar", self.app.log.contents())

    def test_replaced_session_returns_to_disconnected(self):
        self.fake.servicer.abort_listen_with = (grpc.StatusCode.ABORTED, "SESSION_REPLACED")
        self.app.connect()
        pump(self.root, lambda: "inlocuita" in self.app.log.contents(), message="inlocuita")
        self.assertEqual("Deconectat", self.app.bar.status.get())

    def test_broker_going_away_returns_to_disconnected_and_allows_reconnect(self):
        self.connect()
        self.fake.kill()
        pump(self.root, lambda: self.app.bar.status.get() == "Deconectat", timeout=8, message="deconectat")
        self.assertEqual("normal", str(self.app.bar.connect_button.cget("state")))

    def test_ack_error_is_logged(self):
        self.app.auto_ack.set(False)
        self.fake.servicer.listen_messages = [delivered("m1")]
        self.connect()
        pump(self.root, lambda: len(self.app.table.rows()) == 1, message="primit")
        self.fake.kill()
        self.app.table.select("m1")
        self.app.ack_selected()
        pump(self.root, lambda: "ACK esuat" in self.app.log.contents(), timeout=8, message="ack esuat")


if __name__ == "__main__":
    unittest.main()
