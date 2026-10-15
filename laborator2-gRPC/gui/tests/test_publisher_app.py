import unittest

import grpc

from publisher_app import PublisherApp
from tests.fake_broker import RunningFakeBroker
from tests.support import make_root, pump, unused_port


class PublisherAppTest(unittest.TestCase):
    def setUp(self):
        self.fake = RunningFakeBroker()
        self.fake.__enter__()
        self.addCleanup(self.fake.__exit__)
        self.root = make_root(self)
        self.app = PublisherApp(self.root, "127.0.0.1", self.fake.port)
        self.addCleanup(self.app.disconnect)

    def connect(self):
        self.app.connect()
        pump(self.root, lambda: "Conectat" in self.app.bar.status.get(), message="conectare")

    def history_labels(self):
        return [row[3] for row in self.app.history.rows()]

    def test_connect_shows_storage_mode(self):
        self.connect()
        self.assertIn("transient", self.app.bar.status.get())

    def test_connect_to_unavailable_broker_reports_in_log(self):
        self.app.bar.port.set(str(unused_port()))
        self.app.connect()
        pump(self.root, lambda: "indisponibil" in self.app.log.contents(), timeout=8, message="eroare")
        self.assertEqual("Deconectat", self.app.bar.status.get())

    def test_publish_adds_ok_row_with_subscriber_count(self):
        self.connect()
        self.app.topic.set("sport")
        self.app.content.set("meci ă")
        self.app.publish()
        pump(self.root, lambda: self.history_labels() == ["OK, 2 abonati"], message="rezultat")
        self.assertEqual("meci ă", self.fake.servicer.published[0].content)

    def test_invalid_topic_is_blocked_before_the_network(self):
        self.connect()
        self.app.topic.set("topic invalid")
        self.app.content.set("x")
        self.app.publish()
        self.assertIn("Eroare", self.app.log.contents())
        self.assertEqual([], self.app.history.rows())
        self.assertEqual([], self.fake.servicer.published)

    def test_dlq_buttons_bypass_local_validation(self):
        self.connect()
        self.fake.servicer.reject_with = (grpc.StatusCode.INVALID_ARGUMENT, "topic")
        self.app.publish_without_topic()
        self.app.publish_without_content()
        pump(self.root, lambda: self.history_labels() == ["INVALID -> DLQ", "INVALID -> DLQ"], message="dlq")

    def test_resend_last_reuses_the_message_id_and_shows_duplicate(self):
        self.connect()
        self.app.topic.set("sport")
        self.app.content.set("unu")
        self.app.publish()
        pump(self.root, lambda: len(self.app.history.rows()) == 1, message="primul")
        self.fake.servicer.reject_with = (grpc.StatusCode.ALREADY_EXISTS, "DUPLICATE_MESSAGE_ID")
        self.app.resend_last()
        pump(self.root, lambda: "DUPLICATE" in self.history_labels(), message="duplicat")
        rows = self.app.history.rows()
        self.assertEqual(rows[0][1], rows[1][1])

    def test_resend_without_previous_message_explains(self):
        self.connect()
        self.app.resend_last()
        self.assertIn("Publica un mesaj", self.app.log.contents())

    def test_actions_without_connection_do_not_crash(self):
        self.app.topic.set("sport")
        self.app.content.set("x")
        self.app.publish()
        self.assertIn("Nu esti conectat", self.app.log.contents())

    def test_late_connect_result_of_an_old_gateway_is_ignored(self):
        self.app.connect()
        self.app.disconnect()
        for _ in range(20):
            self.root.update()
        self.assertEqual("Deconectat", self.app.bar.status.get())

    def test_ping_logs_pong(self):
        self.connect()
        self.app.ping()
        pump(self.root, lambda: "PONG" in self.app.log.contents(), message="pong")


if __name__ == "__main__":
    unittest.main()
