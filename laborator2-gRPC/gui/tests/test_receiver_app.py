import time
import unittest

import grpc

from core.gateway import broker_pb2
from receiver_app import ReceiverApp
from tests.fake_broker import RunningFakeBroker
from tests.support import FakeRpcError, make_root, pump, release_on_cleanup


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

    def wait_for_undelivered_events(self, count=1):
        deadline = time.monotonic() + 5
        while self.app._bus._queue.qsize() < count and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(count, self.app._bus._queue.qsize())

    def settle(self, seconds=0.4):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.root.update()
            time.sleep(0.02)

    def action_states(self):
        return {str(button.cget("state")) for button in self.app._actions}

    def test_stream_closed_by_broker_disables_actions_and_allows_a_working_reconnect(self):
        self.connect()
        self.fake.servicer.finish_listen.set()
        pump(self.root, lambda: self.app.bar.status.get() == "Deconectat", message="deconectat")
        self.assertEqual({"disabled"}, self.action_states())
        self.assertEqual("normal", str(self.app.bar.connect_button.cget("state")))
        self.fake.servicer.finish_listen.clear()
        self.app.auto_ack.set(False)
        self.fake.servicer.listen_messages = [delivered("m1")]
        self.connect()
        self.assertEqual({"normal"}, self.action_states())
        pump(self.root, lambda: len(self.app.table.rows()) == 1, message="mesaj dupa reconectare")

    def test_connect_disables_actions_until_the_new_connection_is_confirmed(self):
        self.connect()
        self.assertEqual({"normal"}, self.action_states())
        self.app.connect()
        self.assertEqual({"disabled"}, self.action_states())
        self.assertEqual("Se conecteaza...", self.app.bar.status.get())

    def test_late_ack_result_of_an_old_gateway_does_not_change_the_row(self):
        self.app.auto_ack.set(False)
        self.fake.servicer.listen_messages = [delivered("m1")]
        self.connect()
        pump(self.root, lambda: len(self.app.table.rows()) == 1, message="primit")
        self.fake.servicer.listen_messages = []
        self.app.table.select("m1")
        self.app.ack_selected()
        self.wait_for_undelivered_events()
        self.app.disconnect()
        self.connect()
        self.settle()
        self.assertEqual("asteapta ACK", self.app.table.rows()[0][4])
        self.assertNotIn("[ACK]", self.app.log.contents())

    def test_late_subscribe_result_of_an_old_gateway_does_not_add_the_topic(self):
        self.connect()
        self.app.topic.set("sport")
        self.app.subscribe()
        self.wait_for_undelivered_events()
        self.app.disconnect()
        self.connect()
        self.settle()
        self.assertNotIn("sport", self.app.topics_text.get())
        self.assertEqual("Topicuri sesiune: -", self.app.topics_text.get())

    def test_queued_message_of_an_old_listener_is_dropped_after_reconnect(self):
        self.app.auto_ack.set(False)
        self.fake.servicer.listen_messages = [delivered("m1")]
        self.connect()
        self.wait_for_undelivered_events()
        self.fake.servicer.listen_messages = []
        self.app.connect()
        pump(self.root, lambda: self.app.bar.status.get().startswith("Listen"), message="listen nou")
        self.settle()
        self.assertEqual([], self.app.table.rows())
        self.assertEqual([], self.fake.servicer.acks)

    def test_queued_listen_closed_of_an_old_listener_does_not_drop_the_new_connection(self):
        self.connect()
        self.fake.servicer.finish_listen.set()
        self.wait_for_undelivered_events()
        self.fake.servicer.finish_listen.clear()
        self.app.connect()
        pump(self.root, lambda: self.app.bar.status.get().startswith("Listen"), message="listen nou")
        self.settle()
        self.assertTrue(self.app.bar.status.get().startswith("Listen"))
        self.assertEqual({"normal"}, self.action_states())

    def test_topic_list_is_cleared_when_the_session_ends(self):
        self.connect()
        self.app.topic.set("sport")
        self.app.subscribe()
        pump(self.root, lambda: "sport" in self.app.topics_text.get(), message="abonat")
        self.app.disconnect()
        self.assertEqual("Topicuri sesiune: -", self.app.topics_text.get())
        self.connect()
        self.assertEqual("Topicuri sesiune: -", self.app.topics_text.get())

    def id_entry_state(self):
        return str(self.app.subscriber_entry.cget("state"))

    def test_subscriber_id_entry_is_locked_while_connecting_and_connected(self):
        self.assertEqual("normal", self.id_entry_state())
        self.app.connect()
        self.assertEqual("disabled", self.id_entry_state())
        pump(self.root, lambda: self.app.bar.status.get().startswith("Listen"), message="listen")
        self.assertEqual("disabled", self.id_entry_state())
        self.app.disconnect()
        self.assertEqual("normal", self.id_entry_state())

    def test_subscriber_id_entry_is_unlocked_when_connect_fails(self):
        self.fake.kill()
        self.app.connect()
        pump(self.root, lambda: self.app.bar.status.get() == "Deconectat", timeout=8, message="deconectat")
        self.assertEqual("normal", self.id_entry_state())

    def test_subscriber_id_entry_is_unlocked_when_the_broker_closes_the_stream(self):
        self.connect()
        self.fake.servicer.finish_listen.set()
        pump(self.root, lambda: self.app.bar.status.get() == "Deconectat", message="deconectat")
        self.assertEqual("normal", self.id_entry_state())

    def test_reconnect_as_another_id_clears_the_table_and_forgets_old_deliveries(self):
        self.fake.servicer.listen_messages = [delivered("m1")]
        self.connect()
        pump(self.root, lambda: self.app.table.rows() == [("m1", "sport", "salut", "1/3", "ACK trimis")], message="primit")
        self.app.disconnect()
        self.app.auto_ack.set(False)
        self.app.subscriber_id.set("gui-other")
        self.app.connect()
        self.assertEqual([], self.app.table.rows())
        pump(self.root, lambda: self.app.bar.status.get().startswith("Listen"), message="listen nou")
        pump(self.root, lambda: self.app.table.rows() == [("m1", "sport", "salut", "1/3", "asteapta ACK")],
             message="mesaj nou fara retransmisie")

    def test_reconnect_with_the_same_id_keeps_the_table(self):
        self.fake.servicer.listen_messages = [delivered("m1")]
        self.connect()
        pump(self.root, lambda: len(self.app.table.rows()) == 1, message="primit")
        self.fake.servicer.listen_messages = []
        self.app.disconnect()
        self.app.connect()
        self.assertEqual(1, len(self.app.table.rows()))

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

        def failing_ack(subscriber_id, message_id):
            raise FakeRpcError(grpc.StatusCode.NOT_FOUND, "necunoscut")

        self.app._gateway.ack = failing_ack
        self.app.table.select("m1")
        self.app.ack_selected()
        pump(self.root, lambda: "ACK esuat" in self.app.log.contents(), message="ack esuat")
        self.assertIn("NOT_FOUND", self.app.log.contents())
        self.assertEqual("asteapta ACK", self.app.table.rows()[0][4])


if __name__ == "__main__":
    unittest.main()
