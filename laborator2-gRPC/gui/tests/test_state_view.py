import unittest

from core import state_view
from core.gateway import broker_pb2


def sample_state():
    return broker_pb2.BrokerState(
        storage_mode="persistent",
        messages=[broker_pb2.MessageRecord(message_id="m1", topic="t", content="c", status="PENDING", created_at_utc="x")],
        deliveries=[broker_pb2.DeliveryRecord(message_id="m1", subscriber_id="s1", status="DEAD", retry_count=3, last_error="e")],
        dead_letters=[broker_pb2.DeadLetterRecord(id=7, message_id="m1", subscriber_id="s1", topic="t",
                                                   raw_payload="p", reason="r", retry_count=3, created_at_utc="y")],
        subscriptions=[broker_pb2.SubscriptionRecord(subscriber_id="s1", topic="t")],
        online_subscribers=["s1", "s2"],
    )


class StateViewTest(unittest.TestCase):
    def test_message_rows(self):
        self.assertEqual([("m1", ("m1", "t", "c", "PENDING", "x"))], state_view.message_rows(sample_state()))

    def test_delivery_rows_key_combines_message_and_subscriber(self):
        self.assertEqual([("m1|s1", ("m1", "s1", "DEAD", "3", "e"))], state_view.delivery_rows(sample_state()))

    def test_dead_letter_rows(self):
        self.assertEqual([("7", ("7", "m1", "s1", "t", "r", "3", "y", "p"))], state_view.dead_letter_rows(sample_state()))

    def test_subscription_rows(self):
        self.assertEqual([("s1|t", ("s1", "t"))], state_view.subscription_rows(sample_state()))

    def test_online_text(self):
        self.assertEqual("Abonati online: s1, s2", state_view.online_text(sample_state()))
        self.assertEqual("Abonati online: nimeni", state_view.online_text(broker_pb2.BrokerState()))

    def test_column_counts_match_row_widths(self):
        state = sample_state()
        pairs = [
            (state_view.MESSAGE_COLUMNS, state_view.message_rows(state)),
            (state_view.DELIVERY_COLUMNS, state_view.delivery_rows(state)),
            (state_view.DEAD_LETTER_COLUMNS, state_view.dead_letter_rows(state)),
            (state_view.SUBSCRIPTION_COLUMNS, state_view.subscription_rows(state)),
        ]
        for columns, rows in pairs:
            self.assertEqual(len(columns), len(rows[0][1]))


if __name__ == "__main__":
    unittest.main()
