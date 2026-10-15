import unittest

from core import received_messages as rm
from core.gateway import broker_pb2


def delivered(message_id="m1", attempt=1):
    return broker_pb2.DeliveredMessage(message_id=message_id, topic="t", content="ăb\nc", attempt=attempt, max_attempts=3)


class ReceivedMessagesTest(unittest.TestCase):
    def test_first_delivery_waits_for_ack_and_is_not_a_retransmission(self):
        item = rm.ReceivedMessages().register(delivered())
        self.assertEqual(("m1", "t", "ăb\nc", "1/3", rm.WAITING), item.row())

    def test_same_message_id_is_marked_as_retransmission(self):
        messages = rm.ReceivedMessages()
        messages.register(delivered(attempt=1))
        item = messages.register(delivered(attempt=2))
        self.assertEqual(("m1", "t", "ăb\nc", "2/3", f"{rm.WAITING}, retransmisie"), item.row())

    def test_mark_acked_and_duplicate(self):
        messages = rm.ReceivedMessages()
        messages.register(delivered())
        self.assertEqual(rm.ACKED, messages.mark_acked("m1", duplicate=False).status)
        self.assertEqual(rm.ACKED_DUPLICATE, messages.mark_acked("m1", duplicate=True).status)

    def test_mark_acked_unknown_message_returns_none(self):
        self.assertIsNone(rm.ReceivedMessages().mark_acked("nope", duplicate=False))


if __name__ == "__main__":
    unittest.main()
